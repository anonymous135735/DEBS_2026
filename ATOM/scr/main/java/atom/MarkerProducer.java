package atom;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class MarkerProducer {
    private static final String UNIQUE_ID = UUID.randomUUID().toString().substring(0, 8);
    private final ConcurrentHashMap<String, Integer> currentPartitions = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long>> ack = new ConcurrentHashMap<>();
    private final BlockingQueue<AckEvent> ackQueue = new LinkedBlockingQueue<>(ACK_QUEUE_CAPACITY);
    private static final int ACK_QUEUE_CAPACITY = 10000;

    private volatile boolean running = true;
    private final AtomicBoolean newFlag = new AtomicBoolean(false);
    private final AtomicBoolean waitFlag = new AtomicBoolean(false);
    private final AtomicBoolean isLeader = new AtomicBoolean(false);

    private final AtomicLong m = new AtomicLong(0L);
    private final AtomicLong newm = new AtomicLong(0L);
    private final AtomicLong sId = new AtomicLong(0L);

    private final AtomicInteger retryCnt = new AtomicInteger(0);

    private final ScheduledThreadPoolExecutor markerScheduler = new ScheduledThreadPoolExecutor(1);
    private final ScheduledThreadPoolExecutor ackTimeoutScheduler = new ScheduledThreadPoolExecutor(1);
    private final ScheduledThreadPoolExecutor partitionMonitor = new ScheduledThreadPoolExecutor(1);
    private final ExecutorService ackExecutor = Executors.newSingleThreadExecutor();
    private final ExecutorService repProcessor = Executors.newSingleThreadExecutor();

    private volatile ScheduledFuture<?> mTimer = null;
    private volatile ScheduledFuture<?> ackTimer = null;
    private volatile ScheduledFuture<?> monitorTimer = null;

    private final long deltaM;
    private final long deltaAck;
    private final long deltaMonitor = 5;
    private final int rMax;
    private final String leaderId;
    private final long runDuration;

    private final KafkaProducer<String, String> producer;
    private final KafkaConsumer<String, String> repConsumer;
    private final AdminClient adminClient;
    
    public static void main(String[] args) {
        long maxHeapSize = Runtime.getRuntime().maxMemory();
        System.out.println("Max Heap Size: " + (maxHeapSize / (1024 * 1024 * 1024)) + " GB");
        System.out.println("MarkerProducerID: " + UNIQUE_ID);

        if (args.length < 5) {
            System.out.println("Usage: MarkerProducer <leader ID> <marker interval in ms> <ack interval in ms> <max retries> <run duration in s>");
            System.exit(1);
        }

        System.out.println(Arrays.toString(args));
        
        String leaderId = args[0];
        long deltaM = Long.parseLong(args[1]);
        long deltaAck = Long.parseLong(args[2]);
        int rMax = Integer.parseInt(args[3]);
        long runDuration = Integer.parseInt(args[4]);
        
        MarkerProducer mp = new MarkerProducer(leaderId, deltaM, deltaAck, rMax, runDuration);

        try {
            Utils.sendStartSignal();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public MarkerProducer(String leaderId, long deltaM, long deltaAck, int rMax, long runDuration) {
        markerScheduler.setRemoveOnCancelPolicy(true);
        ackTimeoutScheduler.setRemoveOnCancelPolicy(true);

        this.leaderId = leaderId;
        this.deltaM = deltaM;
        this.deltaAck = deltaAck;
        this.rMax = rMax;
        this.runDuration = runDuration;

        this.producer = Utils.createProducer(Utils.BOOTSTRAP_SERVERS, true);
        this.repConsumer = Utils.createConsumer(Utils.BOOTSTRAP_SERVERS, "rep-consumer", true);
        this.adminClient = Utils.createAdmin(Utils.BOOTSTRAP_SERVERS);

        initialize();

        if (runDuration > 0) {
            System.out.println("Running the MP for " + runDuration + " seconds");
            new Thread(() -> {
                try {
                    Thread.sleep(runDuration * 1000);
                    System.out.println("Run duration reached. Shutting down gracefully...");
                    shutdown(false);    //timer shutdown 
                } catch (InterruptedException ignored) {}
            }, "ShutdownTimer").start();
        } else {
            System.out.println("Running the MP continuously (Ctrl+C or send SIGINT/SIGTERM to stop).");
        }

        // hook for external signal 
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            externalShutdown.set(true);
            //System.out.println("Shutdown signal received.");
            shutdown(true);
        }));
    }

    private void initialize() {
        updateLists();
        repProcessor.submit(this::performLeaderElection);
        startMonitor();
        startAckSystem();
        startPeriodicMarker();
    }

    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);
    private final AtomicBoolean externalShutdown = new AtomicBoolean(false);

    private void shutdown(boolean fromExternal) {
        if (!shuttingDown.compareAndSet(false, true)) {
            return; // already shutting down
        }

        running = false;
        System.out.println("Initiating " + (fromExternal ? "external" : "internal") + " shutdown...");
        System.out.flush();

        try {
            // Interrupt any blocking poll
            repConsumer.wakeup();

            // Cancel timers safely
            cancelTimer(mTimer);
            cancelTimer(ackTimer);
            cancelTimer(monitorTimer);

            // Shut down executors
            markerScheduler.shutdownNow();
            ackExecutor.shutdownNow();
            ackTimeoutScheduler.shutdownNow();
            partitionMonitor.shutdownNow();
            repProcessor.shutdownNow();

            // Wait for termination (non-blocking wait)
            markerScheduler.awaitTermination(3, TimeUnit.SECONDS);
            ackExecutor.awaitTermination(3, TimeUnit.SECONDS);
            ackTimeoutScheduler.awaitTermination(3, TimeUnit.SECONDS);
            partitionMonitor.awaitTermination(3, TimeUnit.SECONDS);
            repProcessor.awaitTermination(3, TimeUnit.SECONDS);

            // Close Kafka resources
            producer.close(Duration.ofSeconds(5));
            repConsumer.close(Duration.ofSeconds(5));
            adminClient.close(Duration.ofSeconds(5));

            System.out.println("Kafka clients and executers closed successfully.");
        } catch (Exception e) {
            System.err.println("Error during shutdown: " + e.getMessage());
            e.printStackTrace();
        }

        if (fromExternal) {
            new Thread(() -> {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ignored) {}
                System.exit(0);
            }, "ExitThread").start();
        }
    }

    private void partitionMonitor() {
        if (!running || Thread.currentThread().isInterrupted())
            return;
        if (!waitFlag.get() && updateLists()) {
            newFlag.set(true);
        }
    }

    private void performLeaderElection() {
        try {
            repConsumer.subscribe(Arrays.asList(Utils.REP_TOPIC), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    if (partitions.contains(new TopicPartition(Utils.REP_TOPIC, 0))) {
                        handleLeadershipLoss();
                    }
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    if (partitions.contains(new TopicPartition(Utils.REP_TOPIC, 0))) {
                        handleLeadershipAcquisition();
                    }
                }
            });

            // Maintain leadership by periodic poll 
            while (running && !Thread.currentThread().isInterrupted()) {
                ConsumerRecords<String, String> records = repConsumer.poll(Duration.ofMillis(200));
            }
        } catch (WakeupException e) {
        } catch (Exception e) {
        }
    }

    private synchronized void handleLeadershipAcquisition() {
        if (!isLeader.get()) {
            if (isRepTopicEmpty()) {
                publishNew();
            } else {
                applyRep();
                if (waitFlag.get()) {
                    publishNew();
                } else {
                    publishNormal();
                }
                startPeriodicMarker();
            }
            isLeader.set(true);
        }
    }

    private synchronized void handleLeadershipLoss() {
        isLeader.set(false);
        cancelTimer(mTimer);
        cancelTimer(ackTimer);
        cancelTimer(monitorTimer);
    }

    private boolean isRepTopicEmpty() {
        TopicPartition repPartition = new TopicPartition(Utils.REP_TOPIC, 0);
        Map<TopicPartition, Long> beginning = repConsumer.beginningOffsets(Collections.singleton(repPartition));
        Map<TopicPartition, Long> end = repConsumer.endOffsets(Collections.singleton(repPartition));
        return beginning.get(repPartition).equals(end.get(repPartition));
    }

    private void applyRep() {
        TopicPartition repPartition = new TopicPartition(Utils.REP_TOPIC, 0);
        repConsumer.seekToEnd(Arrays.asList(repPartition));
        long lastOffset = repConsumer.position(repPartition) - 1;

        if (lastOffset >= 0) {
            repConsumer.seek(repPartition, lastOffset);
            ConsumerRecords<String, String> records = repConsumer.poll(Duration.ofMillis(100));
            ConsumerRecord<String, String> record = records.iterator().next();
            String[] parts = record.value().split("\\|", 4);
            if (parts.length != 4) {
                throw new IllegalStateException("Invalid REP record format");
            }
            m.set(Integer.parseInt(parts[0]));
            newm.set(Integer.parseInt(parts[1]));
            sId.set(Integer.parseInt(parts[2]));
            // parts[3] contains the previous leaderId
            if ("NEW".equals(record.key())) {
                waitFlag.set(true);
            }
        }
    }

    private boolean updateLists() {
        try {
            boolean delta = false;
            ListTopicsOptions listTopicsOptions = new ListTopicsOptions().listInternal(false); // Exclude internal
            ListTopicsResult topicsResult = adminClient.listTopics(listTopicsOptions);
            Set<String> currentTopics = topicsResult.names().get();

            DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(currentTopics);
            Map<String, KafkaFuture<TopicDescription>> topicDescriptions = describeTopicsResult.topicNameValues();
            for (Map.Entry<String, KafkaFuture<TopicDescription>> entry : topicDescriptions.entrySet()) {
                String topic = entry.getKey();
                if (Utils.isInternalTopic(topic))
                    continue;
                int n = entry.getValue().get().partitions().size();
                System.out.println("topic: " + topic + " has: " + n + " partitions.");
                Integer prev = currentPartitions.get(topic);
                if (prev == null || n > prev) {
                    currentPartitions.put(topic, n);
                    delta = true;
                }
            }
            if (delta) {
                System.out.println("lists are updated.");
            } else {
                System.out.println("lists are not changed.");
            }
            return delta;
        } catch (Exception e) {
            return false;
        }
    }

    private class AckEvent {
        final String topic;
        final int partition;
        final long markerId;

        AckEvent(String topic, int partition, long markerId) {
            this.topic = topic;
            this.partition = partition;
            this.markerId = markerId;
        }
    }

    private void startAckSystem() {
        ackExecutor.execute(() -> {
            while (running && !Thread.currentThread().isInterrupted()) {
                try {
                    processAckEvents();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
    }

    private void processAckEvents() throws InterruptedException {
        AckEvent event = ackQueue.poll(100, TimeUnit.MILLISECONDS);
        
        if (event != null) {
            //System.out.println("Event: " + event.topic + " " + event.partition + " " + event.markerId);
            updateAck(event);
            checkAck();
        }
    }

    private void updateAck(AckEvent event) {
        ack.computeIfAbsent(event.topic, k -> new ConcurrentHashMap<>())
                .merge(event.partition, event.markerId, Math::max);
    }

    private void checkAck() {
        long targetMarker = waitFlag.get() ? newm.get() : sId.get();
        Map<String, Map<Integer, Long>> snapshot = new HashMap<>();
        ack.forEach((topic, partitions) -> snapshot.put(topic, new HashMap<>(partitions)));
        boolean allAcked = allGE(snapshot, targetMarker);

        if (allAcked) {
            handleAllAcked();
        }
    }

    private static boolean allGE(Map<String, Map<Integer, Long>> ack, long threshold) {
        for (Map<Integer, Long> partitions : ack.values()) {
            for (Long value : partitions.values()) {
                if (value < threshold) {
                    return false;
                }
            }
        }
        return true;
    }

    private void handleAllAcked() {
        // new marker 
        if (waitFlag.get()) {
            cancelTimer(ackTimer);
            waitFlag.set(false);
            producer.send(new ProducerRecord<>(Utils.STABLE_TOPIC, 0, newm.get(), Utils.NEW_KEY, leaderId));
            publishRep("STABLE");
            newm.incrementAndGet();
            //System.out.println("new value = " + newm.get());
        } else {
            long tmp = smallestAck(ack);
            sId.set(tmp);
            producer.send(new ProducerRecord<>(Utils.STABLE_TOPIC, 0, sId.get(), Utils.SID_KEY, leaderId));
            publishRep("STABLE");
            //System.out.println("af sId value = " + sId.get());
        }
        resetAck();
    }

    private static Long smallestAck(ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long>> ack) {
        Long min = Long.MAX_VALUE;
        for (Map<Integer, Long> partitions : ack.values()) {
            for (Long value : partitions.values()) {
                if (value < min) {
                    min = value;
                }
            }
        }
        return min;
    }

    private void handleAckTimeout() {
        if (!isLeader.get() || !waitFlag.get())
            return;
        retryCnt.incrementAndGet();
        Map<String, Integer> snapshot = new HashMap<>(currentPartitions);
        snapshot.forEach((topic, partitions) -> {
            ConcurrentHashMap<Integer, Long> topicAcks = ack.get(topic);
            for (int p = 0; p < partitions; p++) {
                long currentAck = topicAcks.getOrDefault(p, -1L);
                if (currentAck < newm.get()) {
                    producer.send(new ProducerRecord<>(topic, p, newm.get(), Utils.MARKER_KEY, leaderId));
                }
            }
        });

        if (waitFlag.get()) {
            if (retryCnt.get() == rMax)
                retryCnt.set(0);
            scheduleAckTimeout();
        }
    }

    private void resetAck() {
        Map<String, Integer> snapshot = new HashMap<>(currentPartitions);
        snapshot.forEach((topic, count) -> {
            ack.compute(topic, (k, partitionMap) -> {
                if (partitionMap == null) {
                    partitionMap = new ConcurrentHashMap<>();
                }
                for (int p = 0; p < count; p++) {
                    partitionMap.put(p, -1L);
                }
                return partitionMap;
            });
        });
    }

    private void startPeriodicMarker() {
        mTimer = markerScheduler.scheduleAtFixedRate(this::periodicMarker, deltaM, deltaM, TimeUnit.MILLISECONDS);
    }

    private void startMonitor() {
        monitorTimer = partitionMonitor.scheduleWithFixedDelay(this::partitionMonitor, deltaMonitor, deltaMonitor,
                TimeUnit.SECONDS);
    }

    private void scheduleAckTimeout() {
        cancelTimer(ackTimer);
        ackTimer = ackTimeoutScheduler.scheduleAtFixedRate(this::handleAckTimeout, deltaAck, deltaAck,
                TimeUnit.MILLISECONDS);
    }

    private void cancelTimer(ScheduledFuture<?> f) {
        if (f != null)
            f.cancel(false); // do not interrupt an already running task 
    }

    private void periodicMarker() {
        if (!isLeader.get())
            return;
        if (newFlag.get() && !waitFlag.get()) {
            publishNew();
        } else if (!waitFlag.get()) {
            publishNormal();
        }
    }

    private void publishNew() {
        waitFlag.set(true);
        newFlag.set(false);
        resetAck();
        publishRep("NEW");
        retryCnt.set(0);
        scheduleAckTimeout();
        publishAll(Utils.NEW_KEY, newm.get());
        publishRep("MARKER");
    }

    private void publishNormal() {
        publishAll(Utils.MARKER_KEY, m.get());
        publishRep("MARKER");
        m.incrementAndGet();
    }

    private void publishAll(String type, long markerId) {
        currentPartitions.forEach((topic, count) -> {
            for (int p = 0; p < count; p++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, p, markerId, type, leaderId);
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        System.err.printf("Failed to send %s marker %d to %s-%d: %s%n",
                                type, metadata.timestamp(), metadata.topic(), metadata.partition(),
                                exception.getMessage());
                        return;
                    }
                    //System.out.println("metadata: " + metadata.topic() + " " + metadata.partition() + " " + metadata.timestamp());
                    ackQueue.offer(new AckEvent(metadata.topic(), metadata.partition(), metadata.timestamp()));

                });
            }
        });
    }

    private void publishRep(String type) {
        long mVal, newmVal, sIdVal;
        synchronized (this) {
            mVal = m.get();
            newmVal = newm.get();
            sIdVal = sId.get();
        }
        String value = String.format("%d|%d|%d|%s", mVal, newmVal, sIdVal, leaderId);
        producer.send(new ProducerRecord<>(Utils.REP_TOPIC, 0, null, type, value));
    }
}
