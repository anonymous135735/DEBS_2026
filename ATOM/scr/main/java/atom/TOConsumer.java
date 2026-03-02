package atom;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.io.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;

public class TOConsumer {
    private static final String UNIQUE_ID = UUID.randomUUID().toString().substring(0, 8);
    private static String consumerId;
    private static String timestamp;
    private static String LOOP_STATS_PATH;
    private static String MESSAGE_REC_STATS_PATH;
    private static String MESSAGE_DEL_STATS_PATH;
    private static String TO_OFFSET_PATH; 

    private static final int BATCH_SIZE = 100_000;
    private static final long FLUSH_INTERVAL_MS = 10_000;
    private static final int HISTO_BUCKETS = 1000_000;

    private static class TOConsumerStat {
        int REC_markers;
        int REC_warmUp; 
        int REC_normal;
        int DEL_markers;
        int DEL_warmUp;
        int DEL_normal;
        long startConsumeTime;
        long endWarmUpTime;
        long endConsumeTime;

        public TOConsumerStat(int REC_markers, int REC_warmUp, int REC_normal, int DEL_markers, int DEL_warmUp, int DEL_normal,
                long startConsumeTime, long endWarmUpTime, long endConsumeTime) {
            this.REC_markers = REC_markers;
            this.REC_warmUp = REC_warmUp;
            this.REC_normal = REC_normal;
            this.DEL_markers = DEL_markers;
            this.DEL_warmUp = DEL_warmUp;
            this.DEL_normal = DEL_normal;
            this.startConsumeTime = startConsumeTime;
            this.endWarmUpTime = endWarmUpTime;
            this.endConsumeTime = endConsumeTime;
        }
    }

    private static class NewState {
        boolean newMarkerFound;
        boolean newUpdateRequest;
        long currentNewIndex;
        int newCounter;
        int totalPartitions;

        public NewState(boolean newMarkerFound, boolean newUpdateRequest, long currentNewIndex, int newCounter,
                int totalPartitions) {
            this.newMarkerFound = newMarkerFound;
            this.newUpdateRequest = newUpdateRequest;
            this.currentNewIndex = currentNewIndex;
            this.newCounter = newCounter;
            this.totalPartitions = totalPartitions;
        }
    }

    private interface RecordStat {
    }

    private static final class MsgRec implements RecordStat {
        final String topic;
        final int partition;
        final long offset;
        final String key;
        final long pTimestamp;
        final long cTimestamp;
        final long latencyMs;

        MsgRec(String topic, int partition, long offset, String key, long pTimestamp, long cTimestamp, long latencyMs) {
            this.topic = topic;
            this.partition = partition;
            this.offset = offset;
            this.key = key;
            this.pTimestamp = pTimestamp;
            this.cTimestamp = cTimestamp;
            this.latencyMs = latencyMs;
        }
    }

    private static final class MsgDel implements RecordStat {
        final String topic;
        final int partition;
        final long offset;
        final String key;
        final long pTimestamp;
        final long cTimestamp;
        final long latencyMs;

        MsgDel(String topic, int partition, long offset, final String key, long pTimestamp, long cTimestamp, long latencyMs) {
            this.topic = topic;
            this.partition = partition;
            this.offset = offset;
            this.key = key;
            this.pTimestamp = pTimestamp;
            this.cTimestamp = cTimestamp;
            this.latencyMs = latencyMs;
        }
    }

    private static final class LoopRec implements RecordStat {
        final long consumerTs;
        final long loopNs;
        final int rec_markers;
        final int rec_warmup;
        final int rec_normal;
        final int del_markers;
        final int del_warmup;
        final int del_normal;

        LoopRec(long consumerTs, long loopNs, int rec_markers, int rec_warmup, int rec_normal, int del_markers, int del_warmup, int del_normal) {
            this.consumerTs = consumerTs;
            this.loopNs = loopNs;
            this.rec_markers = rec_markers;
            this.rec_warmup = rec_warmup;
            this.rec_normal = rec_normal;
            this.del_markers = del_markers;
            this.del_warmup = del_warmup;
            this.del_normal = del_normal; 
        }
    }

    private static final BlockingQueue<RecordStat> statQueue = new LinkedBlockingQueue<>(3_000_000);

    private static volatile boolean keepWriting = true;
    private static final Map<String, Integer> currentPartitionMap = new TreeMap<>();
    
    public static void main(String[] args) {
        long maxHeapSize = Runtime.getRuntime().maxMemory();
        System.out.println("Max Heap Size: " + (maxHeapSize / (1024 * 1024 * 1024)) + " GB");
        System.out.println("TO_ConsumerID: " + UNIQUE_ID);

        if (args.length < 2) {
            System.out.println("Usage: TOConsumer <consumerId> <timestamp>");
            System.exit(1);
        }

        consumerId = args[0];
        timestamp = args[1];

        LOOP_STATS_PATH = "./ATOM/logs/loop_stats/TO_loop_stats_" + timestamp + "_" + consumerId + ".csv";
        MESSAGE_REC_STATS_PATH = "./ATOM/logs/message_stats/TO_message_REC_stats_" + timestamp + "_" + consumerId + ".csv";
        MESSAGE_DEL_STATS_PATH = "./ATOM/logs/message_stats/TO_message_DEL_stats_" + timestamp + "_" + consumerId + ".csv";
        TO_OFFSET_PATH = "./ATOM/logs/offset/TO_offset_" + timestamp + "_" + consumerId + ".log";

        try {
            Utils.loadArgs();
            Utils.initTopicsPartitions(Utils.CONSUMER_TOPICS, currentPartitionMap);

        } catch (Exception e) {
            System.err.println("Startup failed: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }      

        try {

            Thread writerThread = new Thread(new LogWriter(MESSAGE_REC_STATS_PATH, LOOP_STATS_PATH, MESSAGE_DEL_STATS_PATH, BATCH_SIZE, FLUSH_INTERVAL_MS, HISTO_BUCKETS));
            writerThread.start();
            
            Utils.waitStartSignal(UNIQUE_ID);

            consume();

            keepWriting = false;
            writerThread.join();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static final Map<String, Map<Integer, List<ConsumerRecord<String, String>>>> eventsMap = new TreeMap<>();
    private static final Map<String, Map<Integer, Boolean>> readyMap = new TreeMap<>();
    private static final Map<String, Map<Integer, Boolean>> newMap = new TreeMap<>();
    private static final Map<String, Map<Integer, Integer>> currentIndexMap = new TreeMap<>();
    private static final Map<String, Map<Integer, Long>> currentOffsetMap = new TreeMap<>();

    private static int processCounter = 0;
    private static int detectCounter = 0;
    private static int delCounter = 0;

    private static void assignAndSeek(KafkaConsumer <String, String> consumer) {
        List<TopicPartition> partitionsToConsume = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : currentPartitionMap.entrySet()) {
            String topic = entry.getKey();
            int partitionCount = entry.getValue();
            for (int i = 0; i < partitionCount; i++) {
                partitionsToConsume.add(new TopicPartition(topic, i));
                // System.out.println("assignAndSeek: partitionsToConsume" + partitionsToConsume);
            }
        }

        consumer.assign(partitionsToConsume);

        for (TopicPartition partition : partitionsToConsume) {
            Map<Integer, Long> partitionOffsets = currentOffsetMap.get(partition.topic());
            if (partitionOffsets != null) {
                Long offset = partitionOffsets.get(partition.partition());
                if (offset != null) {
                    consumer.seek(partition, offset);
                    // System.out.println("assignAndSeek: partition and offset" + partition + "" + offset);
                }
            }
        }
    }

    private static void countAndTagTime(ConsumerRecord<String, String> record, TOConsumerStat stat) {
        String key = record.key();
        long producerTs = record.timestamp();
        long consumerTs = System.currentTimeMillis();
        long latencyMs = consumerTs - producerTs;
        
        if (Utils.MARKER_KEY.equals(key) || Utils.NEW_KEY.equals(key)) {
            stat.REC_markers++;
            latencyMs = 0;
        } else if ("warmUp".equals(key)) {
            stat.REC_warmUp++;
        } else {
            stat.REC_normal++;
        }

        boolean added = statQueue.offer(new MsgRec(record.topic(), record.partition(), record.offset(), key, producerTs, consumerTs, latencyMs));
        if (!added) {
            System.err.println("Stat queue full — message rec dropped.");
        }
    }

    private static void addToBuffer(ConsumerRecords<String, String> records, TOConsumerStat stat) {
        for (ConsumerRecord<String, String> record : records) {
            String topic = record.topic();
            int partition = record.partition();

            initPartitionMaps(topic, partition);
            countAndTagTime(record, stat);
            // Add the record
            eventsMap.get(topic).get(partition).add(record);

            /*
             * System.out.println("Topic: " + record.topic() + " Partition: " +
             * record.partition() + " Offset: "
             * + record.offset() + " Key:" + record.key() + " Value: " + record.value() +
             * " Timestamp: "
             * + record.timestamp());
             */

            // System.out.println("evntsMap" + eventsMap);
        }
    }

    private static void processEvents(NewState newState) {
        processCounter++;
        for (Map.Entry<String, Map<Integer, List<ConsumerRecord<String, String>>>> topicEntry : eventsMap.entrySet()) {
            String topic = topicEntry.getKey();
            Map<Integer, List<ConsumerRecord<String, String>>> partitionsMap = topicEntry.getValue();
            Map<Integer, Boolean> topicReadyMap = readyMap.get(topic);
            Map<Integer, Long> topicOffsetMap = currentOffsetMap.get(topic);

            for (Map.Entry<Integer, List<ConsumerRecord<String, String>>> partitionEntry : partitionsMap.entrySet()) {
                Integer partition = partitionEntry.getKey();
                List<ConsumerRecord<String, String>> records = partitionEntry.getValue();

                // Skip if the partition is ready to deliver or the list is empty
                if (Boolean.TRUE.equals(topicReadyMap.get(partition)) || records.isEmpty()) {
                    continue;
                }

                // sort the records by offset
                records.sort(Comparator.comparingLong(ConsumerRecord::offset));

                // check if the first offset matches the stored offset
                long currentOffset = topicOffsetMap.getOrDefault(partition, -1L);
                if (records.get(0).offset() == 0L || records.get(0).offset() == currentOffset + 1) {
                    // System.out.println("Topic: " + topic + " Partition: " + partition + " Offset:
                    // " + records.get(0).offset());
                    // System.out.println(currentOffsetMap.get(topic).get(partition));
                    detectMarkers(topic, partition, records, newState);
                }
            }
        }
    }

    private static void detectMarkers(String topic, Integer partition, List<ConsumerRecord<String, String>> records,
            NewState newState) {
        boolean firstMarkerFound = false;
        boolean secondMarkerFound = false;
        long previousOffset = -1; // To track the offset between records
        boolean gapDetected = false; // To track if gaps are detected between markers

        detectCounter++;

        for (int i = 0; i < records.size(); i++) {
            ConsumerRecord<String, String> record = records.get(i);
            String recordKey = record.key();
            long recordOffset = record.offset();

            if (!firstMarkerFound) {
                if (Utils.MARKER_KEY.equals(recordKey) || Utils.NEW_KEY.equals(recordKey)) {
                    // logMarkerInfo(topic, partition, record.offset(), "First");
                    firstMarkerFound = true;
                    previousOffset = recordOffset;
                    if (Utils.NEW_KEY.equals(recordKey)) {
                        handleNewMarker(record, newState);
                    }
                    continue;
                }
            } else if (!secondMarkerFound) {
                // Check for gaps after the first marker
                if (previousOffset != -1 && recordOffset - previousOffset > 1) {
                    System.out.println("Gap detected between offsets: " + previousOffset + " and " + recordOffset);
                    gapDetected = true;
                }

                previousOffset = recordOffset;

                // Check for the second marker
                if (isSecondMarker(record, newState)) {
                    secondMarkerFound = true;
                    currentIndexMap.get(topic).put(partition, i); // Store the index of the second marker
                }
            }

            // Stop processing if both markers are found
            if (firstMarkerFound && secondMarkerFound) {
                break;
            }
        }

        updateReadyState(topic, partition, firstMarkerFound, secondMarkerFound, gapDetected, newState);
    }

    private static void logMarkerInfo(String topic, Integer partition, Long offset, String markerType) {
        System.out.println(
                "Topic: " + topic + " Partition: " + partition + " Offset: " + offset + "Marker Type: " + markerType);
    }

    private static void handleNewMarker(ConsumerRecord<String, String> record, NewState newState) {
        if (!newState.newMarkerFound) {
            newState.newMarkerFound = true;
            newState.currentNewIndex = record.timestamp();
        }

        newMap.get(record.topic()).put(record.partition(), true);
        // System.out.println("The first marker is new and has the value: " +
        // record.timestamp());
    }

    private static boolean isSecondMarker(ConsumerRecord<String, String> record, NewState newState) {
        if (Utils.MARKER_KEY.equals(record.key())) {
            // logMarkerInfo(record.topic(), record.partition(), record.offset(), "Second");
            return true;
        }

        if (Utils.NEW_KEY.equals(record.key())) {
            // System.out.println("The second marker is new and has the value: " +
            // record.timestamp());
            boolean isNewMarked = newMap.get(record.topic()).get(record.partition());
            return !isNewMarked || record.timestamp() != newState.currentNewIndex;
        }
        return false;
    }

    private static void updateReadyState(String topic, Integer partition, boolean firstMarkerFound,
            boolean secondMarkerFound, boolean gapDetected, NewState newState) {
        if (firstMarkerFound && secondMarkerFound && !gapDetected) {
            if (newMap.get(topic).get(partition)) {
                newState.newCounter++;
            }

            readyMap.get(topic).put(partition, true); // No gaps, partition is ready
        } else {
            readyMap.get(topic).put(partition, false); // Gaps detected or markers are missing
        }
    }

    private static boolean checkReadyToDeliver() {
        for (Map<Integer, Boolean> partitions : readyMap.values()) {
            for (Boolean value : partitions.values()) {
                if (Boolean.FALSE.equals(value)) {
                    return false;
                }
            }
        }
        return true;
    }

    private static void deliverEvents(TOConsumerStat stat, NewState newState) throws IOException {
        try {

            boolean deliverAll = false;

            if (newState.newUpdateRequest && newState.newCounter == newState.totalPartitions) {
                deliverAll = true;
                newState.newMarkerFound = false;
                newState.newUpdateRequest = false;
                newState.newCounter = 0;
                newState.currentNewIndex ++;
            }

            for (Map.Entry<String, Map<Integer, List<ConsumerRecord<String, String>>>> topicsEntry : eventsMap
                    .entrySet()) {
                String topic = topicsEntry.getKey();
                Map<Integer, List<ConsumerRecord<String, String>>> partitionsMap = topicsEntry.getValue();

                for (Map.Entry<Integer, List<ConsumerRecord<String, String>>> partitionEntry : partitionsMap
                        .entrySet()) {
                    Integer partition = partitionEntry.getKey();

                    if (deliverAll || Boolean.FALSE.equals(newMap.get(topic).get(partition))) {
                        // System.out.println("deliverAll: " + deliverAll + " New Topic: " + topic + "
                        // Partition: " + partition + " is: " + newMap.get(topic).get(partition));

                        // reset new stat
                        if (deliverAll) {
                            newMap.get(topic).put(partition, false);
                        }

                        int lastIndex = currentIndexMap.get(topic).get(partition);
                        List<ConsumerRecord<String, String>> records = partitionEntry.getValue().subList(0, lastIndex);

                        delCounter++;
                        // System.out.println("records size: " + lastIndex + " counter: " + delCounter);

                        for (int i = 0; i < lastIndex; i++) {
                            ConsumerRecord<String, String> record = records.get(i);

                            // Deliver records without markers (if the first marker is not at index 0)
                            normalDeliver(record, stat);

                            if (i == lastIndex - 1) {
                                // Save the offset of last event
                                currentOffsetMap.get(topic).put(partition, record.offset());
                            }
                        }

                        // Clear readyToDeliverMap
                        readyMap.get(topic).put(partition, false);

                        // Remove from the buffer
                        records.clear();

                        /*
                         * System.out.println("Events delivered from Topic: " + topic + " Partition: " +
                         * partition);
                         */
                    }
                }
            }

            // Commit last offsets + the current new partitons index to the file
            writeOffsetsToFile(currentOffsetMap, TO_OFFSET_PATH, newState);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void normalDeliver(ConsumerRecord<String, String> record, TOConsumerStat stat) throws IOException {
        String key = record.key();
        long producerTs = record.timestamp();
        long consumerTs = System.currentTimeMillis();
        long latencyMs = consumerTs - producerTs;
        stat.endConsumeTime = consumerTs;

        if (Utils.MARKER_KEY.equals(key) || Utils.NEW_KEY.equals(key)) {
            stat.DEL_markers++;
        } else {
            if ("warmUp".equals(key)) {
                stat.DEL_warmUp++;
                if (stat.DEL_warmUp == Utils.WARMUP_MESSAGES) {
                    stat.endWarmUpTime = System.currentTimeMillis();
                }
            } else {
                stat.DEL_normal++;
            }

            boolean added = statQueue.offer(new MsgDel(record.topic(), record.partition(), record.offset(), key, producerTs, consumerTs, latencyMs));
            if (!added) {
                System.err.println("Stat queue full — message del dropped.");
            }

            Map<Integer, Long> partitionOffsetMap = currentOffsetMap.get(record.topic());
            long currentOffset = partitionOffsetMap.getOrDefault(record.partition(), -1L);
            if (currentOffset < record.offset()) {
                partitionOffsetMap.put(record.partition(), record.offset());
            }
        }
    }

    private static void updatePartitions(KafkaConsumer<String, String> consumer, NewState newState) {
        List<TopicPartition> newPartitionsToConsume = new ArrayList<>();
        boolean assign = false;

        for (Map.Entry<String, Integer> entry : currentPartitionMap.entrySet()) {
            String topic = entry.getKey();
            List<PartitionInfo> partitions = consumer.partitionsFor(topic);

            if (partitions != null) {
                int currentNumber = currentPartitionMap.getOrDefault(topic, 0);
                // Only assign if there are new partitions
                if (currentNumber < partitions.size()) {
                    assign = true;
                    for (int i = currentNumber; i < partitions.size(); i++) {
                        newPartitionsToConsume.add(new TopicPartition(topic, i));
                    }
                    // Update the topicPartitions
                    entry.setValue(partitions.size());
                }
            } else {
                System.err.println("Null partitions upon update");
            }
        }

        if (assign) {
            newState.totalPartitions = currentPartitionMap.values().stream().mapToInt(Integer::intValue).sum();
            consumer.assign(newPartitionsToConsume);
        }
        // System.out.println("assign: " + assign);
    }

    private static void consume() {
        try (KafkaConsumer<String, String> consumer = Utils.createConsumer(Utils.BOOTSTRAP_SERVERS,
                "TO-consumer-group-" + UNIQUE_ID, false)) {

            initMaps();
            long currentNewIndex = 0;
            // currentNewIndex = loadOffsetsFromFile(TO_OFFSET_PATH, currentOffsetMap);
            // System.out.println(currentOffsetMap);

            int totalPartitions = currentPartitionMap.values().stream().mapToInt(Integer::intValue).sum();
            NewState newState = new NewState(false, true, currentNewIndex, 0, totalPartitions);
            TOConsumerStat stat = new TOConsumerStat(0, 0, 0, 0, 0, 0, 0, 0, 0);
            
            assignAndSeek(consumer);
            System.out.println("Subscribed to topic: " + currentPartitionMap.keySet());
            
            stat.startConsumeTime = System.currentTimeMillis();
 
            while (true) {               
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                long loopStart = System.nanoTime();

                if (!records.isEmpty()) {
                    // System.out.println("received");
                    addToBuffer(records, stat);
                    // System.out.println("buffered");
                    processEvents(newState);
                    // System.out.println("processed");
                    // if (checkReadyToDeliver()) {
                    while (checkReadyToDeliver()) {
                        // System.out.println("Ready");
                        deliverEvents(stat, newState);
                        // System.out.println("delivered");
                        processEvents(newState);
                    }
                    
                    if (newState.newMarkerFound && !newState.newUpdateRequest) {
                        // System.out.println("request update");
                        newState.newUpdateRequest = true;
                        updatePartitions(consumer, newState);
                    }
                }

                long loopEnd = System.nanoTime();
                long loopNs = loopEnd - loopStart;

                boolean loopAdded = statQueue.offer(new LoopRec(System.currentTimeMillis(), loopNs, stat.REC_markers, stat.REC_warmUp, stat.REC_normal, stat.DEL_markers, stat.DEL_warmUp, stat.DEL_normal));
                if (!loopAdded) {
                    System.err.println("Stat queue full — loop stat dropped.");
                }

                if (stat.DEL_normal >= Utils.TEST_MESSAGES) {
                    break;
                }
            }

            long warmUpDuration = stat.endWarmUpTime - stat.startConsumeTime;
            long testDuration = stat.endConsumeTime - stat.endWarmUpTime;
            
            System.out.println();
            System.out.println("Message offsets saved to: " + TO_OFFSET_PATH);
            
            printConsumerLog(stat.DEL_warmUp, stat.DEL_normal, warmUpDuration, testDuration);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void writeOffsetsToFile(Map<String, Map<Integer, Long>> currentOffsetMap, String offsetPath, NewState newState) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(offsetPath))) {
            writer.write(String.format("TO_ConsumerID = %s%n", UNIQUE_ID));
            for (Map.Entry<String, Map<Integer, Long>> topicEntry : currentOffsetMap.entrySet()) {
                String topic = topicEntry.getKey();
                Map<Integer, Long> partitionsMap = topicEntry.getValue();

                for (Map.Entry<Integer, Long> partitionEntry : partitionsMap.entrySet()) {
                    Integer partition = partitionEntry.getKey();
                    Long offset = partitionEntry.getValue();
                    writer.write(String.format("Topic: %s, Partition: %d, Offset: %d%n", topic, partition, offset));
                }
            }

            if (newState != null) {
                writer.write(String.format("Current new partitions index: %d%n", newState.currentNewIndex));
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static long loadOffsetsFromFile(String offsetPath, Map<String, Map<Integer, Long>> currentOffsetMap) {
        long currentNewIndex = 0;
        try (BufferedReader reader = new BufferedReader(new FileReader(offsetPath))) {
            String line;
            while ((line = reader.readLine()) != null) {
            // Expected format: "Topic: topic_name, Partition: partition_number, Offset: offset_value"
                if (line.startsWith("Topic:")) {
                    String[] parts = line.split(", ");
                    if (parts.length == 3) {
                        String topic = parts[0].split(": ")[1].trim();
                        int partition = Integer.parseInt(parts[1].split(": ")[1].trim());
                        long offset = Long.parseLong(parts[2].split(": ")[1].trim());

                        currentOffsetMap.computeIfAbsent(topic, k -> new TreeMap<>()).put(partition, offset);
                    }
                } else if (line.startsWith("Current new partitions index:")) {
                    currentNewIndex = Long.parseLong(line.split(": ")[1].trim());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return currentNewIndex;
    }

    private static void printConsumerLog(
            long warmUpCounter,
            long testCounter,
            long warmUpDuration,
            long testDuration) {

        double throughput = testCounter / (testDuration / 1000.0);
        double measuredWarmUp = warmUpDuration / 1000.0;
        double measuredTest = testDuration / 1000.0;

        System.out.println();
        System.out.println("TO_Consumer Statistics:");
        System.out.printf("  Measured warmUp duration in sec:   %.3f%n", measuredWarmUp);
        System.out.printf("  Measured test duration in sec:     %.3f%n", measuredTest);
        System.out.printf("  WarmUp messages received:          %d%n", warmUpCounter);
        System.out.printf("  Test messages received:            %d%n", testCounter);
        System.out.printf("  Total messages received:           %d%n", warmUpCounter + testCounter);
        System.out.printf("  Expected TO_Consumer throughput:   %.2f msg/s%n", throughput);
    }

    private static void initMaps() {
        for (Map.Entry<String, Integer> entry : currentPartitionMap.entrySet()) {
            String topic = entry.getKey();
            int partitionCount = entry.getValue();

            for (int i = 0; i < partitionCount; i++) {
                initPartitionMaps(topic, i);
            }
        }
    }

    private static void initPartitionMaps(String topic, int partition) {
        eventsMap.computeIfAbsent(topic, k -> new TreeMap<>()).computeIfAbsent(partition, p -> new ArrayList<>());
        readyMap.computeIfAbsent(topic, k -> new TreeMap<>()).putIfAbsent(partition, false);
        // If the new partition is detected before the new marker, tag it as new
        newMap.computeIfAbsent(topic, k -> new TreeMap<>()).putIfAbsent(partition, true);
        currentIndexMap.computeIfAbsent(topic, k -> new TreeMap<>()).putIfAbsent(partition, 0);
        currentOffsetMap.computeIfAbsent(topic, k -> new TreeMap<>()).putIfAbsent(partition, 0L);
    }

    // Online summary stats (mean/stddev/min/max) using Welford
    static final class OnlineSummary {
        long count = 0;
        double mean = 0.0;
        double m2 = 0.0;
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;

        void add(long v) {
            count++;
            double delta = v - mean;
            mean += delta / count;
            m2 += delta * (v - mean);
            if (v < min)
                min = v;
            if (v > max)
                max = v;
        }

        double avg() {
            return mean;
        }

        double stdDev() {
            return count > 1 ? Math.sqrt(m2 / (count - 1)) : 0.0;
        }
    }

    // Fixed-width histogram for approximate quantiles
    static final class HistogramQuantiles {
        final int[] buckets;
        long total = 0;

        HistogramQuantiles(int bucketCount) {
            this.buckets = new int[bucketCount];
        }

        // Value must be in milliseconds. Last bucket captures overflow.
        void add(long valueMs) {
            int idx = (valueMs < buckets.length) ? (int) valueMs : buckets.length - 1;
            buckets[idx]++;
            total++;
        }

        // Returns bucket index (ms) for the percentile.
        int percentile(double q) {
            if (total == 0)
                return 0;
            long threshold = (long) Math.ceil(q * total);
            long cum = 0;
            for (int i = 0; i < buckets.length; i++) {
                cum += buckets[i];
                if (cum >= threshold)
                    return i;
            }
            return buckets.length - 1;
        }
    }

    private static class LogWriter implements Runnable {
        private final String msgPath;
        private final String loopPath;
        private final String delPath;
        private final int batchSize;
        private final long flushIntervalMs;

        private final OnlineSummary msgSum = new OnlineSummary();
        private final OnlineSummary loopSum = new OnlineSummary();
        private final OnlineSummary delSum = new OnlineSummary();
        
        private final HistogramQuantiles msgH;
        private final HistogramQuantiles loopH;
        private final HistogramQuantiles delH;

        public LogWriter(String msgPath, String loopPath, String delPath, int batchSize, long flushIntervalMs, int histogramBuckets) {
            this.msgPath = msgPath;
            this.loopPath = loopPath;
            this.delPath = delPath;
            this.batchSize = batchSize;
            this.flushIntervalMs = flushIntervalMs;
            this.msgH = new HistogramQuantiles(histogramBuckets);
            this.loopH = new HistogramQuantiles(histogramBuckets);
            this.delH = new HistogramQuantiles(histogramBuckets);
        }

        @Override
        public void run() {
            ArrayList<MsgRec> msgBuf = new ArrayList<>(batchSize);
            ArrayList<LoopRec> loopBuf = new ArrayList<>(batchSize);
            ArrayList<MsgDel> delBuf = new ArrayList<>(batchSize);

            try (PrintWriter msgOut = new PrintWriter(new BufferedWriter(new FileWriter(msgPath)));
                PrintWriter loopOut = new PrintWriter(new BufferedWriter(new FileWriter(loopPath)));
                PrintWriter delOut = new PrintWriter(new BufferedWriter(new FileWriter(delPath)))) {

                msgOut.println("TO_ConsumerID");
                msgOut.println(UNIQUE_ID);
                msgOut.println("#Topics,#Partitions,RF#,#Producers,#Consumers,#Warmup,#test,MessageSize,DelayDistribution,ProductionRate,MessageDistribution,ProbabilityThreshold,ZipfianSkew,ProducerTopics,ConsumerTopics,#MP,MPdeltaM,MPdeltaAck,MPrMax,MPduration");
                msgOut.println(String.join(", ", Utils.cleanArgs));
                msgOut.println("Topic,Partition,Offset,Key,ProducerTs,ConsumerTs");

                loopOut.println("TO_ConsumerID");
                loopOut.println(UNIQUE_ID);
                loopOut.println("#Topics,#Partitions,RF#,#Producers,#Consumers,#Warmup,#test,MessageSize,DelayDistribution,ProductionRate,MessageDistribution,ProbabilityThreshold,ZipfianSkew,ProducerTopics,ConsumerTopics,#MP,MPdeltaM,MPdeltaAck,MPrMax,MPduration");
                loopOut.println(String.join(", ", Utils.cleanArgs));
                loopOut.println("ConsumerTs,LoopDurationMs,rec_markers,rec_warmUp,rec_normal,del_markers,del_warmUp,del_normal");
                
                delOut.println("TO_ConsumerID");
                delOut.println(UNIQUE_ID);
                delOut.println("#Topics,#Partitions,RF#,#Producers,#Consumers,#Warmup,#test,MessageSize,DelayDistribution,ProductionRate,MessageDistribution,ProbabilityThreshold,ZipfianSkew,ProducerTopics,ConsumerTopics,#MP,MPdeltaM,MPdeltaAck,MPrMax,MPduration");
                delOut.println(String.join(", ", Utils.cleanArgs));
                delOut.println("Topic,Partition,Offset,Key,ProducerTs,ConsumerTs");

                long lastFlush = System.currentTimeMillis();

                while (keepWriting || !statQueue.isEmpty() || !msgBuf.isEmpty() || !loopBuf.isEmpty() || !delBuf.isEmpty()) {
                    // remove the head element from the queue 
                    RecordStat rec = statQueue.poll(10, TimeUnit.MILLISECONDS);

                    if (rec instanceof MsgRec) {
                        MsgRec m = (MsgRec) rec;
                        msgBuf.add(m);
                        if ("warmUp".equals(m.key) || "normal".equals(m.key)) {
                            msgSum.add(m.latencyMs);
                            msgH.add(m.latencyMs);
                        }
                    } else if (rec instanceof LoopRec) {
                        LoopRec l = (LoopRec) rec;
                        loopBuf.add(l);
                        loopSum.add(l.loopNs);
                        loopH.add(l.loopNs);
                    } else if (rec instanceof MsgDel) {
                        MsgDel d = (MsgDel) rec;
                        delBuf.add(d);
                        delSum.add(d.latencyMs);
                        delH.add(d.latencyMs);
                    }

                    boolean timeToFlush = (System.currentTimeMillis() - lastFlush) >= flushIntervalMs;

                    if (msgBuf.size() >= batchSize || timeToFlush) {
                        for (MsgRec m : msgBuf) {
                            msgOut.printf(Locale.US, "%s,%d,%d,%s,%d,%d%n", m.topic, m.partition, m.offset, m.key, m.pTimestamp, m.cTimestamp);
                        }
                        msgOut.flush();
                        msgBuf.clear();
                    }
                    
                    if (loopBuf.size() >= batchSize || timeToFlush) {
                        for (LoopRec l : loopBuf) {
                            double loopMs = l.loopNs / 1_000_000.0;
                            loopOut.printf(Locale.US, "%d,%.3f,%d,%d,%d,%d,%d,%d%n", l.consumerTs, loopMs, l.rec_markers, l.rec_warmup, l.rec_normal, l.del_markers, l.del_warmup, l.del_normal);
                        }
                        loopOut.flush();
                        loopBuf.clear();
                    }
                    
                    if (delBuf.size() >= batchSize || timeToFlush) {
                        for (MsgDel d : delBuf) {
                            delOut.printf(Locale.US, "%s,%d,%d,%s,%d,%d%n", d.topic, d.partition, d.offset, d.key, d.pTimestamp, d.cTimestamp);
                        }
                        delOut.flush();
                        delBuf.clear();
                    }

                    if (timeToFlush)
                        lastFlush = System.currentTimeMillis();
                }

                // Print aggregated statistics once at the end
                printFinalStats("Message Receiving Latency (ms)", msgSum, msgH);
                printFinalStats("Loop Duration (ms)", loopSum, loopH);
                printFinalStats("Message Delivery Latency (ms)", delSum, delH);

                System.out.println();
                System.out.println("Message Receiving stats saved to: " + msgPath);
                System.out.println("Loop stats saved to: " + loopPath);
                System.out.println("Message Delivery stats saved to: " + delPath);

            } catch (Exception e) {
                System.err.println("Error in LogWriter: " + e.getMessage());
            }
        }
    }

    private static void printFinalStats(String name, OnlineSummary s, HistogramQuantiles h) {
        if (s.count == 0)
            return;

        boolean isLoopStats = name.contains("Loop Duration");
        double scale = isLoopStats ? 1_000_000.0 : 1.0;
        
        int p50 = h.percentile(0.50);
        int p95 = h.percentile(0.95);
        int p99 = h.percentile(0.99);

        double min = s.min / scale;
        double max = s.max / scale;
        double avg = s.avg() / scale;
        double std = s.stdDev() / scale;

        System.out.printf(
                "%n%s:%n" +
                "  Min: %.3f%n" +
                "  P50: %.3f%n" +
                "  P95: %.3f%n" +
                "  P99: %.3f%n" +
                "  Max: %.3f%n" +
                "  Avg: %.3f%n" +
                "  StdDev: %.3f%n",
                name, min, (p50 / scale), (p95 / scale), (p99 / scale), max, avg, std);
    }
}
