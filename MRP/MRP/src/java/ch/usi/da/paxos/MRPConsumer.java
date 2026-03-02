package ch.usi.da.paxos;

import ch.usi.da.paxos.ring.Node;
import ch.usi.da.paxos.ring.RingDescription;
import ch.usi.da.paxos.api.PaxosRole;
import ch.usi.da.paxos.storage.Decision;
import ch.usi.da.paxos.MRPUtils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.*;

public class MRPConsumer {
    private static final String UNIQUE_ID = UUID.randomUUID().toString().substring(0, 8);
    private static int nodeId;
    private static String timestamp;
    private static String MESSAGE_STATS_PATH;
    private static String OFFSET_PATH;

    private static final int BATCH_SIZE = 100_000;
    private static final long FLUSH_INTERVAL_MS = 10_000;
    static final int HISTO_BUCKETS = 1000_000;

    private static List<RingDescription> rings = new ArrayList<>();
    private static Node node;

    private static class ConsumerStat {
        int warmUpCount;
        int normalCount;
        long startConsumeTime;
        long endWarmUpTime;
        long startNormalTime;
        long endConsumeTime;


        ConsumerStat(int warmUpCount, int normalCount, long startConsumeTime, long endWarmUpTime, long startNormalTime, long endConsumeTime) {
            this.warmUpCount = warmUpCount;
            this.normalCount = normalCount;
            this.startConsumeTime = startConsumeTime;
            this.endWarmUpTime = endWarmUpTime;
            this.startNormalTime = startNormalTime;
            this.endConsumeTime = endConsumeTime;
        }
    }

    private interface RecordStat {
    }

    private static final class MsgRec implements RecordStat {
        final int ringId;
        final long instance;
        final String key;
        final long pTimestamp;
        final long cTimestamp;
        final long latencyMs;

        MsgRec(int ringId, long instance, String key, long pTimestamp, long cTimestamp, long latencyMs) {
            this.ringId = ringId;
            this.instance = instance;
            this.key = key;
            this.pTimestamp = pTimestamp;
            this.cTimestamp = cTimestamp;
            this.latencyMs = latencyMs;
        }
    }

    private static final BlockingQueue<RecordStat> statQueue = new LinkedBlockingQueue<>(1_500_000);
    private static volatile boolean keepWriting = true;
    private static final Map<Integer, Long> currentInstances = new TreeMap<>();

    public static void main(String[] args) {
        long maxHeapSize = Runtime.getRuntime().maxMemory();
        System.out.println("Max Heap Size: " + (maxHeapSize / (1024 * 1024 * 1024)) + " GB");
        System.out.println("MRP_ConsumerID: " + UNIQUE_ID);

        if (args.length < 2) {
            System.out.println("Usage: MRPConsumer <nodeId> <timestamp>");
            System.exit(1);
        }

        nodeId = Integer.parseInt(args[0]);
        timestamp = args[1];

        MESSAGE_STATS_PATH = "./MRP/logs/consumer/message_stats/message_stats_" + timestamp + "_" + nodeId + ".csv";
        OFFSET_PATH = "./MRP/logs/consumer/offset/offset_" + timestamp + "_" + nodeId + ".log";

        try {
            MRPUtils.loadMRPArgs();
        } catch (Exception e) {
            System.err.println("Startup failed: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }

        try {
            initLearner();

            Thread writerThread = new Thread(new LogWriter(MESSAGE_STATS_PATH, BATCH_SIZE, FLUSH_INTERVAL_MS, HISTO_BUCKETS));
            writerThread.start();

            consume();

            keepWriting = false;
            writerThread.join();

        } catch (Exception e) {
            e.printStackTrace();
        } 
        /*
        finally {
            if (node != null) {
                try {
                    node.stop();
                } catch (Exception e) {
                    System.err.println("Error stopping node: " + e.getMessage());
                }
            }
        }
        */
    }

    private static void initLearner() {
        System.out.println("Initializing MRP_consumer/learner ...");
        try {
            // Create ring descriptions using PaxosRole.Learner
            for (int r = 0; r < MRPUtils.RING_COUNT; r++) {
                List<PaxosRole> roles = new ArrayList<>();
                roles.add(PaxosRole.Learner);
                rings.add(new RingDescription(r, roles));
                currentInstances.put(r, -1L); // Initialize instance tracking
            }

            // Create Paxos Node (learner)
            node = new Node(nodeId, MRPUtils.GROUP_ID, MRPUtils.ZK_SERVERS, rings);
            node.start();

            Runtime.getRuntime().addShutdownHook(new Thread(){
                @Override
                public void run(){
                    try { 
                        if (node != null) {
                            node.stop();
                        }
                    } catch (Exception e) {
                        System.err.println("Error during shutdown: " + e.getMessage());
                    }
                }
            });

        } catch (Exception e) {
            System.err.println("Error while initializing MRP_Consumer/learner: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void consume() {
        try {
            ConsumerStat stat = new ConsumerStat(0, 0, 0, 0, 0, 0);

            System.out.println("Starting to consume messages from " + MRPUtils.RING_COUNT + " rings...");

            while (true) {
                Decision decision = node.getLearner().getDecisions().poll(100, TimeUnit.MILLISECONDS);
                if (decision != null && !decision.getValue().isSkip()) {
                    if (stat.warmUpCount == 0 && stat.normalCount == 0){
                        stat.startConsumeTime = System.currentTimeMillis();
                    }

                    String message = new String(decision.getValue().getValue());
                    long consumerTs = System.currentTimeMillis();
                    int ringId = decision.getRing();
                    long instanceId = decision.getInstance();
                    // Parse the message: "key|producerTimestamp|payload"
                    String[] parts = message.split("\\|", 3);
                    if (parts.length >= 2) {
                        String key = parts[0];
                        long producerTs = Long.parseLong(parts[1]);
                        // Update instance tracking
                        long currentMax = currentInstances.getOrDefault(ringId,-1L);
                        if (currentMax < instanceId) {
                            currentInstances.put(ringId, instanceId);
                        }

                        if ("warmUp".equals(key)) {
                            stat.warmUpCount++;
                            if (stat.warmUpCount == MRPUtils.WARMUP_MESSAGES) {
                                stat.endWarmUpTime = consumerTs;
                            }
                        } else if ("normal".equals(key)) {
                            if (stat.normalCount == 0) {
                                stat.startNormalTime = consumerTs;
                            }
                            stat.normalCount++;
                            if (stat.normalCount == MRPUtils.TEST_MESSAGES) {
                                stat.endConsumeTime = consumerTs;
                            }
                        }
                        
                        long latencyMs = consumerTs - producerTs;
                        boolean added = statQueue.offer(new MsgRec(ringId, instanceId, key, producerTs, consumerTs, latencyMs));
                        if (!added) {
                            System.err.println("Stat queue full — message stat dropped.");
                        }
                    }
                }

                if (stat.normalCount >= MRPUtils.TEST_MESSAGES) {
                    break;
                }
            }

            long warmUpDuration = stat.endWarmUpTime - stat.startConsumeTime;
            long testDuration = stat.endConsumeTime - stat.startNormalTime;

            // Write final offsets to file
            writeOffsetsToFile(currentInstances, OFFSET_PATH);
            System.out.println("Message instances saved to: " + OFFSET_PATH);

            printConsumerLog(stat.warmUpCount, stat.normalCount, warmUpDuration, testDuration);

        } catch (InterruptedException e) {
            System.err.println("Consumption interrupted: " + e.getMessage());
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void writeOffsetsToFile(Map<Integer, Long> currentInstances, String offsetFile) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(offsetFile))) {
            writer.write(String.format("MRP_ConsumerID = %s%n", UNIQUE_ID));
            writer.write(String.format("NodeID = %d%n", nodeId));
            for (Map.Entry<Integer, Long> entry : currentInstances.entrySet()) {
                Integer ringId = entry.getKey();
                Long instance = entry.getValue();
                writer.write(String.format("Ring: %d, Instance: %d%n", ringId, instance));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
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
        System.out.println("MRP_Consumer Statistics:");
        System.out.printf("  Measured warmUp duration in sec:   %.3f%n", measuredWarmUp);
        System.out.printf("  Measured test duration in sec:     %.3f%n", measuredTest);
        System.out.printf("  WarmUp messages received:          %d%n", warmUpCounter);
        System.out.printf("  Test messages received:            %d%n", testCounter);
        System.out.printf("  Total messages received:           %d%n", warmUpCounter + testCounter);
        System.out.printf("  Expected MRP_Consumer throughput:  %.2f msg/s%n", throughput);
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
            if (v < min) min = v;
            if (v > max) max = v;
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

        void add(long valueMs) {
            int idx = (valueMs < buckets.length) ? (int) valueMs : buckets.length - 1;
            buckets[idx]++;
            total++;
        }

        int percentile(double q) {
            if (total == 0) return 0;
            long threshold = (long) Math.ceil(q * total);
            long cum = 0;
            for (int i = 0; i < buckets.length; i++) {
                cum += buckets[i];
                if (cum >= threshold) return i;
            }
            return buckets.length - 1;
        }
    }

    private static class LogWriter implements Runnable {
        private final String msgPath;
        private final int batchSize;
        private final long flushIntervalMs;

        private final OnlineSummary msgSum = new OnlineSummary();
        private final HistogramQuantiles msgH;

        public LogWriter(String msgPath, int batchSize, long flushIntervalMs, int histogramBuckets) {
            this.msgPath = msgPath;
            this.batchSize = batchSize;
            this.flushIntervalMs = flushIntervalMs;
            this.msgH = new HistogramQuantiles(histogramBuckets);
        }

        @Override
        public void run() {
            ArrayList<MsgRec> msgBuf = new ArrayList<>(batchSize);

            try (PrintWriter msgOut = new PrintWriter(new BufferedWriter(new FileWriter(msgPath)))) {

                msgOut.println("MRP_ConsumerID");
                msgOut.println(UNIQUE_ID);
                msgOut.println("#Rings,group_id,#Producers,#Consumers,#Warmup,#test,MessageSize,DelayDistribution,ProductionRate,MessageDistribution,ProbabilityThreshold,ZipfianSkew,P1_PREEXECUTION_NUMBER,P1_RESEND_TIME,CONCURRENT_VALUES,VALUE_SIZE,VALUE_COUNT,BATCH_POLICY,VALUE_RESEND_TIME,QUORUM_SIZE,STABLE_STORAGE,TCP_NODELAY,TCP_CRC,BUFFER_SIZE,LEARNER_RECOVERY,TRIM_MODULO,TRIM_QUORUM,AUTO_TRIM,MULTI_RING_LAMBDA,MULTI_RING_DELTA_T,DELIVER_SKIP_MESSAGES,MULTI_RING_START_TIME");               
                msgOut.println(String.join(", ", MRPUtils.cleanArgs));
                msgOut.println("RingId,Instance,Key,ProducerTs,ConsumerTs");

                long lastFlush = System.currentTimeMillis();

                while (keepWriting || !statQueue.isEmpty() || !msgBuf.isEmpty()) {
                    RecordStat rec = statQueue.poll(10, TimeUnit.MILLISECONDS);

                    if (rec instanceof MsgRec) {
                        MsgRec m = (MsgRec) rec;
                        msgBuf.add(m);
                        msgSum.add(m.latencyMs);
                        msgH.add(m.latencyMs);
                    }

                    boolean timeToFlush = (System.currentTimeMillis() - lastFlush) >= flushIntervalMs;

                    if (msgBuf.size() >= batchSize || timeToFlush) {
                        for (MsgRec m : msgBuf) {
                            msgOut.printf(Locale.US, "%d,%d,%s,%d,%d%n", m.ringId, m.instance, m.key, m.pTimestamp, m.cTimestamp);
                        }
                        msgOut.flush();
                        msgBuf.clear();
                    }
                    if (timeToFlush) {
                        lastFlush = System.currentTimeMillis();
                    }
                }

                // Print aggregated statistics once at the end
                printFinalStats("Message Latency (ms)", msgSum, msgH);

                System.out.println("Message stats saved to: " + msgPath);

            } catch (Exception e) {
                System.err.println("Error in LogWriter: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    private static void printFinalStats(String name, OnlineSummary s, HistogramQuantiles h) {
        if (s.count == 0) return;

        int p50 = h.percentile(0.50);
        int p95 = h.percentile(0.95);
        int p99 = h.percentile(0.99);

        System.out.printf(
                "%n%s:%n" +
                "  Min: %d ms%n" +
                "  P50: %d ms%n" +
                "  P95: %d ms%n" +
                "  P99: %d ms%n" +
                "  Max: %d ms%n" +
                "  Avg: %.3f ms%n" +
                "  StdDev: %.3f ms%n",
                name, s.min, p50, p95, p99, s.max, s.avg(), s.stdDev());
    }
}
