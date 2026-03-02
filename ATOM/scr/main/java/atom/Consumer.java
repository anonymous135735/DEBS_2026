package atom;

import org.apache.kafka.clients.consumer.*;

import java.io.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;

public class Consumer {
    private static final String UNIQUE_ID = UUID.randomUUID().toString().substring(0, 8);
    private static String consumerId;
    private static String timestamp;
    private static String LOOP_STATS_PATH;
    private static String MESSAGE_REC_STATS_PATH;
    private static String OFFSET_PATH;

    private static final int BATCH_SIZE = 100_000;
    private static final long FLUSH_INTERVAL_MS = 10_000;
    private static final int HISTO_BUCKETS = 1000_000;

    private static class ConsumerStat {
        int REC_markers; 
        int REC_warmUp;
		int REC_normal;
        long startConsumeTime;
        long endWarmUpTime;
        long endConsumeTime;

        ConsumerStat(int REC_markers, int REC_warmUp, int REC_normal, long startConsumeTime, long endWarmUpTime, long endConsumeTime) {
            this.REC_markers = REC_markers;
            this.REC_warmUp = REC_warmUp;
            this.REC_normal = REC_normal;
            this.startConsumeTime = startConsumeTime;
            this.endWarmUpTime = endWarmUpTime;
			this.endConsumeTime = endConsumeTime;
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

    private static final class LoopRec implements RecordStat {
        final long consumerTs;
        final long loopNs;
        final int rec_markers;
        final int rec_warmup;
        final int rec_normal;

        LoopRec(long consumerTs, long loopNs, int rec_markers, int rec_warmup, int rec_normal) {
            this.consumerTs = consumerTs;
            this.loopNs = loopNs;
            this.rec_markers = rec_markers;
            this.rec_warmup = rec_warmup;
            this.rec_normal = rec_normal;
        }
    }

    private static final BlockingQueue<RecordStat> statQueue = new LinkedBlockingQueue<>(1_500_000);
    private static volatile boolean keepWriting = true;
    private static final Map<String, Integer> currentPartitionMap = new TreeMap<>();
    private static final Map<String, Map<Integer, Long>> currentOffsetMap = new TreeMap<>();
    
    public static void main(String[] args) {
        long maxHeapSize = Runtime.getRuntime().maxMemory();
        System.out.println("Max Heap Size: " + (maxHeapSize / (1024 * 1024 * 1024)) + " GB");
        System.out.println("ConsumerID: " + UNIQUE_ID);

        if (args.length < 2) {
            System.out.println("Usage: Consumer <consumerId> <timestamp>");
            System.exit(1);
        }

        consumerId = args[0];
        timestamp = args[1];

        LOOP_STATS_PATH = "./ATOM/logs/loop_stats/loop_stats_" + timestamp + "_" + consumerId + ".csv";
        MESSAGE_REC_STATS_PATH = "./ATOM/logs/message_stats/message_REC_stats_" + timestamp + "_" + consumerId + ".csv";
        OFFSET_PATH = "./ATOM/logs/offset/offset_" + timestamp + "_" + consumerId + ".log";

        try {
            Utils.loadArgs();
            Utils.initTopicsPartitions(Utils.CONSUMER_TOPICS, currentPartitionMap);
            for (Map.Entry<String, Integer> entry : currentPartitionMap.entrySet()) {
                String topic = entry.getKey();
                int partitionCount = entry.getValue();
                for (int i = 0; i < partitionCount; i++) {
                    currentOffsetMap.computeIfAbsent(topic, k -> new TreeMap<>()).putIfAbsent(i, -1L);
                }
            }
        } catch (Exception e) {
            System.err.println("Startup failed: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }      

        try {

            Thread writerThread = new Thread(new LogWriter(MESSAGE_REC_STATS_PATH, LOOP_STATS_PATH, BATCH_SIZE, FLUSH_INTERVAL_MS, HISTO_BUCKETS));
            writerThread.start();
            
            Utils.waitStartSignal(UNIQUE_ID);

            consume();

            keepWriting = false;
            writerThread.join();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void countAndTagTimeAndOffset(ConsumerRecord<String, String> record, ConsumerStat stat) {
        String topic = record.topic();
        int partition = record.partition();
        long offset = record.offset();
        String key = record.key();
        long producerTs = record.timestamp();
        long consumerTs = System.currentTimeMillis();
        long latencyMs = consumerTs - producerTs;
        
        if (Utils.MARKER_KEY.equals(key) || Utils.NEW_KEY.equals(key)) {
            stat.REC_markers++;
            latencyMs = 0;
        } else if ("warmUp".equals(key)) {
            stat.REC_warmUp++;
            if (stat.REC_warmUp == Utils.WARMUP_MESSAGES) {
                stat.endWarmUpTime = consumerTs;
            }
        } else {
            stat.REC_normal++;
            if (stat.REC_normal == Utils.TEST_MESSAGES) {
                stat.endConsumeTime = consumerTs;
            }
        }

        boolean added = statQueue.offer(new MsgRec(topic, partition, offset, key, producerTs, consumerTs, latencyMs));
        if (!added) {
            System.err.println("Stat queue full — message rec dropped.");
        }

        Map<Integer, Long> partitionOffsetMap = currentOffsetMap.get(topic);
        long currentOffset = partitionOffsetMap.getOrDefault(partition, -1L);
        if (currentOffset < offset) {
            partitionOffsetMap.put(partition, offset);
        }
    }

    private static void consume() {      
        try (KafkaConsumer<String, String> consumer = Utils.createConsumer(Utils.BOOTSTRAP_SERVERS,
                "consumer-group-" + UNIQUE_ID, false)) {
            
            ConsumerStat stat = new ConsumerStat(0, 0, 0, 0, 0, 0);
            consumer.subscribe(currentPartitionMap.keySet());
            System.out.println("Subscribed to topic: " + currentPartitionMap.keySet());

            stat.startConsumeTime = System.currentTimeMillis();

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                long loopStart = System.nanoTime();
                
                if (!records.isEmpty()) {
                    for (ConsumerRecord<String, String> record : records) {
                        countAndTagTimeAndOffset(record, stat);
                    }
                }

                long loopEnd = System.nanoTime();
                long loopNs = loopEnd - loopStart;

                boolean loopAdded = statQueue.offer(new LoopRec(System.currentTimeMillis(), loopNs, stat.REC_markers, stat.REC_warmUp, stat.REC_normal));
                if (!loopAdded) {
                    System.err.println("Stat queue full — loop stat dropped.");
                }

                if (stat.REC_normal >= Utils.TEST_MESSAGES) {
                    break;
                }
            }

            long warmUpDuration = stat.endWarmUpTime - stat.startConsumeTime;
            long testDuration = stat.endConsumeTime - stat.endWarmUpTime;
            
            // Commit last offsets to the file
            writeOffsetsToFile(currentOffsetMap, OFFSET_PATH);
            System.out.println();
            System.out.println("Message offsets saved to: " + OFFSET_PATH);

            printConsumerLog(stat.REC_warmUp, stat.REC_normal, warmUpDuration, testDuration);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void writeOffsetsToFile(Map<String, Map<Integer, Long>> currentOffsetMap, String offsetPath) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(offsetPath))) {
            writer.write(String.format("ConsumerID = %s%n", UNIQUE_ID));
            for (Map.Entry<String, Map<Integer, Long>> topicEntry : currentOffsetMap.entrySet()) {
                String topic = topicEntry.getKey();
                Map<Integer, Long> partitionsMap = topicEntry.getValue();

                for (Map.Entry<Integer, Long> partitionEntry : partitionsMap.entrySet()) {
                    Integer partition = partitionEntry.getKey();
                    Long offset = partitionEntry.getValue();
                    writer.write(String.format("Topic: %s, Partition: %d, Offset: %d%n", topic, partition, offset));
                }
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
        System.out.println("Consumer Statistics:");
        System.out.printf("  Measured warmUp duration in sec:   %.3f%n", measuredWarmUp);
        System.out.printf("  Measured test duration in sec:     %.3f%n", measuredTest);
        System.out.printf("  WarmUp messages received:          %d%n", warmUpCounter);
        System.out.printf("  Test messages received:            %d%n", testCounter);
        System.out.printf("  Total messages received:           %d%n", warmUpCounter + testCounter);
        System.out.printf("  Expected Consumer throughput:      %.2f msg/s%n", throughput);
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
        private final int batchSize;
        private final long flushIntervalMs;

        private final OnlineSummary msgSum = new OnlineSummary();
        private final OnlineSummary loopSum = new OnlineSummary();
        private final HistogramQuantiles msgH;
        private final HistogramQuantiles loopH;

        public LogWriter(String msgPath, String loopPath, int batchSize, long flushIntervalMs, int histogramBuckets) {
            this.msgPath = msgPath;
            this.loopPath = loopPath;
            this.batchSize = batchSize;
            this.flushIntervalMs = flushIntervalMs;
            this.msgH = new HistogramQuantiles(histogramBuckets);
            this.loopH = new HistogramQuantiles(histogramBuckets);
        }

        @Override
        public void run() {
            ArrayList<MsgRec> msgBuf = new ArrayList<>(batchSize);
            ArrayList<LoopRec> loopBuf = new ArrayList<>(batchSize);

            try (PrintWriter msgOut = new PrintWriter(new BufferedWriter(new FileWriter(msgPath)));
                PrintWriter loopOut = new PrintWriter(new BufferedWriter(new FileWriter(loopPath)))) {

                msgOut.println("ConsumerID");
                msgOut.println(UNIQUE_ID);
                msgOut.println("#Topics,#Partitions,RF#,#Producers,#Consumers,#Warmup,#test,MessageSize,DelayDistribution,ProductionRate,MessageDistribution,ProbabilityThreshold,ZipfianSkew,ProducerTopics,ConsumerTopics,#MP,MPdeltaM,MPdeltaAck,MPrMax,MPduration");
                msgOut.println(String.join(", ", Utils.cleanArgs));
                msgOut.println("Topic,Partition,Offset,Key,ProducerTs,ConsumerTs");

                loopOut.println("ConsumerID");
                loopOut.println(UNIQUE_ID);
                loopOut.println("#Topics,#Partitions,RF#,#Producers,#Consumers,#Warmup,#test,MessageSize,DelayDistribution,ProductionRate,MessageDistribution,ProbabilityThreshold,ZipfianSkew,ProducerTopics,ConsumerTopics,#MP,MPdeltaM,MPdeltaAck,MPrMax,MPduration");
                loopOut.println(String.join(", ", Utils.cleanArgs));
                loopOut.println("ConsumerTs,LoopDurationMs,rec_markers,rec_warmUp,rec_normal");

                long lastFlush = System.currentTimeMillis();

                while (keepWriting || !statQueue.isEmpty() || !msgBuf.isEmpty() || !loopBuf.isEmpty()) {
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
                            loopOut.printf(Locale.US, "%d,%.3f,%d,%d,%d%n", l.consumerTs, loopMs, l.rec_markers, l.rec_warmup, l.rec_normal);
                        }
                        loopOut.flush();
                        loopBuf.clear();
                    }
                    if (timeToFlush)
                        lastFlush = System.currentTimeMillis();
                }

                // Print aggregated statistics once at the end
                printFinalStats("Message Latency (ms)", msgSum, msgH);
                printFinalStats("Loop Duration (ms)", loopSum, loopH);

                System.out.println();
                System.out.println("Message stats saved to: " + msgPath);
                System.out.println("Loop stats saved to: " + loopPath);

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
