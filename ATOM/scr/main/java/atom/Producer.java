package atom;

import org.apache.kafka.clients.producer.*;
import java.util.*;
import java.util.concurrent.locks.LockSupport;

public class Producer {
    private static final String UNIQUE_ID = UUID.randomUUID().toString().substring(0, 8);
    private static int producerId;
    private static Random random = new Random();
    
    private static long MEAN_DELAY_NS;
    private static long STDDEV_DELAY_NS;
    private static long MIN_DELAY_NS;
    private static long MAX_DELAY_NS;

    private static final Map<String, Integer> currentPartitions = new TreeMap<>();

    public static void main(String[] args) {
        long maxHeapSize = Runtime.getRuntime().maxMemory();
        System.out.println("Max Heap Size: " + (maxHeapSize / (1024 * 1024 * 1024)) + " GB");
        System.out.println("ProducerID: " + UNIQUE_ID);

        if (args.length < 1) {
            System.out.println("Usage: Producer <producerId>");
            System.exit(1);
        }

        producerId = Integer.parseInt(args[0]);
        random.setSeed(((long) producerId) << 32 | producerId);
        System.out.println("Seed is set to = " + (((long) producerId) << 32 | producerId));

        try {
            Utils.loadArgs();
            Utils.initTopicsPartitions(Utils.PRODUCER_TOPICS, currentPartitions);
        } catch (Exception e) {
            System.err.println("Startup failed: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }

        char[] payload = new char[Utils.MESSAGE_SIZE_BYTES];
        Arrays.fill(payload, 'x');
        String message = new String(payload);
        try {
            Utils.waitStartSignal(UNIQUE_ID);
            produce(message);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static class ProducerStat {
        long startProduceTime;
        long endWarmUpTime;
        long endProduceTime;

        ProducerStat(long startProduceTime, long endWarmUpTime, long endProduceTime) {
            this.startProduceTime = startProduceTime;
            this.endWarmUpTime = endWarmUpTime;
			this.endProduceTime = endProduceTime;
        }
    }

    private static double[] buildZipfCdf(int partitions, double alpha) {
        double[] cdf = new double[partitions];
        double normalizer = 0.0;

        for (int i = 1; i <= partitions; i++) {
            normalizer += 1.0 / Math.pow(i, alpha);
        }

        double cumulative = 0.0;
        for (int i = 1; i <= partitions; i++) {
            cumulative += (1.0 / Math.pow(i, alpha)) / normalizer;
            cdf[i - 1] = cumulative;
        }
        return cdf;
    }

    private static int sampleZipfPartition(double[] cdf, Random random) {
        double x = random.nextDouble();
        for (int i = 0; i < cdf.length; i++) {
            if (x <= cdf[i]) {
                return i;
            }
        }
        return cdf.length - 1; // fallback
    }

    private static void produce(String message) {   
        try (KafkaProducer<String, String> producer = Utils.createProducer(Utils.BOOTSTRAP_SERVERS, false)){
            System.out.println("Publish to topic: " + currentPartitions.keySet());    
            
            ProducerStat stat = new ProducerStat(0, 0, 0);
            double[] zipfCdf = buildZipfCdf(Utils.NUMBER_OF_PARTITIONS, Utils.ZIPFIAN_SKEW);
            
            MEAN_DELAY_NS = 1_000_000_000L / Utils.PRODUCTION_RATE_SEC;
            STDDEV_DELAY_NS = 20 * MEAN_DELAY_NS / 100;     // 20% of the mean
            MIN_DELAY_NS = 1_000L;        // 1 µs lower bound
            MAX_DELAY_NS = 4 * MEAN_DELAY_NS;

            if (Utils.DELAY_DISTRIBUTION.equals("NONE")){
                stat.startProduceTime = System.currentTimeMillis();
                sendMessagesNoDelay(producer, Utils.WARMUP_MESSAGES, zipfCdf, "warmUp", message);
                stat.endWarmUpTime = System.currentTimeMillis();
                sendMessagesNoDelay(producer, Utils.TEST_MESSAGES, zipfCdf, "normal", message);
                stat.endProduceTime = System.currentTimeMillis();
            } else {
                stat.startProduceTime = System.currentTimeMillis();
                sendMessages(producer, Utils.WARMUP_MESSAGES, zipfCdf, "warmUp", message);
                stat.endWarmUpTime = System.currentTimeMillis();
                sendMessages(producer, Utils.TEST_MESSAGES, zipfCdf, "normal", message);
                stat.endProduceTime = System.currentTimeMillis();
            }
            
            long warmUpDuration = stat.endWarmUpTime - stat.startProduceTime;
            long testDuration = stat.endProduceTime - stat.endWarmUpTime;

            printProducerLog(Utils.WARMUP_MESSAGES, Utils.TEST_MESSAGES, warmUpDuration, testDuration);
     
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    private static void sendMessagesNoDelay(KafkaProducer<String, String> producer, int messageNum, double[] zipfCdf, String key, String value) {
        long counter = 0;
        random.setSeed(((long) producerId) << 32 | producerId);
        while (counter < messageNum) {               
            for (Map.Entry<String, Integer> entry : currentPartitions.entrySet()) {
                String topic = entry.getKey();
                int partitionCount = entry.getValue();
                int partition = 0;

                if (Utils.MESSAGE_DISTRIBUTION.equals("UNIFORM")) {
                    partition = random.nextInt(partitionCount);
                } else if (Utils.MESSAGE_DISTRIBUTION.equals("ZIPFIAN")) {
                    partition = sampleZipfPartition(zipfCdf, random);
                }

                if (random.nextDouble()<= Utils.PROBABILITY_THRESHOLD) {
                    ProducerRecord<String, String> record = new ProducerRecord<>(
                        topic, 
                        partition, 
                        System.currentTimeMillis(), 
                        key, 
                        value
                    );
                    producer.send(record, (metadata, exception) -> {
                        if (exception != null) {
                            System.out.println("Error sending message: " + exception);
                        }
                    });
                    counter++;
                }

                if (counter >= messageNum) {
                    break;
                }
            }
        }
    }

    private static void sendMessages(KafkaProducer<String, String> producer, int messageNum, double[] zipfCdf, String key, String value) {
        long counter = 0;        
        random.setSeed(((long) producerId) << 32 | producerId);
        long nextSendTime = System.nanoTime();
        while (counter < messageNum) {
            for (Map.Entry<String, Integer> entry : currentPartitions.entrySet()) {
                String topic = entry.getKey();
                int partitionCount = entry.getValue();
                int partition = 0;
                long delayNs = MIN_DELAY_NS;

                if (Utils.MESSAGE_DISTRIBUTION.equals("UNIFORM")) {
                    partition = random.nextInt(partitionCount);
                } else if (Utils.MESSAGE_DISTRIBUTION.equals("ZIPFIAN")) {
                    partition = sampleZipfPartition(zipfCdf, random);
                }

                ProducerRecord<String, String> record = new ProducerRecord<>(
                        topic,
                        partition,
                        System.currentTimeMillis(),
                        key,
                        value
                );
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        System.err.println("Error sending message: " + exception);
                    }
                });
                counter++;

                if (Utils.DELAY_DISTRIBUTION.equals("NORMAL")){
                    delayNs = (long) (MEAN_DELAY_NS + random.nextGaussian() * STDDEV_DELAY_NS);
                } else if (Utils.DELAY_DISTRIBUTION.equals("POISSON")){
                    delayNs = (long) (-MEAN_DELAY_NS * Math.log(1.0 - random.nextDouble()));
                }

                delayNs = Math.max(MIN_DELAY_NS, Math.min(MAX_DELAY_NS, delayNs));
                
                nextSendTime += delayNs;
                long now = System.nanoTime();
                if (nextSendTime > now) {
                    LockSupport.parkNanos(nextSendTime - now);
                }

                if (counter >= messageNum) {
                    break;
                }
            }
        }
    }

    private static void printProducerLog(
            long warmUpMessages,
            long testMessages,
            long warmUpDuration,
            long testDuration) {
        
        double throughput = testMessages / (testDuration / 1000.0);
        double measuredWarmUp = warmUpDuration / 1000.0;
        double measuredtest = testDuration / 1000.0;

        System.out.println("Producer Statistics:");
        System.out.printf("  Measured warmUp duration in sec:   %.3f%n", measuredWarmUp);
        System.out.printf("  Measured test duration in sec:     %.3f%n", measuredtest);
        System.out.printf("  WarmUp messages sent:              %d%n", warmUpMessages);
        System.out.printf("  Test messages sent:                %d%n", testMessages);
        System.out.printf("  Total messages sent:               %d%n", warmUpMessages + testMessages);
        System.out.printf("  Expected Producer throughput:      %.2f msg/s%n", throughput);
    }
}
