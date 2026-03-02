package ch.usi.da.paxos;

import ch.usi.da.paxos.ring.Node;
import ch.usi.da.paxos.ring.RingDescription;
import ch.usi.da.paxos.api.PaxosRole;
import ch.usi.da.paxos.MRPUtils;

import java.util.*;
import java.util.concurrent.locks.LockSupport;

public class MRPProducer {
    private static final String UNIQUE_ID = UUID.randomUUID().toString().substring(0, 8);
    private static int nodeId;
    private static List<RingDescription> rings = new ArrayList<>();
    private static Node node;
   	private static Random random = new Random();
    
    private static long MEAN_DELAY_NS;
    private static long STDDEV_DELAY_NS;
    private static long MIN_DELAY_NS;
    private static long MAX_DELAY_NS;
    
    public static void main(String[] args) {
        long maxHeapSize = Runtime.getRuntime().maxMemory();
        System.out.println("Max Heap Size: " + (maxHeapSize / (1024 * 1024 * 1024)) + " GB");
        System.out.println("MRP_ProducerID: " + UNIQUE_ID);

        if (args.length < 1) {
            System.out.println("Usage: MRPProducer <nodeId>");
            System.exit(1);
        }

        nodeId = Integer.parseInt(args[0]);
        random.setSeed(((long) nodeId) << 32 | nodeId);
        System.out.println("Seed is set to = " + (((long) nodeId) << 32 | nodeId));

        try {
            MRPUtils.loadMRPArgs();
        } catch (Exception e) {
            System.err.println("Startup failed: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }

        initProposer();

        char[] payload = new char[MRPUtils.MESSAGE_SIZE_BYTES];
        Arrays.fill(payload, 'x');
        String message = new String(payload);
               
        try {
            Thread.sleep(5000); 
            produce(message);
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
    
    private static void initProposer() {       
        System.out.println("Initializing MRP_Producer/proposer ...");
        try {
            // Create ring descriptions using PaxosRole.Proposer
            for (int r = 0; r < MRPUtils.RING_COUNT; r++) {
                List<PaxosRole> roles = new ArrayList<>();
                roles.add(PaxosRole.Proposer);
                rings.add(new RingDescription(r, roles));
            }
            // Create Paxos Node (proposer)
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
            System.err.println("Error while Initializing MRP_Producer/proposer: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    private static void produce(String message) {
        ProducerStat stat = new ProducerStat(0, 0, 0);
        double[] zipfCdf = buildZipfCdf(MRPUtils.RING_COUNT, MRPUtils.ZIPFIAN_SKEW);
            
        MEAN_DELAY_NS = 1_000_000_000L / MRPUtils.PRODUCTION_RATE_SEC;
        STDDEV_DELAY_NS = 20 * MEAN_DELAY_NS / 100;     // 20% of the mean
        MIN_DELAY_NS = 1_000L;        // 1 µs lower bound
        MAX_DELAY_NS = 4 * MEAN_DELAY_NS;
        
        System.out.println("Starting to produce messages to " + MRPUtils.RING_COUNT + " rings...");

        if (MRPUtils.DELAY_DISTRIBUTION.equals("NONE")){
            stat.startProduceTime = System.currentTimeMillis();
            sendMessagesNoDelay(MRPUtils.WARMUP_MESSAGES, zipfCdf, "warmUp", message);
            stat.endWarmUpTime = System.currentTimeMillis();
            sendMessagesNoDelay(MRPUtils.TEST_MESSAGES, zipfCdf, "normal", message);
            stat.endProduceTime = System.currentTimeMillis();
        } else {
            stat.startProduceTime = System.currentTimeMillis();
            sendMessages(MRPUtils.WARMUP_MESSAGES, zipfCdf, "warmUp", message);
            stat.endWarmUpTime = System.currentTimeMillis();
            sendMessages(MRPUtils.TEST_MESSAGES, zipfCdf, "normal", message);
            stat.endProduceTime = System.currentTimeMillis();
        }

        long warmUpDuration = stat.endWarmUpTime - stat.startProduceTime;
		long testDuration = stat.endProduceTime - stat.endWarmUpTime;
        
        printProducerLog(MRPUtils.WARMUP_MESSAGES, MRPUtils.TEST_MESSAGES, warmUpDuration, testDuration);
            
    }

    private static void sendMessagesNoDelay(int messageNum, double[] zipfCdf, String key, String value) {
        long counter = 0;
        random.setSeed(((long) nodeId) << 32 | nodeId);
        while (counter < messageNum) {               
            int ringCount = MRPUtils.RING_COUNT;
            int ring = 0;

            if (MRPUtils.MESSAGE_DISTRIBUTION.equals("UNIFORM")) {
                ring = random.nextInt(ringCount);
            } else if (MRPUtils.MESSAGE_DISTRIBUTION.equals("ZIPFIAN")) {
                ring = sampleZipfPartition(zipfCdf, random);
            }

            if (random.nextDouble()<= MRPUtils.PROBABILITY_THRESHOLD) {
                String payload = String.format("%s|%d|%s", key, System.currentTimeMillis(), value);
                node.getProposer(ring).propose(payload.getBytes());
                counter++;
            }

            if (counter >= messageNum) {
                break;
            }
        }
    }

    private static void sendMessages(int messageNum, double[] zipfCdf, String key, String value) {
        long counter = 0;        
        random.setSeed(((long) nodeId) << 32 | nodeId);
        long nextSendTime = System.nanoTime();
        while (counter < messageNum) {     
            int ringCount = MRPUtils.RING_COUNT;
            int ring = 0;
            long delayNs = MIN_DELAY_NS;

            if (MRPUtils.MESSAGE_DISTRIBUTION.equals("UNIFORM")) {
                ring = random.nextInt(ringCount);
            } else if (MRPUtils.MESSAGE_DISTRIBUTION.equals("ZIPFIAN")) {
                ring = sampleZipfPartition(zipfCdf, random);
            }

            String payload = String.format("%s|%d|%s", key, System.currentTimeMillis(), value);
            node.getProposer(ring).propose(payload.getBytes());
            counter++;

            if (MRPUtils.DELAY_DISTRIBUTION.equals("NORMAL")){
                delayNs = (long) (MEAN_DELAY_NS + random.nextGaussian() * STDDEV_DELAY_NS);
            } else if (MRPUtils.DELAY_DISTRIBUTION.equals("POISSON")){
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
           
    private static void printProducerLog(long warmUpMessages, long testMessages, long warmUpDuration, long testDuration) {
        
        double throughput = testMessages / (testDuration / 1000.0);
        double measuredWarmUp = warmUpDuration / 1000.0;
        double measuredtest = testDuration / 1000.0;
       
        System.out.println("MRP_Producer Statistics:");
        System.out.printf("  Measured warmUp duration in sec:   %.3f%n", measuredWarmUp);
        System.out.printf("  Measured test duration in sec:     %.3f%n", measuredtest);
        System.out.printf("  WarmUp messages sent:              %d%n", warmUpMessages);
        System.out.printf("  Test messages sent:                %d%n", testMessages);
        System.out.printf("  Total messages sent:               %d%n", warmUpMessages + testMessages);
        System.out.printf("  Expected MRP_Producer throughput:  %.2f msg/s%n", throughput);
    }
}