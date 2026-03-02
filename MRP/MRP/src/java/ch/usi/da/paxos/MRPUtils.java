package ch.usi.da.paxos;

import java.util.*;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Collectors;

public class MRPUtils {

    // zookeeper configurations
    public static String ZK_SERVERS = "192.168.0.101:2181,192.168.0.102:2181,192.168.0.103:2181";
      
    // MRP Ring Configuration
    public static int RING_COUNT;
    public static int GROUP_ID;
    
    // Performance Configuration
    public static List<String> cleanArgs;
    public static final String UNIFORM_DIST = "UNIFORM";
    public static final String NORMAL_DIST = "NORMAL";
    public static final String POISSON_DIST = "POISSON";
    public static final String ZIPFIAN_DIST = "ZIPFIAN";

    public static int NUMBER_OF_PRODUCERS;
    public static int WARMUP_MESSAGES;
    public static int TEST_MESSAGES;
    public static int MESSAGE_SIZE_BYTES;
    public static String DELAY_DISTRIBUTION;
    public static long PRODUCTION_RATE_SEC;
    public static String MESSAGE_DISTRIBUTION;

    public static double PROBABILITY_THRESHOLD;
    public static double ZIPFIAN_SKEW;

    // MRP Configurations (with defaults matching TopologyManager)
    public static String P1_PREEXECUTION_NUMBER = "5000";
    public static String P1_RESEND_TIME = "1000";
    public static String CONCURRENT_VALUES = "20";
    public static String VALUE_SIZE = "32768";
    public static String VALUE_COUNT = "900000";
    public static String BATCH_POLICY = "none";
    public static String VALUE_RESEND_TIME = "3000";
    public static String QUORUM_SIZE = "2";
    public static String STABLE_STORAGE = "ch.usi.da.paxos.storage.BufferArray";
    public static String TCP_NODELAY = "1";
    public static String TCP_CRC = "0";
    public static String BUFFER_SIZE = "2097152";
    public static String LEARNER_RECOVERY = "1";
    public static String TRIM_MODULO = "0";
    public static String TRIM_QUORUM = "2";
    public static String AUTO_TRIM = "0";
    public static String MULTI_RING_LAMBDA = "0";
    public static String MULTI_RING_DELTA_T = "100";
    public static String DELIVER_SKIP_MESSAGES = "1";
    public static String MULTI_RING_START_TIME = "0";

    public static final int MRP_ARGS_COUNT = 12;

    public static void loadMRPArgs() {
        List<String> allLines;
        try {
            allLines = Files.readAllLines(Paths.get("./MRP/config/mrpargs.txt"));
        } catch (IOException e) {
            throw new RuntimeException("Error reading mrpargs.txt: " + e.getMessage(), e);
        }
            
        cleanArgs = allLines.stream()
            .map(String::trim)
            .filter(line -> !line.isEmpty() && !line.startsWith("#"))
            .collect(Collectors.toList());

        if (cleanArgs.size() < MRP_ARGS_COUNT) {
            throw new IllegalArgumentException("Expected at least " + MRP_ARGS_COUNT + " arguments, but found " + cleanArgs.size());
        }

        RING_COUNT = Integer.parseInt(cleanArgs.get(0));
        GROUP_ID = Integer.parseInt(cleanArgs.get(1));
        NUMBER_OF_PRODUCERS = Integer.parseInt(cleanArgs.get(2));
        NUMBER_OF_CONSUMERS = Integer.parseInt(cleanArgs.get(3));
        WARMUP_MESSAGES = Integer.parseInt(cleanArgs.get(4));
        TEST_MESSAGES = Integer.parseInt(cleanArgs.get(5));
        MESSAGE_SIZE_BYTES = Integer.parseInt(cleanArgs.get(6));
        DELAY_DISTRIBUTION = cleanArgs.get(7);
        PRODUCTION_RATE_SEC = Long.parseLong(cleanArgs.get(8));
        MESSAGE_DISTRIBUTION = cleanArgs.get(9);
        PROBABILITY_THRESHOLD = Double.parseDouble(cleanArgs.get(10));
        ZIPFIAN_SKEW = Double.parseDouble(cleanArgs.get(11));

        if (cleanArgs.size() > 12) P1_PREEXECUTION_NUMBER = cleanArgs.get(12);
        if (cleanArgs.size() > 13) P1_RESEND_TIME = cleanArgs.get(13);
        if (cleanArgs.size() > 14) CONCURRENT_VALUES = cleanArgs.get(14);
        if (cleanArgs.size() > 15) VALUE_SIZE = cleanArgs.get(15);
        if (cleanArgs.size() > 16) VALUE_COUNT = cleanArgs.get(16);
        if (cleanArgs.size() > 17) BATCH_POLICY = cleanArgs.get(17);
        if (cleanArgs.size() > 18) VALUE_RESEND_TIME = cleanArgs.get(18);
        if (cleanArgs.size() > 19) QUORUM_SIZE = cleanArgs.get(19);
        if (cleanArgs.size() > 20) STABLE_STORAGE = cleanArgs.get(20);
        if (cleanArgs.size() > 21) TCP_NODELAY = cleanArgs.get(21);
        if (cleanArgs.size() > 22) TCP_CRC = cleanArgs.get(22);
        if (cleanArgs.size() > 23) BUFFER_SIZE = cleanArgs.get(23);
        if (cleanArgs.size() > 24) LEARNER_RECOVERY = cleanArgs.get(24);
        if (cleanArgs.size() > 25) TRIM_MODULO = cleanArgs.get(25);
        if (cleanArgs.size() > 26) TRIM_QUORUM = cleanArgs.get(26);
        if (cleanArgs.size() > 27) AUTO_TRIM = cleanArgs.get(27);
        if (cleanArgs.size() > 28) MULTI_RING_LAMBDA = cleanArgs.get(28);
        if (cleanArgs.size() > 29) MULTI_RING_DELTA_T = cleanArgs.get(29);
        if (cleanArgs.size() > 30) DELIVER_SKIP_MESSAGES = cleanArgs.get(30);
        if (cleanArgs.size() > 31) MULTI_RING_START_TIME = cleanArgs.get(31);
        
        //MULTI_RING_START_TIME = String.valueOf(Long.parseLong(cleanArgs.get(31)) * 1000);
        
        MULTI_RING_START_TIME = String.valueOf(System.currentTimeMillis() + (Long.parseLong(cleanArgs.get(31)) * 1000));
        cleanArgs.set(31, MULTI_RING_START_TIME);

        System.out.println("All parameters are loaded from mrpargs.txt file ...");
        System.out.println(cleanArgs);
    }
}