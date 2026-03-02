package atom;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.*;

public class Utils {

    public static final String BOOTSTRAP_SERVERS = "192.168.0.101:9092,192.168.0.102:9092,192.168.0.103:9092";
    //public static final String BOOTSTRAP_SERVERS = "192.168.0.101:9092";
    public static final String SIGNAL_TOPIC = "start-signal";
    public static final String REP_TOPIC  = "rep";
    public static final String STABLE_TOPIC = "stable";
    public static final String SIGNAL_KEY = "SIGNAL";
    public static final String SIGNAL_VALUE = "START";
    public static final String MARKER_KEY = "Marker";
    public static final String NEW_KEY = "New";
    public static final String SID_KEY = "Sid";
    public static final String UNIFORM_DIST = "UNIFORM";
    public static final String NORMAL_DIST = "NORMAL";
    public static final String POISSON_DIST = "POISSON";
    public static final String ZIPFIAN_DIST = "ZIPFIAN";

    public static List<String> cleanArgs;
    public static int NUMBER_OF_PARTITIONS;
    public static int NUMBER_OF_PRODUCERS;
    public static int WARMUP_MESSAGES;
    public static int TEST_MESSAGES;
    public static int MESSAGE_SIZE_BYTES;
    
    public static String DELAY_DISTRIBUTION;
    public static long PRODUCTION_RATE_SEC;
    public static String MESSAGE_DISTRIBUTION;

    public static double PROBABILITY_THRESHOLD;
    public static double ZIPFIAN_SKEW;
   
    public static String PRODUCER_TOPICS; 
    public static String CONSUMER_TOPICS;

    // Some of the args are used in the running scripts, and others are used in applications, and some in both
    public static final int ARGS_COUNT = 20;

    public static void loadArgs(){
        List<String> allLines;
        try {
            allLines = Files.readAllLines(Paths.get("./ATOM/config/args.txt"));
        } catch (IOException e) {
            throw new RuntimeException("Error reading args.txt: " + e.getMessage(), e);
        }
        
        cleanArgs = allLines.stream()
            .map(String::trim)
            .filter(line -> !line.isEmpty() && !line.startsWith("#"))
            .collect(Collectors.toList());

        if (cleanArgs.size() != ARGS_COUNT) {
            throw new IllegalArgumentException("Expected at least " + ARGS_COUNT + " arguments, but found " + cleanArgs.size());
        }

        /*
            // Some of the args are used in the running scripts, and others are used in applications, and some in both
            int NUMBER_OF_TOPICS = Integer.parseInt(cleanArgs.get(0));
            int NUMBER_OF_PARTITIONS = Integer.parseInt(cleanArgs.get(1));
            int REPLICATION_FACTOR = Integer.parseInt(cleanArgs.get(2));
            int NUMBER_OF_PRODUCERS = Integer.parseInt(cleanArgs.get(3));
            int NUMBER_OF_CONSUMERS = Integer.parseInt(cleanArgs.get(4));
        */

        NUMBER_OF_PARTITIONS = Integer.parseInt(cleanArgs.get(1));
        NUMBER_OF_PRODUCERS = Integer.parseInt(cleanArgs.get(3));
        WARMUP_MESSAGES = Integer.parseInt(cleanArgs.get(5));
        TEST_MESSAGES = Integer.parseInt(cleanArgs.get(6));
        MESSAGE_SIZE_BYTES = Integer.parseInt(cleanArgs.get(7));
        
        DELAY_DISTRIBUTION = cleanArgs.get(8);
        PRODUCTION_RATE_SEC = Long.parseLong(cleanArgs.get(9));
        MESSAGE_DISTRIBUTION = cleanArgs.get(10);

        PROBABILITY_THRESHOLD = Double.parseDouble(cleanArgs.get(11));
        ZIPFIAN_SKEW = Double.parseDouble(cleanArgs.get(12));
        
        PRODUCER_TOPICS = cleanArgs.get(13);
        CONSUMER_TOPICS = cleanArgs.get(14);

        /*  
            // These configs are used the MP running script
            int NUMBER_OF_MP = Integer.parseInt(cleanArgs.get(15));
            long DELTA_M = Long.parseLong(cleanArgs.get(16));
            long DELTA_ACK = Long.parseLong(cleanArgs.get(17));
            int R_MAX = Integer.parseInt(cleanArgs.get(18));
            long RUN_DURATION = Long.parseLong(cleanArgs.get(19)); 
        */

        if (PRODUCER_TOPICS.trim().isEmpty() || CONSUMER_TOPICS.trim().isEmpty())
            throw new IllegalArgumentException("Producer or Consumer topic list is empty or invalid.");

        System.out.println("All parameters are loaded from args.txt file ...");
        System.out.println(cleanArgs);
    }

    public static void initTopicsPartitions(String topics, Map<String, Integer> currentPartitions) {
        // admin client to get the list of topics 
        try (AdminClient adminClient = Utils.createAdmin(Utils.BOOTSTRAP_SERVERS)){
            ListTopicsOptions listTopicsOptions = new ListTopicsOptions().listInternal(false).timeoutMs(5_000); // Exclude internal
            ListTopicsResult topicsResult = adminClient.listTopics(listTopicsOptions);
            
            Set<String> tmpTopics;
            if ("all".equalsIgnoreCase(topics.trim())) {
                tmpTopics = topicsResult.names().get();
            } else {
                tmpTopics = parseTopics(topics);
            }

            DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(tmpTopics);
            Map<String, KafkaFuture<TopicDescription>> topicDescriptions = describeTopicsResult.topicNameValues();

            for (Map.Entry<String, KafkaFuture<TopicDescription>> entry : topicDescriptions.entrySet()) {
                String topic = entry.getKey();
                if (Utils.isInternalTopic(topic))
                    continue;
                int partitionCount = entry.getValue().get().partitions().size();
                currentPartitions.put(topic, partitionCount);
            }           
        } catch (Exception e) {
            //throw new RuntimeException("Failed to init the list of topics and partitions: " + e.getMessage(), e);
        }
    }
    
    private static Set<String> parseTopics(String arg) {
        return Set.copyOf(
            Arrays.stream(arg.split(",")).collect(Collectors.toSet())
        );
    }

    public static boolean isInternalTopic(String topic) {
        return topic.equals(Utils.SIGNAL_TOPIC) || topic.equals(Utils.REP_TOPIC)
                || topic.equals(Utils.STABLE_TOPIC);
    }

    public static void waitStartSignal(String UID) {
        try (KafkaConsumer<String, String> consumer = createConsumer(BOOTSTRAP_SERVERS,
                "signal-group-" + UID, false)) {
            consumer.subscribe(Arrays.asList(SIGNAL_TOPIC));
            //TopicPartition partition = new TopicPartition("start-signal", 0);
            //consumer.assign(Arrays.asList(partition));
            System.out.println("Waiting for start signal ...");
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10));
                for (ConsumerRecord<String, String> record : records) {
                    if ("SIGNAL".equals(record.key()) && "START".equals(record.value())) {
                        System.out.println("Start signal received.");
                        return;
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void sendStartSignal() {
        try (KafkaProducer<String, String> producer = createProducer(BOOTSTRAP_SERVERS, true)) {
            long timestamp = Instant.now().toEpochMilli();
                producer.send(new ProducerRecord<>(SIGNAL_TOPIC, 0, timestamp, SIGNAL_KEY, SIGNAL_VALUE)).get();
                System.out.println("Start signal sent.");
        } catch (InterruptedException | ExecutionException e) {
            Thread.currentThread().interrupt();
            e.printStackTrace();
        }
    }

    public static AdminClient createAdmin(String broker) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        return AdminClient.create(props);
    }

    public static KafkaConsumer<String, String> createConsumer(String broker, String groupId, boolean MP) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        //props.put(ConsumerConfig.SEND_BUFFER_CONFIG, "1048576"); // 131072 // 1048576 // 2097152
        //props.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, "1048576"); // 65536 // 1048576 // 2097152
        // props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 6291456); // 1048576
        // props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1024); // 1
        // props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 104857600); // 52428800
        // props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1000); // 500

        if (MP){
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
            props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "6000");
            props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "2000");
            props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        }
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        return new KafkaConsumer<>(props);
    }

    public static KafkaProducer<String, String> createProducer(String broker, boolean MP) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "org.apache.kafka.clients.producer.RoundRobinPartitioner");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");                
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0); // Wait up to 100ms for batching
        //props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384); // 16384 16KB batch size
        //props.put(ProducerConfig.SEND_BUFFER_CONFIG, "1048576"); // 131072 // 1048576 // 2097152 
        //props.put(ProducerConfig.RECEIVE_BUFFER_CONFIG, "1048576"); // 32768 // 1048576 // 2097152
        if (MP){
            props.put(ProducerConfig.ACKS_CONFIG, "all");
            props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
            //props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1); // Strict ordering
            //props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE); // Keep retrying
        }
        return new KafkaProducer<>(props);
    }
}