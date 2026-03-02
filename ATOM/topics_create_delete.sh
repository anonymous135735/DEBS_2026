#!/bin/bash

KAFKA_DIR="/opt/kafka/kafka_2.13-3.9.1"
BROKER="192.168.0.101:9092,192.168.0.102:9092,192.168.0.103:9092"
#BROKER="192.168.0.101:9092"

create_topics() {
    if [ "$#" -lt 3 ]; then
        echo "Usage: $0 create <number_of_topics> <partitions> <replication_factor>"
        exit 1
    fi

    NUM_TOPICS=$1
    PARTITIONS=$2
    REPLICATION_FACTOR=$3

    echo "Creating $NUM_TOPICS topics with $PARTITIONS partitions and replication factor $REPLICATION_FACTOR..."
    for ((i = 1; i <= NUM_TOPICS; i++)); do
        echo "Creating topic: test$i"
        $KAFKA_DIR/bin/kafka-topics.sh --bootstrap-server $BROKER --create \
            --topic "test$i" \
            --partitions $PARTITIONS \
            --replication-factor $REPLICATION_FACTOR
    done

    echo "Creating system topics..." 
    $KAFKA_DIR/bin/kafka-topics.sh --bootstrap-server $BROKER --create --topic "start-signal" --partitions 3 --replication-factor 3
    $KAFKA_DIR/bin/kafka-topics.sh --bootstrap-server $BROKER --create --topic "rep" --partitions 1 --replication-factor 3
    $KAFKA_DIR/bin/kafka-topics.sh --bootstrap-server $BROKER --create --topic "stable" --partitions 1 --replication-factor 3
    
    # only on one broker
    #$KAFKA_DIR/bin/kafka-topics.sh --bootstrap-server $BROKER --create --topic "start-signal" --partitions 1 --replication-factor 1
    #$KAFKA_DIR/bin/kafka-topics.sh --bootstrap-server $BROKER --create --topic "rep" --partitions 1 --replication-factor 1
    #$KAFKA_DIR/bin/kafka-topics.sh --bootstrap-server $BROKER --create --topic "stable" --partitions 1 --replication-factor 1

    sleep 2
    echo "Topics created successfully!"
    
    # Execute partition reassignment for test1 topic
    echo "Executing partition reassignment for test1 topic..."
    
    # Create reassignment JSON file in the bin directory
    REASSIGNMENT_FILE="$KAFKA_DIR/bin/reassignment.json"
    
    cat > $REASSIGNMENT_FILE << EOF
{
  "version": 1,
  "partitions": [
    {
      "topic": "test1",
      "partition": 0,
      "replicas": [2, 3]
    },
    {
      "topic": "test1",
      "partition": 1,
      "replicas": [3, 1]
    },
    {
      "topic": "test1",
      "partition": 2,
      "replicas": [1, 2]
    }
  ]
}
EOF
    
    echo "Reassignment JSON file created at: $REASSIGNMENT_FILE"
    cat $REASSIGNMENT_FILE
    
    # Execute the partition reassignment
    echo "Executing: kafka-reassign-partitions.sh --bootstrap-server 192.168.0.101:9092 --reassignment-json-file $REASSIGNMENT_FILE --execute"
    $KAFKA_DIR/bin/kafka-reassign-partitions.sh --bootstrap-server 192.168.0.101:9092 --reassignment-json-file $REASSIGNMENT_FILE --execute
    
    # Wait for reassignment to start
    sleep 2
    
    # Verify the reassignment
    echo "Verifying: kafka-reassign-partitions.sh --bootstrap-server 192.168.0.101:9092 --reassignment-json-file $REASSIGNMENT_FILE --verify"
    $KAFKA_DIR/bin/kafka-reassign-partitions.sh --bootstrap-server 192.168.0.101:9092 --reassignment-json-file $REASSIGNMENT_FILE --verify
    
    # Wait for reassignment to complete
    echo "Waiting for reassignment to complete..."
    sleep 5
    
    # Describe the topic to confirm the new replica assignment
    echo "Describing test1 topic to verify new replica assignment:"
    echo "Executing: kafka-topics.sh --bootstrap-server 192.168.0.101:9092 --describe --topic test1"
    $KAFKA_DIR/bin/kafka-topics.sh --bootstrap-server 192.168.0.101:9092 --describe --topic test1
    
    echo "Partition reassignment completed!"
    echo "Reassignment file saved at: $REASSIGNMENT_FILE"
    sleep 1
}

delete_topics() {
    echo "Fetching all available topics..."
    TOPICS=$($KAFKA_DIR/bin/kafka-topics.sh --bootstrap-server $BROKER --list)

    if [ -z "$TOPICS" ]; then
        echo "No topics available to delete."
        exit 0
    fi

    echo "Deleting topics (excluding __consumer_offsets)..."
    for TOPIC in $TOPICS; do
        if [ "$TOPIC" != "__consumer_offsets" ]; then
            echo "Deleting topic: $TOPIC"
            $KAFKA_DIR/bin/kafka-topics.sh --bootstrap-server $BROKER --delete --topic "$TOPIC"
        else
            echo "Skipping topic: __consumer_offsets"
        fi
    done

    # Remove reassignment JSON file from bin directory if it exists
    REASSIGNMENT_FILE="$KAFKA_DIR/bin/reassignment.json"
    if [ -f "$REASSIGNMENT_FILE" ]; then
        echo "Removing reassignment.json file from bin directory..."
        rm "$REASSIGNMENT_FILE"
    fi

    sleep 2
    echo "Topic deletion complete!"
}

case "$1" in
  create)
    shift
    create_topics "$@"
    ;;
  delete)
    delete_topics
    ;;
  *)
    echo "Usage:"
    echo "  $0 create <number_of_topics> <partitions> <replication_factor>"
    echo "  $0 delete"
    exit 1
    ;;
esac