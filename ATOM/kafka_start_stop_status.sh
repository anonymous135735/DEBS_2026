#!/bin/bash

KAFKA_DIR="/opt/kafka/kafka_2.13-3.9.1"
CONFIG_DIR="$KAFKA_DIR/config"

SERVERS=("192.168.0.101" "192.168.0.102" "192.168.0.103")
ZK_IDS=(1 2 3)

USER="$(whoami)"

MAX_RETRIES=5
SLEEP_BETWEEN_RETRIES=3

setup_zookeeper_myid() {
    local server_ip=$1
    local id=$2
    echo "Setting up ZooKeeper myid on $server_ip..."
    ssh "$USER@$server_ip" "mkdir -p /tmp/zookeeper && [ ! -f /tmp/zookeeper/myid ] && echo \"$id\" > /tmp/zookeeper/myid"
}

check_and_start_again() {
    local server=$1
    local process_name=$2
    local start_cmd=$3
    local retries=0

    echo "Checking for $process_name on $server..."
    while true; do
        running=$(ssh "$USER@$server" "jps | grep $process_name")
        if [[ -n "$running" ]]; then
            echo "$process_name is running on $server"
            break
        fi

        retries=$((retries+1))
        if [[ $retries -ge $MAX_RETRIES ]]; then
            echo "ERROR: $process_name did not start on $server after $MAX_RETRIES retries."
            exit 1
        fi

        echo "$process_name not found on $server. Retrying in $SLEEP_BETWEEN_RETRIES seconds..."
        sleep "$SLEEP_BETWEEN_RETRIES"

        echo "Attempting to start $process_name again on $server..."
        ssh "$USER@$server" "$start_cmd"
    done
}

check_and_stop_again() {
    local server=$1
    local process_name=$2
    local stop_cmd=$3
    local retries=0

    echo "Checking if $process_name stopped on $server..."
    while true; do
        running=$(ssh "$USER@$server" "jps | grep $process_name")
        if [[ -z "$running" ]]; then
            echo "$process_name stopped successfully on $server"
            break
        fi

        retries=$((retries+1))
        if [[ $retries -ge $MAX_RETRIES ]]; then
            echo "ERROR: $process_name still running on $server after $MAX_RETRIES retries."
            ssh "$USER@$server" "jps | grep $process_name"
            break
        fi

        echo "$process_name still running. Retrying in $SLEEP_BETWEEN_RETRIES seconds..."
        sleep "$SLEEP_BETWEEN_RETRIES"
        echo "Attempting to stop $process_name again on $server..."
        ssh "$USER@$server" "$stop_cmd"
    done
}

start_cluster() {
    echo "========== STARTING KAFKA CLUSTER =========="

    # Setup ZooKeeper myid
    for i in "${!SERVERS[@]}"; do
        setup_zookeeper_myid "${SERVERS[$i]}" "${ZK_IDS[$i]}"
    done

    # Start ZooKeeper servers
    for server in "${SERVERS[@]}"; do
        echo "Starting ZooKeeper on $server..."
        ssh "$USER@$server" "$KAFKA_DIR/bin/zookeeper-server-start.sh -daemon $CONFIG_DIR/zookeeper.properties"
    done

    sleep 1

    # Verify ZooKeeper
    for server in "${SERVERS[@]}"; do
        check_and_start_again "$server" "QuorumPeerMain" "$KAFKA_DIR/bin/zookeeper-server-start.sh -daemon $CONFIG_DIR/zookeeper.properties"
    done

    sleep 1
    
    # Start Kafka brokers
    for server in "${SERVERS[@]}"; do
        echo "Starting Kafka broker on $server..."
        ssh "$USER@$server" "$KAFKA_DIR/bin/kafka-server-start.sh -daemon $CONFIG_DIR/server.properties"
    done
    
    sleep 1
    
    # Verify Kafka
    for server in "${SERVERS[@]}"; do
        check_and_start_again "$server" "Kafka" "$KAFKA_DIR/bin/kafka-server-start.sh -daemon $CONFIG_DIR/server.properties"
    done

    sleep 1
    
    # Start only on one broker 
    #echo "Starting Kafka broker on 192.168.0.101 ..."
    #ssh "$USER@192.168.0.101" "$KAFKA_DIR/bin/kafka-server-start.sh -daemon $CONFIG_DIR/server.properties"
    
    #sleep 1

    # Verify Kafka only on one broker 
    #check_and_start_again "192.168.0.101" "Kafka" "$KAFKA_DIR/bin/kafka-server-start.sh -daemon $CONFIG_DIR/server.properties"

    echo "Kafka cluster started successfully!"
}

stop_cluster() {
    echo "========== STOPPING KAFKA CLUSTER =========="

    # Stop Kafka brokers (reverse order)
    for ((i=${#SERVERS[@]}-1; i>=0; i--)); do
        server=${SERVERS[$i]}
        echo "Stopping Kafka broker on $server..."
        ssh "$USER@$server" "$KAFKA_DIR/bin/kafka-server-stop.sh"
    done
    
	sleep 1
    
    # Verify Kafka stopped
    for ((i=${#SERVERS[@]}-1; i>=0; i--)); do
        check_and_stop_again "${SERVERS[$i]}" "Kafka" "$KAFKA_DIR/bin/kafka-server-stop.sh"
    done
	
	sleep 2
	
    # Stop ZooKeeper servers (reverse order)
    for ((i=${#SERVERS[@]}-1; i>=0; i--)); do
        server=${SERVERS[$i]}
        echo "Stopping ZooKeeper on $server..."
        ssh "$USER@$server" "$KAFKA_DIR/bin/zookeeper-server-stop.sh"
    done
	
	sleep 1 
	
    # Verify ZooKeeper stopped
    for ((i=${#SERVERS[@]}-1; i>=0; i--)); do
        check_and_stop_again "${SERVERS[$i]}" "QuorumPeerMain" "$KAFKA_DIR/bin/zookeeper-server-stop.sh"
    done

    echo "Kafka cluster stopped successfully!"
}

status_cluster() {
    echo "========== CLUSTER STATUS =========="

    for server in "${SERVERS[@]}"; do
        echo "--- $server ---"
        zk_status=$(ssh "$USER@$server" "jps | grep QuorumPeerMain")
        kafka_status=$(ssh "$USER@$server" "jps | grep Kafka")

        if [[ -n "$zk_status" ]]; then
            echo "ZooKeeper running: $zk_status"
        else
            echo "ZooKeeper not running"
        fi

        if [[ -n "$kafka_status" ]]; then
            echo "Kafka running: $kafka_status"
        else
            echo "Kafka not running"
        fi

        echo
    done
    echo "==================================="
}

case "$1" in
  start)
    start_cluster
    ;;
  stop)
    stop_cluster
    ;;
  status)
    status_cluster
    ;;
  *)
    echo "Usage: $0 {start|stop|status}"
    exit 1
    ;;
esac
