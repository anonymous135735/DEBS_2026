#!/bin/bash

set -euo pipefail

USERNAME="$(whoami)"
BASE_PATH="/home/$USERNAME/ATOM"
LOCAL_JAR_PATH="$BASE_PATH/build/libs/ATOM-1.1.5.jar"
REMOTE_JAR_PATH="$BASE_PATH/ATOM-1.1.5.jar"
LOG_PATH="$BASE_PATH/logs"
ARGS_PATH="$BASE_PATH/config/args.txt"

PRODUCER_CLASS="atom.Producer"
TOCONSUMER_CLASS="atom.TOConsumer"
MARKER_PRODUCER_CLASS="atom.MarkerProducer"

MACHINE1="192.168.0.101"
MACHINE2="192.168.0.102"
MACHINE3="192.168.0.103"
MACHINE4="192.168.0.104"
MACHINE5="192.168.0.105"
MACHINE6="192.168.0.106"
MACHINE7="192.168.0.107"
MACHINE8="192.168.0.108"

args=()
while IFS= read -r line; do
  [[ -z "$line" || "$line" =~ ^# ]] && continue
  args+=("$line")
done < "$ARGS_PATH"

if [ "${#args[@]}" -ne 20 ]; then
  echo "Error: Expected 20 arguments, got ${#args[@]}"
  exit 1
fi

TOPICS=${args[0]}
PARTITIONS=${args[1]}
REPLICATION=${args[2]}
NUM_PRODUCERS=${args[3]}
NUM_CONSUMERS=${args[4]}
NUM_MP=${args[15]}
DELTA_M=${args[16]}
DELTA_ACK=${args[17]}
R_MAX=${args[18]}
RUN_DURATION=${args[19]}

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

run_consumer() {
    local machine=$1
    local instance=$2
    scp $LOCAL_JAR_PATH "$USERNAME@$machine:$BASE_PATH"
    scp $ARGS_PATH "$USERNAME@$machine:$ARGS_PATH"
    sleep 1
    ssh "$USERNAME@$machine" "
        java \
        -Xlog:gc*,gc+heap=debug,gc+age=trace:file=$LOG_PATH/gc/gc_TO_consumer${TIMESTAMP}_${instance}.log:time,uptime,level,tags \
        -cp $REMOTE_JAR_PATH $TOCONSUMER_CLASS $instance $TIMESTAMP \
        > $LOG_PATH/consumer/TO_consumer_${TIMESTAMP}_${instance}.log 2>&1"
}

run_producer() {
    local machine=$1
    local instance=$2
    scp $LOCAL_JAR_PATH "$USERNAME@$machine:$BASE_PATH"
    scp $ARGS_PATH "$USERNAME@$machine:$ARGS_PATH"
    sleep 1
    ssh "$USERNAME@$machine" "
        java \
        -Xlog:gc*,gc+heap=debug,gc+age=trace:file=$LOG_PATH/gc/gc_producer_${TIMESTAMP}_${instance}.log:time,uptime,level,tags \
        -cp $REMOTE_JAR_PATH $PRODUCER_CLASS $instance \
        > $LOG_PATH/producer/producer_${TIMESTAMP}_${instance}.log 2>&1"
}

run_marker_producer() {
    local machine=$1
    local leader_id=$2
    scp $LOCAL_JAR_PATH "$USERNAME@$machine:$BASE_PATH"
    scp $ARGS_PATH "$USERNAME@$machine:$ARGS_PATH"
    sleep 1
    ssh "$USERNAME@$machine" "
        java \
        -Xlog:gc*,gc+heap=debug,gc+age=trace:file=$LOG_PATH/gc/gc_marker_producer_${TIMESTAMP}_${leader_id}.log:time,uptime,level,tags \
        -cp $REMOTE_JAR_PATH $MARKER_PRODUCER_CLASS $leader_id $DELTA_M $DELTA_ACK $R_MAX $RUN_DURATION \
        > $LOG_PATH/mProducer/marker_producer_${TIMESTAMP}_${leader_id}.log 2>&1"
}

echo "Deleting all topics..."
./topics_create_delete.sh "delete"
if [ $? -ne 0 ]; then
    echo "Error: Failed to delete topics. Exiting."
    exit 1
fi

sleep 2

echo "Creating topics..."
./topics_create_delete.sh "create" "$TOPICS" "$PARTITIONS" "$REPLICATION"
if [ $? -ne 0 ]; then
    echo "Error: Failed to create topics. Exiting."
    exit 1
fi

consumer_instance=1
if (( NUM_CONSUMERS == 1 )); then
    echo "Starting consumer $consumer_instance on $MACHINE7 ..."
    run_consumer "$MACHINE7" "$consumer_instance" &
elif (( NUM_CONSUMERS == 2 )); then
    echo "Starting consumer $consumer_instance on $MACHINE7 ..."
    run_consumer "$MACHINE7" "$consumer_instance" &
    ((consumer_instance++))
    sleep 1
    echo "Starting consumer $consumer_instance on $MACHINE8 ..."
    run_consumer "$MACHINE8" "$consumer_instance" &
else
    count7=$(( NUM_CONSUMERS / 2 + NUM_CONSUMERS % 2 ))  # Ceiling of i/2
    count8=$(( NUM_CONSUMERS / 2 ))          # Floor of i/2

    for ((j=1; j<=count7; j++)); do
        echo "Starting consumer $consumer_instance on $MACHINE7 ..."
        run_consumer "$MACHINE7" "$consumer_instance" &
        ((consumer_instance++))
        sleep 1
    done

    for ((j=1; j<=count8; j++)); do
        echo "Starting consumer $consumer_instance on $MACHINE8 ..."
        run_consumer "$MACHINE8" "$consumer_instance" &
        ((consumer_instance++))
        sleep 1
    done
fi

sleep 2

producer_instance=1
if (( NUM_PRODUCERS == 1 )); then
    echo "Starting producer $producer_instance on $MACHINE5 ..."
    run_producer "$MACHINE5" "$producer_instance" &
elif (( NUM_PRODUCERS == 2 )); then
    echo "Starting producer $producer_instance on $MACHINE5 ..."
    run_producer "$MACHINE5" "$producer_instance" &
    ((producer_instance++))
    sleep 1
    echo "Starting producer $producer_instance on $MACHINE6 ..."
    run_producer "$MACHINE6" "$producer_instance" &
else
    count5=$(( NUM_PRODUCERS / 2 + NUM_PRODUCERS % 2 ))  # Ceiling of i/2
    count6=$(( NUM_PRODUCERS / 2 ))          # Floor of i/2

    for ((j=1; j<=count5; j++)); do
        echo "Starting producer $producer_instance on $MACHINE5 ..."
        run_producer "$MACHINE5" "$producer_instance" &
        ((producer_instance++))
        sleep 1
    done

    for ((j=1; j<=count6; j++)); do
        echo "Starting producer $producer_instance on $MACHINE6 ..."
        run_producer "$MACHINE6" "$producer_instance" &
        ((producer_instance++))
        sleep 1
    done
fi

sleep 2

echo "Starting $NUM_MP marker producer(s)..."
for (( mp=1; mp<=NUM_MP; mp++ )); do
    leader_id="leader_$mp"
    if (( mp % 2 == 1 )); then
        machine="$MACHINE4"
    else
        machine="$MACHINE6"
    fi
    echo "Starting marker producer and sending signal from $machine with $leader_id ..."
    run_marker_producer "$machine" "$leader_id" &
    sleep 2
done

echo "waiting for all background tasks to be completed ..."
wait
echo "All background tasks completed."
