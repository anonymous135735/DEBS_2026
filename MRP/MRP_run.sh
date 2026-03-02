#!/bin/bash

set -euo pipefail

USERNAME="$(whoami)"
BASE_PATH="/home/$USERNAME/MRP"
LOCAL_ZIP_PATH="$BASE_PATH/target/Paxos-trunk-release.zip"
DEPLOY_PATH="$BASE_PATH/deployment"
CLASS_PATH="$DEPLOY_PATH/Paxos-trunk/lib"
ARGS_PATH="$BASE_PATH/config/mrpargs.txt"
LOG_PATH="$BASE_PATH/logs"

ACCEPTOR_CLASS="ch.usi.da.paxos.MRPAcceptor"
PRODUCER_CLASS="ch.usi.da.paxos.MRPProducer"
CONSUMER_CLASS="ch.usi.da.paxos.MRPConsumer"

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

if [ "${#args[@]}" -lt 6 ]; then
  echo "Error: Expected 6 arguments, got ${#args[@]}"
  exit 1
fi

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

compile_project() {
    echo "Compiling project..."
    cd "$BASE_PATH" || exit 1
    if ! mvn -DskipTests clean package; then
        echo "Error: Maven compilation failed"
        exit 1
    fi
    echo "Project compiled successfully"
    cd
}

# Deploy to remote machine (unzip on remote side)
deploy_to_machine() {
    local machine=$1
  
    # Create remote directory structure
    ssh "$USERNAME@$machine" "
        mkdir -p $DEPLOY_PATH $BASE_PATH/config $LOG_PATH/acceptor $LOG_PATH/producer $LOG_PATH/consumer
    " || echo "Warning: Directory creation failed on $machine"
    
    # Copy zip file
    if ! scp "$LOCAL_ZIP_PATH" "$USERNAME@$machine:$DEPLOY_PATH"; then
        echo "Error: Failed to copy zip to $machine"
        return 1
    fi
    
    # Unzip on remote machine
    ssh "$USERNAME@$machine" "
        cd $DEPLOY_PATH
        unzip -o Paxos-trunk-release.zip > /dev/null 2>&1
        echo 'Deployment completed on $machine'
    " || {
        echo "Error: Failed to unzip on $machine"
        return 1
    }
    
    # Copy configuration file
    scp "$ARGS_PATH" "$USERNAME@$machine:$ARGS_PATH" || echo "Warning: Failed to copy config to $machine"
    
    echo "Deployment to $machine completed successfully"
}

run_acceptor() {
    local machine=$1
    local nodeId=$2
    
    echo "Starting acceptor $nodeId on $machine..."
    ssh "$USERNAME@$machine" "
        IFACE='enp4s0' \
        java \
        -cp '$CLASS_PATH/*' \
            $ACCEPTOR_CLASS \
            $nodeId \
        > '$LOG_PATH/acceptor/acceptor_${TIMESTAMP}_${nodeId}.log' 2>&1"
}

run_consumer() {
    local machine=$1
    local nodeId=$2
    
    echo "Starting consumer $nodeId on $machine..."
    ssh "$USERNAME@$machine" "
        IFACE="enp4s0" \
        java \
        -cp '$CLASS_PATH/*' $CONSUMER_CLASS $nodeId $TIMESTAMP \
        > '$LOG_PATH/consumer/consumer_${TIMESTAMP}_${nodeId}.log' 2>&1"
}

run_producer() {
    local machine=$1
    local nodeId=$2
    
    echo "Starting producer $nodeId on $machine..."
    ssh "$USERNAME@$machine" "
        IFACE="enp4s0" \
        java \
        -cp '$CLASS_PATH/*' $PRODUCER_CLASS $nodeId \
        > '$LOG_PATH/producer/producer_${TIMESTAMP}_${nodeId}.log' 2>&1"
}

check_requirements() {
    # Check if zip file exists
    if [[ ! -f "$LOCAL_ZIP_PATH" ]]; then
        echo "Error: Zip file not found: $LOCAL_ZIP_PATH"
        exit 1
    fi
    
    # Check if config file exists
    if [[ ! -f "$ARGS_PATH" ]]; then
        echo "Error: Config file not found: $ARGS_PATH"
        exit 1
    fi
}

compile_project
sleep 1

check_requirements
sleep 1

# Main execution
echo "=== MRP Cluster Deployment ==="

./zk_deleteall.sh
if [ $? -ne 0 ]; then
    echo "Error: Failed to delete /ringpaxos from zookeeper. Exiting."
    exit 1
fi
sleep 1

# Deploy to all machines first
echo "=== Phase 1: Deployment ==="
deploy_to_machine "$MACHINE1"
deploy_to_machine "$MACHINE2" 
deploy_to_machine "$MACHINE3"
#deploy_to_machine "$MACHINE4"
deploy_to_machine "$MACHINE5"
# deploy_to_machine "$MACHINE6"
deploy_to_machine "$MACHINE7"
deploy_to_machine "$MACHINE8"

sleep 2

echo "=== Phase 2: Starting Services ==="

# Start acceptors
echo "Starting acceptors..."

run_acceptor "$MACHINE1" "1" &
sleep 5
run_acceptor "$MACHINE2" "2" &
sleep 5
run_acceptor "$MACHINE3" "3" &
sleep 5

echo "Starting consumers..."
run_consumer "$MACHINE7" "7" &
sleep 5
run_consumer "$MACHINE8" "8" &
sleep 5

echo "Starting producers..."
#run_producer "$MACHINE4" "4" &
#sleep 5 
run_producer "$MACHINE5" "5" &
#sleep 5 
#run_consumer "$MACHINE8" "8" &

sleep 2
echo "=== All services started successfully ==="