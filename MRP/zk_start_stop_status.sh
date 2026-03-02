#!/bin/bash

USER="$(whoami)"
ZK_HOME="/opt/zookeeper"

nodes=(
  192.168.0.101
  192.168.0.102
  192.168.0.103
)

action=$1

if [[ -z "$action" ]]; then
  echo "Usage: $0 start|stop|status"
  exit 1
fi

for ip in "${nodes[@]}"; do
  echo ">>> $action ZooKeeper on $ip"
  ssh -o StrictHostKeyChecking=no "$USER@$ip" "
  cd $ZK_HOME
  ./bin/zkServer.sh $action"
  echo ""
done
