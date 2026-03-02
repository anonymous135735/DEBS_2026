#!/bin/bash

USER="$(whoami)"

IPS=(
    "192.168.0.101"
    "192.168.0.102"
    "192.168.0.103"
    "192.168.0.104"
    "192.168.0.105"
    "192.168.0.106"
    "192.168.0.107"
    "192.168.0.108"
)

for ip in "${IPS[@]}"; do
    echo ">>> Stopping all running MRP apps on $ip"

    ssh -n "$USER@$ip" '
        # List of MRP process names
        patterns="MRPAcceptor|MRPProducer|MRPConsumer|sonarlint-ls"

        # Get matching PIDs from jps
        pids=$(jps -l | grep -E "$patterns" | awk "{print \$1}")

        if [ -n "$pids" ]; then
            echo "Killing: $pids"
            kill -9 $pids 2>/dev/null
        else
            echo "No MRP processes found."
        fi
    '
done

echo ">>> All MRP processes stopped."
