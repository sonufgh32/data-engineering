#!/bin/bash

set -e

# Start Node Exporter
echo "Starting Node Exporter..."
wget -q https://github.com/prometheus/node_exporter/releases/download/v1.8.2/node_exporter-1.8.2.linux-amd64.tar.gz && \
tar -xzf node_exporter-1.8.2.linux-amd64.tar.gz && \
cd node_exporter-1.8.2.linux-amd64 && \
./node_exporter > /var/log/node_exporter.log 2>&1 &
NODE_PID=$!
echo "Node Exporter started with PID $NODE_PID"

# Start Alloy
echo "Starting Alloy..."
/usr/local/bin/alloy-linux-amd64 run --server.http.listen-addr=0.0.0.0:12345 /etc/alloy/config.alloy > /var/log/alloy.log 2>&1 &
ALLOY_PID=$!
echo "Alloy started with PID $ALLOY_PID"

# Give services time to start
sleep 2

# Check if services are running
if ps -p $NODE_PID > /dev/null; then
    echo "✓ Node Exporter is running"
else
    echo "✗ Node Exporter failed to start"
fi

if ps -p $ALLOY_PID > /dev/null; then
    echo "✓ Alloy is running"
else
    echo "✗ Alloy failed to start"
    cat /var/log/alloy.log
fi

# Handle signals for graceful shutdown
trap "echo 'Shutting down...'; kill $NODE_PID $ALLOY_PID 2>/dev/null || true" SIGTERM SIGINT

exec "$@"