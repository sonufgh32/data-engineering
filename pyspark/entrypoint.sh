#!/bin/bash

set -e

# Start Alloy
echo "Starting Alloy..."
/usr/local/bin/alloy-linux-amd64 run /etc/alloy/config.alloy > /var/log/alloy.log 2>&1 &
ALLOY_PID=$!
echo "Alloy started with PID $ALLOY_PID"

# Give services time to start
sleep 2

# Check if services are running
if ps -p $ALLOY_PID > /dev/null; then
    echo "✓ Alloy is running"
else
    echo "✗ Alloy failed to start"
    cat /var/log/alloy.log
fi

# Handle signals for graceful shutdown
trap "echo 'Shutting down...'; kill $ALLOY_PID 2>/dev/null || true" SIGTERM SIGINT

exec "$@"