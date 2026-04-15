#!/bin/bash

if [ -z "$1" ]; then
    echo "Usage: ./bench.sh <SERVER_PID>"
    exit 1
fi

SERVER_PID=$1
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_DIR="benchmark_results"
mkdir -p "$LOG_DIR"
LOG_FILE="${LOG_DIR}/benchmark_${TIMESTAMP}.log"

echo "Starting benchmark... Logging to ${LOG_FILE}"

USAGE_FILE="${LOG_DIR}/usage_${TIMESTAMP}.csv"
echo "Time,CPU,Memory(KB)" > "$USAGE_FILE"
# Start background resource monitor
while true; do
    TIME=$(date +"%H:%M:%S")
    STATS=$(ps -p $SERVER_PID -o %cpu,rss --no-headers 2>/dev/null)
    if [ -n "$STATS" ]; then
        # Convert spaces to comma
        echo "$TIME,$(echo $STATS | awk '{print $1","$2}')" >> "$USAGE_FILE"
    fi
    sleep 0.5
done &
MONITOR_PID=$!

# Run benchmarks and redirect output to the timestamped log file
{
    echo "--- Standard Commands ---"
    redis-benchmark -h 127.0.0.1 -p 6379 -n 100000 -c 100 -t ping,set,get,rpush,lpop

    echo ""
    echo "--- Slow List Commands ---"
    redis-benchmark -h 127.0.0.1 -p 6379 -n 1000 -c 100 -t lpush,lrange

    echo ""
    echo "--- Additional Commands ---"
    echo "ECHO:"
    redis-benchmark -h 127.0.0.1 -p 6379 -n 100000 -c 100 ECHO "Hello World"
    
    echo "LLEN:"
    redis-benchmark -h 127.0.0.1 -p 6379 -n 100000 -c 100 LLEN mylist
    
    echo "TYPE:"
    redis-benchmark -h 127.0.0.1 -p 6379 -n 100000 -c 100 TYPE mykey

    echo "BLPOP:"
    redis-benchmark -h 127.0.0.1 -p 6379 -n 10000 -c 100 BLPOP mylist 1

    echo ""
    echo "--- Stream Commands ---"
    echo "XADD:"
    redis-benchmark -h 127.0.0.1 -p 6379 -n 100000 -c 100 XADD mystream "*" f1 v1

    redis-benchmark -h 127.0.0.1 -p 6379 -n 100 -c 100 XADD readstream "*" f1 v1

    # echo "XRANGE:"
    # redis-benchmark -h 127.0.0.1 -p 6379 -n 100 -c 100 XRANGE mystream - +
    
    # echo "XREAD:"
    # redis-benchmark -h 127.0.0.1 -p 6379 -n 100 -c 100 XREAD STREAMS mystream 0-0
    echo "XRANGE:"
    redis-benchmark -h 127.0.0.1 -p 6379 -n 100000 -c 100 XRANGE readstream - +
    
    echo "XREAD:"
    redis-benchmark -h 127.0.0.1 -p 6379 -n 100000 -c 100 XREAD STREAMS readstream 0-0
} > "$LOG_FILE"

# Stop monitor
kill $MONITOR_PID 2>/dev/null

# Append resource utilization to the log file
echo "" >> "$LOG_FILE"
echo "--- Server Resource Utilization (Average) ---" >> "$LOG_FILE"
awk -F, 'NR>1 { cpu+=$2; mem+=$3; n++ } END { if(n>0) printf "CPU: %.2f%%\nMemory: %.2f MB\n", cpu/n, (mem/n)/1024; else print "No resource data collected" }' "$USAGE_FILE" >> "$LOG_FILE"

# Display the results to the terminal
cat "$LOG_FILE"
