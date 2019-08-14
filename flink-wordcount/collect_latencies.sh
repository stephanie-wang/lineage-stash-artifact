#!/bin/bash

FLINK_DIR='/home/ubuntu/flink-1.8.1'

LATENCY_FILENAME=$1
THROUGHPUT_FILENAME=$2

# Write CSV headers.
echo "sink_id,timestamp,cur_time,latency" >> $LATENCY_FILENAME
echo "sink_id,timestamp,cur_time,throughput" >> $THROUGHPUT_FILENAME

i=0
for worker in `cat $FLINK_DIR/conf/slaves`; do
    echo $worker

    ssh -o StrictHostKeyChecking=no -i ~/ray_bootstrap_key.pem $worker "grep LATENCY $FLINK_DIR/log/flink-ubuntu-taskexecutor-*.log" | sed 's/\(.*LATENCY [0-9]*\) (\([0-9]*\).*) \([0-9]*\)/\2 \3/' | awk '{ print "'$i',"$1","$2","$2 - $1 }' >> $LATENCY_FILENAME
    ssh -o StrictHostKeyChecking=no -i ~/ray_bootstrap_key.pem $worker "grep THROUGHPUT $FLINK_DIR/log/flink-ubuntu-taskexecutor-0-*.log" | sed 's/\(.*THROUGHPUT [0-9]*\) (\([0-9]*\).*) \([0-9]*\)/\2 \3/' | awk '{ print "'$i',"$1","$2","$3 }' >> $THROUGHPUT_FILENAME

    i=$(( $i + 1 ))
done

