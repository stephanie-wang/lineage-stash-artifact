#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
FLINK_DIR='/home/ubuntu/flink-1.8.1'
HADOOP_DIR='/home/ubuntu/hadoop-3.1.2'

if [[ $# -lt 1 || $# > 5 ]]
then
    echo "usage: ./run_job.sh <master ip> <num workers> <total throughput> <test failure>"
    exit
fi

MASTER_IP=$1
NUM_WORKERS=$2
TOTAL_THROUGHPUT=${3:-$(( 12500 * $NUM_WORKERS ))}
TEST_FAILURE=${4:-0}
RESTART_HDFS=${5:-1}


# Stop Flink.
$FLINK_DIR/bin/stop-cluster.sh
$FLINK_DIR/bin/stop-cluster.sh
parallel-ssh -t 0 -i -P -h ~/workers.txt -x "-o StrictHostKeyChecking=no -i ~/ray_bootstrap_key.pem" "rm -r ~/flink-1.8.1/log/*"
parallel-ssh -t 0 -i -P -h ~/workers.txt -x "-o StrictHostKeyChecking=no -i ~/ray_bootstrap_key.pem" "pkill -9 yes"
rm $FLINK_DIR/log/*

# Stop HDFS.
if [[ $RESTART_HDFS -eq 1 ]]; then
    $HADOOP_DIR/sbin/stop-dfs.sh
    $HADOOP_DIR/sbin/stop-dfs.sh
    rm -r $HADOOP_DIR/namenode
    parallel-ssh -t 0 -i -P -h ~/workers.txt -x "-o StrictHostKeyChecking=no -i ~/ray_bootstrap_key.pem" "rm $HADOOP_DIR/logs/hadoop-ubuntu-* && rm -r $HADOOP_DIR/datanode"
fi

# Sync the config with all workers.
python $DIR/format_config.py --master-ip $MASTER_IP --num-nodes $NUM_WORKERS
num_workers=$(( `wc -l ~/workers.txt | awk '{ print $1 }'` - 1 ))
for worker in `tail -n $num_workers ~/workers.txt`; do
    echo $worker
    rsync -e "ssh -o StrictHostKeyChecking=no -i ~/ray_bootstrap_key.pem" -az "/home/ubuntu/flink-1.8.1" $worker:/home/ubuntu & sleep 0.5
done
wait

# Start HDFS.
if [[ $RESTART_HDFS -eq 1 ]]; then
    $HADOOP_DIR/bin/hdfs namenode -format -nonInteractive
    $HADOOP_DIR/sbin/start-dfs.sh
fi

$FLINK_DIR/bin/start-cluster.sh

latency_prefix=flink-latency-$NUM_WORKERS-workers-$TOTAL_THROUGHPUT-tput-
throughput_prefix=flink-throughput-$NUM_WORKERS-workers-$TOTAL_THROUGHPUT-tput-
DURATION=60

CHECKPOINT_DURATION=30
if [[ $TEST_FAILURE -eq 1 ]]; then
    DURATION=$(( CHECKPOINT_DURATION * 4 ))
    latency_prefix=failure-$latency_prefix$CHECKPOINT_DURATION-checkpoint-
    throughput_prefix=failure-$throughput_prefix$CHECKPOINT_DURATION-checkpoint-
fi

date=`date +%h-%d-%H-%M-%S`.csv
latency_file=$latency_prefix$date
throughput_file=$throughput_prefix$date
echo "Logging to file $latency_file..."


SOURCE_RATE=$(( $TOTAL_THROUGHPUT / $NUM_WORKERS ))
HADOOP_CLASSPATH=`$HADOOP_DIR/bin/hadoop classpath` $FLINK_DIR/bin/flink run ~/flink-wordcount/target/flink-wordcount-0.1-jar-with-dependencies.jar --port 6123 \
    --source-rate $SOURCE_RATE \
    --parallelism $NUM_WORKERS \
    --checkpoint-interval $CHECKPOINT_DURATION \
    --duration $DURATION &

if [[ $TEST_FAILURE -eq 1 ]]
then
    sleep 5
    SLEEP=45
    WORKER_TO_KILL=`head -n 1 $FLINK_DIR/conf/slaves`
    echo "Killing worker $WORKER_TO_KILL after $SLEEP more seconds..."
    ssh -i ~/ray_bootstrap_key.pem $WORKER_TO_KILL "$DIR/kill_worker.sh $SLEEP" &
fi

# Wait for the job to complete.
wait

echo "Collecting stats from workers..."
$DIR/collect_latencies.sh $DIR/$latency_file $DIR/$throughput_file
