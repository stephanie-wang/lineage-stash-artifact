#!/bin/bash


FAIL_TIME=${1:-0}

export HADOOP_CLASSPATH=`/home/ubuntu/hadoop-3.1.2/bin/hadoop classpath`
# Start a new task manager so it is ready by the time the first one dies.
~/flink-1.8.1/bin/taskmanager.sh start

pid=`ps -ef | grep TaskManagerRunner | awk '{ print $2 }' | head -n 1`
echo "PID is $pid"

echo "Sleeping for $FAIL_TIME seconds"
sleep $FAIL_TIME

# Kill the task manager
echo "Killing task manager $pid"
kill -9 $pid
sleep 1

wait
