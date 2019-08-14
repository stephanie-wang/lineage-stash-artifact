#!/bin/bash

set -e

# 10, 100, 1000MB
NUM_NODES=${1:-64}
bytes=25000000

NUM_ITERATIONS=300
CHECKPOINT_INTERVAL=150
FAIL_AT=$(( $CHECKPOINT_INTERVAL + $CHECKPOINT_INTERVAL * 9 / 10))

sleep 1

WORKER_IPS=$(tail -n $NUM_NODES ~/workers.txt)
hosts=""
for worker in $WORKER_IPS;
do
    hosts=$hosts"$worker,"
done
hosts=${hosts:0:-1}
echo $hosts

log="failure-mpi-latency-"$NUM_NODES"-workers-"$bytes"-bytes-`date +%y-%m-%d-%H-%M-%S`.txt"
echo "Logging to file $log"

# NOTE(zongheng): the mca flag must be set https://www.cfd-online.com/Forums/openfoam-installation/164956-host-key-verification-failed-upgrades-openmpi-openfoam-2-3-1-a.html
parallel-ssh -t 0 -i -P -h ~/workers.txt -O "StrictHostKeyChecking=no" "rm /tmp/mpi-checkpoint-*" || true
cmd="/usr/bin/mpiexec.openmpi --mca plm_rsh_no_tree_spawn 1 --mca btl_tcp_if_include ens5 --host $hosts -np $NUM_NODES -N 1 ./allreduce $bytes $NUM_ITERATIONS $CHECKPOINT_INTERVAL $FAIL_AT"
echo $cmd | tee -a $log
$cmd 2>&1 | tee -a $log

cmd="/usr/bin/mpiexec.openmpi --mca plm_rsh_no_tree_spawn 1 --mca btl_tcp_if_include ens5 --host $hosts -np $NUM_NODES -N 1 ./allreduce $bytes $NUM_ITERATIONS $CHECKPOINT_INTERVAL"
echo $cmd | tee -a $log
$cmd 2>&1 | tee -a $log
