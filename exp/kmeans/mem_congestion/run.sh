#!/bin/bash

source ../../shared.sh
caladan_use_small_rto # Cloudlab's amd instances are buggy and drop pkts from time to time.
                      # To avoid the performance impact caused by packet losses, we use a small RTO.

NUM_SRVS=29
VICTIM_IDX=$NUM_SRVS
STANDBY_IDX=`expr $VICTIM_IDX + 1`
CLT_IDX=`expr $STANDBY_IDX + 1`
CTL_IDX=`expr $CLT_IDX + 1`
LPID=1
KS=23

DIR=`pwd`
PHOENIX_DIR=$DIR/../../../app/phoenix++-1.0/nu
KMEANS_DIR=$PHOENIX_DIR/tests/kmeans/

cp kmeans.cpp $KMEANS_DIR/kmeans.cpp
cd $KMEANS_DIR
sed "s/constexpr int kNumWorkerNodes.*/constexpr int kNumWorkerNodes = $NUM_SRVS;/g" \
    -i kmeans.cpp
pushd $PHOENIX_DIR
make clean
make -j
popd

start_iokerneld $CTL_IDX
start_iokerneld $CLT_IDX
for srv_idx in `seq 1 $NUM_SRVS`
do
    start_iokerneld $srv_idx
done
start_iokerneld $STANDBY_IDX
sleep 5

start_ctrl $CTL_IDX
sleep 5

for srv_idx in `seq 1 $NUM_SRVS`
do
    distribute kmeans $srv_idx
    start_server kmeans $srv_idx $LPID $KS &
done
distribute kmeans $STANDBY_IDX

mem_antagonist=$NU_DIR/bin/bench_real_mem_pressure
distribute $mem_antagonist $VICTIM_IDX
antagonist_log=$DIR/logs/antagonist
run_program $mem_antagonist $VICTIM_IDX $DIR/antagonist.conf 10000 900 >$antagonist_log &
antagonist_pid=$!
( tail -f $antagonist_log & ) | grep -q "waiting for signal"

sleep 5
distribute kmeans $CLT_IDX
start_main_server_isol kmeans $CLT_IDX $LPID $KS >$DIR/logs/$NUM_SRVS &

clt_log=$DIR/logs/$NUM_SRVS
standby_log=$DIR/logs/standby
( tail -f $clt_log & ) | grep -q "Wait for Signal"

start_server kmeans $STANDBY_IDX $LPID $KS >$standby_log &
( tail -f $standby_log & ) | grep -q "Init Finished"

run_cmd $CLT_IDX "sudo pkill -SIGHUP kmeans"

( tail -f $clt_log & ) | grep -q "iter = 10"
run_cmd $VICTIM_IDX "sudo pkill -SIGHUP bench"

( tail -f $clt_log & ) | grep -q "iter = 20"
run_cmd $VICTIM_IDX "sudo pkill -SIGHUP bench"
wait $antagonist_pid

scp $(ssh_ip $VICTIM_IDX):`pwd`/*traces $DIR/logs/

cleanup
sleep 5

caladan_use_default_rto
