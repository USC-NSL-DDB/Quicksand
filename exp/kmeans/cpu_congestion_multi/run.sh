#!/bin/bash

source ../../shared.sh

mkdir logs
rm -rf logs/*

set_bridge $CONTROLLER_ETHER
set_bridge $CLIENT1_ETHER

DIR=`pwd`
CTRL_IP=18.18.1.3
LPID=1
NUM_WORKER_SERVERS=31
SRC_SERVER_IP=$SERVER2_IP
BACKUP_SERVER_IP=$SERVER32_IP
KMEANS_DIR=$NU_DIR/app/phoenix++-1.0/tests/kmeans/

cp $KMEANS_DIR/kmeans.cpp $KMEANS_DIR/kmeans.cpp.bak
cp kmeans.cpp $KMEANS_DIR

cd $NU_DIR/app/phoenix++-1.0/
make -j
cd tests/kmeans/
sed "s/constexpr int kNumWorkerNodes.*/constexpr int kNumWorkerNodes = $NUM_WORKER_SERVERS;/g" \
    -i kmeans.cpp
make clean
make -j
cp kmeans $DIR/main

cd $DIR
for ip in ${REMOTE_SERVER_IPS[*]}
do
    scp main $ip:`pwd`
done

sudo $NU_DIR/caladan/iokerneld &
for i in `seq 1 $((NUM_WORKER_SERVERS-1))`
do
    ip=${REMOTE_SERVER_IPS[`expr $i - 1`]}
    ssh $ip "sudo $NU_DIR/caladan/iokerneld" 1>logs/iokerneld.$i 2>&1 &
done
sleep 5

sudo stdbuf -o0 $NU_DIR/bin/ctrl_main conf/controller >logs/controller &
sleep 5

ssh $SRC_SERVER_IP "cd `pwd`; sudo ../../../bin/bench_real_cpu_pressure conf/client0" 1>logs/pressure 2>&1 &
sleep 5

for i in `seq 1 $((NUM_WORKER_SERVERS-1))`
do
    ip=${REMOTE_SERVER_IPS[`expr $i - 1`]}
    conf=conf/server$i
    ether=`cat $conf | grep host_mac | awk '{print $2}'`
    ssh $ip "cd $DIR; source ../../shared.sh; set_bridge $ether"
    ssh $ip "cd `pwd`; sudo bash -c \"rm -f core; ulimit -c unlimited; stdbuf -o0 ./main $conf SRV $CTRL_IP $LPID\"" 1>logs/server.$i 2>&1 &
done
sleep 5

sudo stdbuf -o0 ./main conf/client1 CLT $CTRL_IP $LPID 1>logs/$NUM_WORKER_SERVERS 2>&1 &
client_pid=$!
( tail -f -n0 logs/$NUM_WORKER_SERVERS & ) | grep -q "waiting for signal"

ether=`cat conf/server$NUM_WORKER_SERVERS | grep host_mac | awk '{print $2}'`
ssh $BACKUP_SERVER_IP "cd $DIR; source ../../shared.sh; set_bridge $ether"
ssh $BACKUP_SERVER_IP "sudo $NU_DIR/caladan/iokerneld" &
sleep 5
ssh $BACKUP_SERVER_IP "cd `pwd`; sudo stdbuf -o0 ./main conf/server$NUM_WORKER_SERVERS SRV $CTRL_IP $LPID" \
    1>logs/server.$NUM_WORKER_SERVERS 2>&1 &
sleep 5

sudo pkill -x -SIGHUP main
( tail -f -n0 logs/$NUM_WORKER_SERVERS & ) | grep -q "iter = 15"
ssh $SRC_SERVER_IP "sudo pkill -SIGHUP bench"
sleep 10

for i in `seq 1 $NUM_WORKER_SERVERS`
do
    ip=${REMOTE_SERVER_IPS[`expr $i - 1`]}
    ether=`cat $conf | grep host_mac | awk '{print $2}'`
    ssh $ip "cd $DIR; source ../../shared.sh; unset_bridge $ether"    
    ssh $ip "sudo pkill -9 iokerneld; sudo pkill -9 main; sudo pkill -9 bench"
done
sudo pkill -9 iokerneld
sudo pkill -9 main

cp $KMEANS_DIR/kmeans.cpp.bak $KMEANS_DIR/kmeans.cpp

unset_bridge $CONTROLLER_ETHER
unset_bridge $CLIENT1_ETHER
