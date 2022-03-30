#!/bin/bash

source ../../shared.sh
mkdir logs
rm -rf logs/*

DIR=`pwd`
CTRL_IP=18.18.1.3
LPID=1
KMEANS_DIR=$NU_DIR/app/phoenix++-1.0/tests/kmeans/

set_bridge $CONTROLLER_ETHER
set_bridge $CLIENT1_ETHER

cp $KMEANS_DIR/kmeans.cpp $KMEANS_DIR/kmeans.cpp.bak
cp kmeans.cpp $KMEANS_DIR

for num_threads in `seq 1 30`
do
    cd $KMEANS_DIR
    sed "s/constexpr int kNumWorkerThreads.*/constexpr int kNumWorkerThreads = $num_threads;/g" \
	-i kmeans.cpp
    make clean
    make -j
    cp kmeans $DIR/main
    cd $DIR
    scp main $SERVER2_IP:`pwd`
    sleep 5
    sudo $NU_DIR/caladan/iokerneld &
    ssh $SERVER2_IP "sudo $NU_DIR/caladan/iokerneld" &    
    sleep 5
    sudo $NU_DIR/bin/ctrl_main conf/controller &
    sleep 5
    ssh $SERVER2_IP "cd `pwd`; sudo ./main conf/server1 SRV $CTRL_IP $LPID" &
    sleep 5
    sudo ./main conf/client1 CLT $CTRL_IP $LPID 1>logs/$num_threads 2>&1 &
    ( tail -f -n0 logs/$num_threads & ) | grep -q "iter = 10"
    sudo pkill -9 iokerneld
    sudo pkill -9 main
    ssh $SERVER2_IP "sudo pkill -9 iokerneld"
    ssh $SERVER2_IP "sudo pkill -9 main"
    sleep 5
done

mv $KMEANS_DIR/kmeans.cpp.bak $KMEANS_DIR/kmeans.cpp

unset_bridge $CONTROLLER_ETHER
unset_bridge $CLIENT1_ETHER
