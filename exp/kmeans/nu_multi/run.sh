#!/bin/bash
source ../../shared.sh

mkdir logs
rm -rf logs/*

set_bridge $CONTROLLER_ETHER
set_bridge $CLIENT1_ETHER

DIR=`pwd`
CTRL_IP=18.18.1.3
LPID=1
KMEANS_DIR=$NU_DIR/app/phoenix++-1.0/tests/kmeans/

cp $KMEANS_DIR/kmeans.cpp $KMEANS_DIR/kmeans.cpp.bak
cp kmeans.cpp $KMEANS_DIR

for num_worker_servers in `seq 1 30`
do
    cd $NU_DIR/app/phoenix++-1.0/
    make clean
    make -j
    cd tests/kmeans/
    sed "s/constexpr int kNumWorkerNodes.*/constexpr int kNumWorkerNodes = $num_worker_servers;/g" \
	-i kmeans.cpp
    make clean
    make -j
    cp kmeans $DIR/main
    cd $DIR
    for ip in ${REMOTE_SERVER_IPS[*]}
    do
	scp main $ip:`pwd`
    done
    sleep 5
    sudo $NU_DIR/caladan/iokerneld &
    for i in `seq 1 $num_worker_servers`
    do
	ip=${REMOTE_SERVER_IPS[`expr $i - 1`]}
	ssh $ip "sudo $NU_DIR/caladan/iokerneld" &
    done
    sleep 5
    sudo $NU_DIR/bin/ctrl_main conf/controller &
    sleep 5
    for i in `seq 1 $num_worker_servers`
    do
	ip=${REMOTE_SERVER_IPS[`expr $i - 1`]}
	conf=conf/server$i
	ssh $ip "cd `pwd`; sudo ./main $conf SRV $CTRL_IP $LPID" &
    done
    sleep 5
    sudo ./main conf/client1 CLT $CTRL_IP $LPID >logs/$num_worker_servers 2>&1 &
    ( tail -f -n0 logs/$num_worker_servers & ) | grep -q "iter = 10"
    sudo pkill -9 main
    sudo pkill -9 iokerneld
    for i in `seq 1 $num_worker_servers`
    do
	ip=${REMOTE_SERVER_IPS[`expr $i - 1`]}
	ssh $ip "sudo pkill -9 iokerneld"	
	ssh $ip "sudo pkill -9 main"	    
    done
done

cp $KMEANS_DIR/kmeans.cpp.bak $KMEANS_DIR/kmeans.cpp

unset_bridge $CONTROLLER_ETHER
unset_bridge $CLIENT1_ETHER
