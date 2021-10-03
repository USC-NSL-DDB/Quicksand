#!/bin/bash

source ../../shared.sh

DIR=`pwd`
mkdir logs
rm -rf logs/*

set_bridge $CONTROLLER_ETHER
set_bridge $CLIENT1_ETHER

NGINX_SERVER_IP=$SERVER7_IP
NGINX_SERVER_CALADAN_IP_AND_MASK=18.18.1.254/24
NGINX_SERVER_NIC=ens1f0
SOCIAL_NET_DIR=`pwd`/../../../app/socialNetwork/multi_objs/

cd $SOCIAL_NET_DIR
./build.sh

ssh $NGINX_SERVER_IP "sudo apt-get update; sudo apt-get install -y python3-pip; pip3 install aiohttp"

ssh $NGINX_SERVER_IP "cd $SOCIAL_NET_DIR; ./install_docker.sh"
ssh $NGINX_SERVER_IP "cd $SOCIAL_NET_DIR; ./down_nginx.sh; ./up_nginx.sh"
ssh $NGINX_SERVER_IP "sudo ip addr add $NGINX_SERVER_CALADAN_IP_AND_MASK dev $NGINX_SERVER_NIC"

mops=( 1 1.2 1.4 1.6 )

for num_worker_nodes in `seq 1 4`
do
    cd $SOCIAL_NET_DIR
    sed "s/constexpr static uint32_t kNumEntryObjs.*/constexpr static uint32_t kNumEntryObjs = $num_worker_nodes;/g" \
	-i src/BackEndService.cpp
    sed "s/constexpr static uint32_t kNumEntryObjs.*/constexpr static uint32_t kNumEntryObjs = $num_worker_nodes;/g" \
	-i bench/client.cpp
    mop=${mops[`expr $num_worker_nodes - 1`]}
    sed "s/constexpr static double kTargetMops.*/constexpr static double kTargetMops = $mop;/g" \
	-i bench/client.cpp
    cd build
    make clean
    make -j
    for i in `seq 1 $num_worker_nodes`
    do
        ip=${REMOTE_SERVER_IPS[`expr $i - 1`]}
	ssh $ip "mkdir -p `pwd`/src"
	scp src/BackEndService $ip:`pwd`/src
    done
    sudo $NU_DIR/caladan/iokerneld &
    sleep 5
    cd ..
    sudo build/src/BackEndService $DIR/conf/controller CTL 18.18.1.3 &
    sleep 5
    for i in `seq 1 $num_worker_nodes`
    do
        ip=${REMOTE_SERVER_IPS[`expr $i - 1`]}
	ssh $ip "sudo $NU_DIR/caladan/iokerneld" &
	conf=$DIR/conf/server$i
	sleep 5
	ssh $ip "cd $SOCIAL_NET_DIR; sudo build/src/BackEndService $conf SRV 18.18.1.3" &
    done
    sleep 5
    sudo build/src/BackEndService $DIR/conf/client1 CLT 18.18.1.3 &
    sleep 5
    ssh $NGINX_SERVER_IP "cd $SOCIAL_NET_DIR; python3 scripts/init_social_graph.py"
    sleep 5
    sudo build/bench/client $DIR/conf/client2 CLT 18.18.1.3 1>$DIR/logs/$num_worker_nodes 2>&1
    for i in `seq 1 $num_worker_nodes`
    do
        ip=${REMOTE_SERVER_IPS[`expr $i - 1`]}
	ssh $ip "sudo pkill -9 iokerneld"
	ssh $ip "sudo pkill -9 BackEndService"
    done
    sudo pkill -9 iokerneld
    sudo pkill -9 BackEndService
done

unset_bridge $CONTROLLER_ETHER
unset_bridge $CLIENT1_ETHER

ssh $NGINX_SERVER_IP "cd $SOCIAL_NET_DIR; ./down_nginx.sh;"
ssh $NGINX_SERVER_IP "sudo ip addr delete $NGINX_SERVER_CALADAN_IP_AND_MASK dev $NGINX_SERVER_NIC"
ssh $NGINX_SERVER_IP "docker rm -vf $(docker ps -aq)"
ssh $NGINX_SERVER_IP "docker rmi -f $(docker images -aq)"
ssh $NGINX_SERVER_IP "docker volume prune -f"
