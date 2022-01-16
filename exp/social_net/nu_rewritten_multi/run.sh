#!/bin/bash

source ../../shared.sh

DIR=`pwd`
CTRL_IP=18.18.1.3
LPID=1

mkdir logs
rm -rf logs/*

set_bridge $CONTROLLER_ETHER
set_bridge $CLIENT1_ETHER

NGINX_SERVER_IP=$SERVER40_IP
NGINX_SERVER_CALADAN_IP_AND_MASK=18.18.1.254/24
NGINX_SERVER_NIC=ens1f0
SOCIAL_NET_DIR=`pwd`/../../../app/socialNetwork/single_obj/
CLIENT_IPS=( $SERVER35_IP $SERVER36_IP $SERVER37_IP $SERVER38_IP $SERVER39_IP )

cd $SOCIAL_NET_DIR
./build.sh

ssh $NGINX_SERVER_IP "sudo apt-get update; sudo apt-get install -y python3-pip; pip3 install aiohttp"

ssh $NGINX_SERVER_IP "cd $SOCIAL_NET_DIR; ./install_docker.sh"
ssh $NGINX_SERVER_IP "cd $SOCIAL_NET_DIR; ./down_nginx.sh; ./up_nginx.sh"
ssh $NGINX_SERVER_IP "sudo ip addr add $NGINX_SERVER_CALADAN_IP_AND_MASK dev $NGINX_SERVER_NIC"

mops=( 2 2.5 3 3.5 4 4.5 5 5.5 6 6.5 7 7.5 8 8.5 9 9.5 10 10.5 11 11.5 12 12.5 13 13.5 14 14.5 15 15.5 16 6 )

cd $SOCIAL_NET_DIR
mv bench/client.cpp bench/client.cpp.bak
cp $DIR/client.cpp bench/client.cpp

for num_worker_nodes in `seq 1 30`
do    
    cd $SOCIAL_NET_DIR
    sed "s/constexpr uint32_t kNumEntryObjs.*/constexpr uint32_t kNumEntryObjs = $num_worker_nodes;/g" \
	-i src/main.cpp
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
	scp src/main $ip:`pwd`/src
    done
    sudo $NU_DIR/caladan/iokerneld &
    sleep 5
    cd ..
    sudo $NU_DIR/bin/ctrl_main $DIR/conf/controller $CTRL_IP &
    sleep 5
    for i in `seq 1 $num_worker_nodes`
    do
        ip=${REMOTE_SERVER_IPS[`expr $i - 1`]}
	ssh $ip "sudo $NU_DIR/caladan/iokerneld" &
	conf=$DIR/conf/server$i
	sleep 5
	ssh $ip "cd $SOCIAL_NET_DIR; sudo build/src/main $conf SRV $CTRL_IP $LPID" &
    done
    sleep 5
    sudo build/src/main $DIR/conf/client1 CLT $CTRL_IP $LPID &
    sleep 5
    ssh $NGINX_SERVER_IP "cd $SOCIAL_NET_DIR; python3 scripts/init_social_graph.py"
    client_pids=
    for client_idx in `seq 1 ${#CLIENT_IPS[@]}`
    do
	client_ip=${CLIENT_IPS[`expr $client_idx - 1`]}
	ssh $client_ip "sudo $NU_DIR/caladan/iokerneld" &
	sleep 5
	ssh $client_ip "mkdir -p `pwd`/build/bench/"
	scp build/bench/client $client_ip:`pwd`/build/bench/
	conf=$DIR/conf/client`expr $client_idx + 1`
	ssh $client_ip "sudo `pwd`/build/bench/client $conf" 1>$DIR/logs/$num_worker_nodes.$client_idx 2>&1 &
	client_pids+=" $!"
    done
    wait $client_pids
    for i in `seq 1 $num_worker_nodes`
    do
        ip=${REMOTE_SERVER_IPS[`expr $i - 1`]}
	ssh $ip "sudo pkill -9 iokerneld"
	ssh $ip "sudo pkill -9 main"
    done
    for client_ip in ${CLIENT_IPS[*]}
    do
	ssh $client_ip "sudo pkill -9 iokerneld"
    done
    sudo pkill -9 iokerneld
    sudo pkill -9 main
    sudo pkill -9 ctrl_main
done

cd $SOCIAL_NET_DIR
mv bench/client.cpp.bak bench/client.cpp

unset_bridge $CONTROLLER_ETHER
unset_bridge $CLIENT1_ETHER

ssh $NGINX_SERVER_IP "cd $SOCIAL_NET_DIR; ./down_nginx.sh;"
ssh $NGINX_SERVER_IP "sudo ip addr delete $NGINX_SERVER_CALADAN_IP_AND_MASK dev $NGINX_SERVER_NIC"
ssh $NGINX_SERVER_IP "docker rm -vf $(docker ps -aq)"
ssh $NGINX_SERVER_IP "docker rmi -f $(docker images -aq)"
ssh $NGINX_SERVER_IP "docker volume prune -f"
