#!/bin/bash

source ../../shared.sh

CTRL_IP=18.18.1.3
LPID=1

DIR=`pwd`
mkdir logs
rm -rf logs/*

set_bridge $CONTROLLER_ETHER
set_bridge $CLIENT1_ETHER

NGINX_SERVER_IP=$SERVER32_IP
NGINX_SERVER_CALADAN_IP_AND_MASK=18.18.1.254/24
NGINX_SERVER_NIC=ens1f0
BACKEND_SERVER_IP=$SERVER2_IP
SOCIAL_NET_DIR=`pwd`/../../../app/socialNetwork/single_obj/

cd $SOCIAL_NET_DIR
./build.sh

ssh $NGINX_SERVER_IP "sudo apt-get update; sudo apt-get install -y python3-pip; pip3 install aiohttp"

ssh $NGINX_SERVER_IP "cd $SOCIAL_NET_DIR; ./install_docker.sh"
ssh $NGINX_SERVER_IP "cd $SOCIAL_NET_DIR; ./down_nginx.sh; ./up_nginx.sh"
ssh $NGINX_SERVER_IP "sudo ip addr add $NGINX_SERVER_CALADAN_IP_AND_MASK dev $NGINX_SERVER_NIC"

mops=( 0.1 0.2 0.3 0.4 0.5 0.6 0.7 0.8 0.82 0.84 0.86 0.88 0.90 0.92 )

for mop in ${mops[@]}
do    
    cd $SOCIAL_NET_DIR
    sed "s/constexpr uint32_t kNumEntryObjs.*/constexpr uint32_t kNumEntryObjs = 1;/g" -i src/main.cpp
    sed "s/constexpr static uint32_t kNumEntryObjs.*/constexpr static uint32_t kNumEntryObjs = 1;/g" -i bench/client.cpp
    sed "s/constexpr static double kTargetMops.*/constexpr static double kTargetMops = $mop;/g" -i bench/client.cpp
    cd build
    make clean
    make -j
    ssh $BACKEND_SERVER_IP "mkdir -p `pwd`/src"
    scp src/main $BACKEND_SERVER_IP:`pwd`/src
    sudo $NU_DIR/caladan/iokerneld &
    ssh $BACKEND_SERVER_IP "sudo $NU_DIR/caladan/iokerneld" &
    sleep 5
    cd ..    
    sudo $NU_DIR/bin/ctrl_main $DIR/conf/controller CTL 18.18.1.3 >$DIR/logs/controller &
    sleep 5    
    ssh $BACKEND_SERVER_IP "cd $SOCIAL_NET_DIR; sudo bash -c \"ulimit -c unlimited; build/src/main $DIR/conf/server1 SRV $CTRL_IP $LPID\"" >$DIR/logs/src &
    sleep 5
    sudo stdbuf -o0 build/src/main $DIR/conf/client1 CLT $CTRL_IP $LPID >$DIR/logs/client &
    ( tail -f -n0 $DIR/logs/client & ) | grep -q "Done creating proclets"
    ssh $NGINX_SERVER_IP "cd $SOCIAL_NET_DIR; python3 scripts/init_social_graph.py"    
    sleep 5    
    sudo build/bench/client $DIR/conf/client2 1>$DIR/logs/$mop 2>&1
    ssh $BACKEND_SERVER_IP "sudo pkill -9 iokerneld"    
    ssh $BACKEND_SERVER_IP "sudo pkill -9 main"    
    sudo pkill -9 iokerneld    
    sudo pkill -9 main
done

unset_bridge $CONTROLLER_ETHER
unset_bridge $CLIENT1_ETHER

ssh $NGINX_SERVER_IP "cd $SOCIAL_NET_DIR; ./down_nginx.sh;"
ssh $NGINX_SERVER_IP "sudo ip addr delete $NGINX_SERVER_CALADAN_IP_AND_MASK dev $NGINX_SERVER_NIC"
ssh $NGINX_SERVER_IP "docker rm -vf $(docker ps -aq)"
ssh $NGINX_SERVER_IP "docker rmi -f $(docker images -aq)"
ssh $NGINX_SERVER_IP "docker volume prune -f"
