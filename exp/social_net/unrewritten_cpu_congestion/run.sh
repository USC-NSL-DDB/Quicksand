#!/bin/bash

source ../../shared.sh

mkdir logs
rm -rf logs/*

set_bridge $CONTROLLER_ETHER
set_bridge $CLIENT1_ETHER

SRC_SERVER_IP=$SERVER2_IP
DEST_SERVER_IP=$SERVER3_IP
CLIENT_IP=$SERVER4_IP
NGINX_IP=$SERVER5_IP
NGINX_SERVER_CALADAN_IP_AND_MASK=18.18.1.254/24
NGINX_SERVER_NIC=ens1f0
SOCIAL_NET_DIR=`pwd`/../../../app/socialNetwork/multi_objs/
CTRL_IP=18.18.1.3
LPID=1

ssh $NGINX_IP "sudo apt-get update; sudo apt-get install -y python3-pip; pip3 install aiohttp"

ssh $NGINX_IP "cd $SOCIAL_NET_DIR; ./install_docker.sh"
ssh $NGINX_IP "cd $SOCIAL_NET_DIR; ./down_nginx.sh; ./up_nginx.sh"
ssh $NGINX_IP "sudo ip addr add $NGINX_SERVER_CALADAN_IP_AND_MASK dev $NGINX_SERVER_NIC"

DIR=`pwd`
cd $SOCIAL_NET_DIR
mv src/BackEndService.cpp src/BackEndService.cpp.bak
mv bench/client.cpp bench/client.cpp.bak
cp $DIR/BackEndService.cpp src/BackEndService.cpp
cp $DIR/client.cpp bench/client.cpp
cd build
make clean
make -j
cd ..
ssh $SRC_SERVER_IP "mkdir -p `pwd`/build/src"
scp build/src/BackEndService $SRC_SERVER_IP:`pwd`/build/src
ssh $DEST_SERVER_IP "mkdir -p `pwd`/build/src"
scp build/src/BackEndService $DEST_SERVER_IP:`pwd`/build/src
ssh $CLIENT_IP "mkdir -p `pwd`/build/bench"
scp build/bench/client $CLIENT_IP:`pwd`/build/bench

sudo $NU_DIR/caladan/iokerneld &
sleep 5
sudo $NU_DIR/bin/ctrl_main $DIR/conf/controller $CTRL_IP &
ssh $SRC_SERVER_IP "cd $DIR; source ../../shared.sh; set_bridge $SERVER1_ETHER"
ssh $SRC_SERVER_IP "sudo $NU_DIR/caladan/iokerneld" &
sleep 5
ssh $SRC_SERVER_IP "cd `pwd`; sudo build/src/BackEndService $DIR/conf/server1 SRV $CTRL_IP $LPID" >$DIR/logs/src &
sleep 5
sudo build/src/BackEndService $DIR/conf/client1 CLT $CTRL_IP $LPID &
ssh $DEST_SERVER_IP "cd $DIR; source ../../shared.sh; set_bridge $SERVER2_ETHER"
ssh $DEST_SERVER_IP "sudo $NU_DIR/caladan/iokerneld" &
sleep 5
ssh $DEST_SERVER_IP "cd `pwd`; sudo build/src/BackEndService $DIR/conf/server2 SRV $CTRL_IP $LPID" >$DIR/logs/dest &
sleep 5
sudo pkill -SIGHUP BackEndService
sleep 5
ssh $NGINX_IP "cd `pwd`; python3 scripts/init_social_graph.py"
ssh $SRC_SERVER_IP "cd $DIR; sudo stdbuf -o0 ../../../bin/bench_real_cpu_pressure conf/client3" &
sleep 5
ssh $CLIENT_IP "sudo $NU_DIR/caladan/iokerneld" &
sleep 5
ssh $CLIENT_IP "cd `pwd`; sudo build/bench/client $DIR/conf/client2" &
pid_client=$!
sleep 25
ssh $SRC_SERVER_IP "sudo pkill -SIGHUP bench_real_cpu"
wait $pid_client
scp $CLIENT_IP:$SOCIAL_NET_DIR/timeseries $DIR/logs/

mv src/BackEndService.cpp.bak src/BackEndService.cpp
mv bench/client.cpp.bak bench/client.cpp

sudo pkill -9 iokerneld; sudo pkill -9 BackEndService; sudo pkill -9 ctrl_main
ssh $CLIENT_IP "sudo pkill -9 iokerneld;"
ssh $SRC_SERVER_IP "sudo pkill -9 iokerneld; sudo pkill -9 BackEndService; sudo pkill -9 bench"
ssh $DEST_SERVER_IP "sudo pkill -9 iokerneld; sudo pkill -9 BackEndService"
ssh $NGINX_IP "cd `pwd`; ./down_nginx.sh;"
ssh $NGINX_IP "sudo ip addr delete $NGINX_SERVER_CALADAN_IP_AND_MASK dev $NGINX_SERVER_NIC"
ssh $NGINX_IP "docker rm -vf $(docker ps -aq)"
ssh $NGINX_IP "docker rmi -f $(docker images -aq)"
ssh $NGINX_IP "docker volume prune -f"

unset_bridge $CONTROLLER_ETHER
unset_bridge $CLIENT1_ETHER
ssh $SRC_SERVER_IP "cd $DIR; source ../../shared.sh; unset_bridge $SERVER1_ETHER"
ssh $DEST_SERVER_IP "cd $DIR; source ../../shared.sh; unset_bridge $SERVER2_ETHER"
