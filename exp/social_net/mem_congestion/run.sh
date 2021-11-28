#!/bin/bash

source ../../shared.sh

mkdir logs
rm -rf logs/*

set_bridge $CONTROLLER_ETHER
set_bridge $CLIENT1_ETHER

SRC_SERVER_IP=$SERVER3_IP
DEST_SERVER_IP=$SERVER4_IP
CLIENT_IP=$SERVER5_IP
NGINX_IP=$SERVER7_IP
NGINX_SERVER_CALADAN_IP_AND_MASK=18.18.1.254/24
NGINX_SERVER_NIC=ens1f0
SOCIAL_NET_DIR=`pwd`/../../../app/socialNetwork/single_obj/

ssh $NGINX_IP "sudo apt-get update; sudo apt-get install -y python3-pip; pip3 install aiohttp"

ssh $NGINX_IP "cd $SOCIAL_NET_DIR; ./install_docker.sh"
ssh $NGINX_IP "cd $SOCIAL_NET_DIR; ./down_nginx.sh; ./up_nginx.sh"
ssh $NGINX_IP "sudo ip addr add $NGINX_SERVER_CALADAN_IP_AND_MASK dev $NGINX_SERVER_NIC"

DIR=`pwd`
cd $SOCIAL_NET_DIR
mv src/main.cpp src/main.cpp.bak
mv bench/client.cpp bench/client.cpp.bak
cp $DIR/main.cpp src/main.cpp
cp $DIR/client.cpp bench/client.cpp
cd build
make clean
make -j
cd ..
ssh $SRC_SERVER_IP "mkdir -p `pwd`/build/src"
scp build/src/main $SRC_SERVER_IP:`pwd`/build/src
ssh $DEST_SERVER_IP "mkdir -p `pwd`/build/src"
scp build/src/main $DEST_SERVER_IP:`pwd`/build/src
ssh $CLIENT_IP "mkdir -p `pwd`/build/bench"
scp build/bench/client $CLIENT_IP:`pwd`/build/bench

sudo $NU_DIR/caladan/iokerneld &
sleep 5
sudo build/src/main $DIR/conf/controller CTL 18.18.1.3 &
ssh $SRC_SERVER_IP "cd $DIR; source ../../shared.sh; set_bridge $SERVER1_ETHER"
ssh $SRC_SERVER_IP "sudo $NU_DIR/caladan/iokerneld" &
sleep 5
ssh $SRC_SERVER_IP "cd `pwd`; sudo build/src/main $DIR/conf/server1 SRV 18.18.1.3" >$DIR/logs/src &
sleep 5
sudo build/src/main $DIR/conf/client1 CLT 18.18.1.3 &
ssh $DEST_SERVER_IP "cd $DIR; source ../../shared.sh; set_bridge $SERVER2_ETHER"
ssh $DEST_SERVER_IP "sudo $NU_DIR/caladan/iokerneld" &
sleep 5
ssh $DEST_SERVER_IP "cd `pwd`; sudo build/src/main $DIR/conf/server2 SRV 18.18.1.3" >$DIR/logs/dest &
sleep 5
sudo pkill -SIGHUP main
sleep 5
ssh $NGINX_IP "cd `pwd`; python3 scripts/init_social_graph.py"
ssh $SRC_SERVER_IP "cd $DIR; sudo stdbuf -o0 ../../../bin/bench_real_mem_pressure conf/client3" >$DIR/logs/.pressure &
pid_pressure=$!
( tail -f -n0 $DIR/logs/.pressure & ) | grep -q "waiting for signal"
ssh $CLIENT_IP "sudo $NU_DIR/caladan/iokerneld" &
sleep 5
ssh $CLIENT_IP "cd `pwd`; sudo build/bench/client $DIR/conf/client2" &
pid_client=$!
sleep 13
ssh $SRC_SERVER_IP "sudo pkill -SIGHUP bench_real_mem"
wait $pid_client
ssh $SRC_SERVER_IP "sudo pkill -SIGHUP bench_real_mem"
wait $pid_pressure
scp $CLIENT_IP:$SOCIAL_NET_DIR/timeseries $DIR/logs/
scp $SRC_SERVER_IP:$DIR/alloc_mem_traces $DIR/logs/
scp $SRC_SERVER_IP:$DIR/avail_mem_traces $DIR/logs/

mv src/main.cpp.bak src/main.cpp
mv bench/client.cpp.bak bench/client.cpp

sudo pkill -9 iokerneld; sudo pkill -9 main
ssh $CLIENT_IP "sudo pkill -9 iokerneld;"
ssh $SRC_SERVER_IP "sudo pkill -9 iokerneld; sudo pkill -9 main; sudo pkill -9 bench"
ssh $DEST_SERVER_IP "sudo pkill -9 iokerneld; sudo pkill -9 main"
ssh $NGINX_IP "cd `pwd`; ./down_nginx.sh;"
ssh $NGINX_IP "sudo ip addr delete $NGINX_SERVER_CALADAN_IP_AND_MASK dev $NGINX_SERVER_NIC"
ssh $NGINX_IP "docker rm -vf $(docker ps -aq)"
ssh $NGINX_IP "docker rmi -f $(docker images -aq)"
ssh $NGINX_IP "docker volume prune -f"

unset_bridge $CONTROLLER_ETHER
unset_bridge $CLIENT1_ETHER
ssh $SRC_SERVER_IP "cd $DIR; source ../../shared.sh; unset_bridge $SERVER1_ETHER"
ssh $DEST_SERVER_IP "cd $DIR; source ../../shared.sh; unset_bridge $SERVER2_ETHER"
