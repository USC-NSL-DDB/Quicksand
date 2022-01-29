#!/bin/bash

source ../../shared.sh

CTRL_IP=18.18.1.3
LPID=1

mkdir logs
rm -rf logs/*

set_bridge $CONTROLLER_ETHER
set_bridge $CLIENT1_ETHER

SRC_SERVER_IP=$SERVER2_IP
PROXY_IPS=( $SERVER2_IP $SERVER3_IP $SERVER4_IP $SERVER5_IP $SERVER6_IP $SERVER7_IP $SERVER8_IP $SERVER9_IP $SERVER10_IP $SERVER11_IP \
            $SERVER12_IP $SERVER13_IP $SERVER14_IP $SERVER15_IP $SERVER16_IP $SERVER17_IP $SERVER18_IP $SERVER19_IP $SERVER20_IP $SERVER21_IP \
            $SERVER22_IP $SERVER23_IP $SERVER24_IP $SERVER25_IP $SERVER26_IP $SERVER27_IP $SERVER28_IP $SERVER29_IP $SERVER30_IP $SERVER31_IP )
CLIENT_IPS=( $SERVER32_IP $SERVER33_IP $SERVER34_IP $SERVER35_IP $SERVER36_IP )
NGINX_IP=$SERVER38_IP
NGINX_SERVER_CALADAN_IP_AND_MASK=18.18.1.254/24
NGINX_SERVER_NIC=ens1f0
SOCIAL_NET_DIR=`pwd`/../../../app/socialNetwork/single_obj/

ssh $NGINX_IP "sudo apt-get update; sudo apt-get install -y python3-pip; pip3 install aiohttp"

ssh $NGINX_IP "cd $SOCIAL_NET_DIR; ./install_docker.sh"
ssh $NGINX_IP "cd $SOCIAL_NET_DIR; ./down_nginx.sh; ./up_nginx.sh"
ssh $NGINX_IP "sudo ip addr add $NGINX_SERVER_CALADAN_IP_AND_MASK dev $NGINX_SERVER_NIC"

DIR=`pwd`
cd $SOCIAL_NET_DIR
cp src/states.hpp src/states.hpp.bak
mv src/main.cpp src/main.cpp.bak
mv bench/client.cpp bench/client.cpp.bak
cp $DIR/client.cpp bench
cp $DIR/main.cpp src/main.cpp
sed "s/.*kHashTablePowerNumShards.*/constexpr static uint32_t kHashTablePowerNumShards = 13;/g" -i src/states.hpp
cd build
make clean
make -j
cd ..

for proxy_ip in ${PROXY_IPS[*]}
do
    ssh $proxy_ip "mkdir -p `pwd`/build/src"
    scp build/src/main $proxy_ip:`pwd`/build/src
done

sudo $NU_DIR/caladan/iokerneld &
sleep 5
sudo $NU_DIR/bin/ctrl_main $DIR/conf/controller &
for proxy_ip_idx in `seq 1 ${#PROXY_IPS[@]}`
do
    proxy_ip=${PROXY_IPS[`expr $proxy_ip_idx - 1`]}
    ether=`cat $DIR/conf/server$proxy_ip_idx | grep host_mac | awk '{print $2}'`
    ssh $proxy_ip "cd $DIR; source ../../shared.sh; set_bridge $ether"
    ssh $proxy_ip "sudo $NU_DIR/caladan/iokerneld" &
    stdbuf -o0 ssh $proxy_ip "sleep 5; cd `pwd`; sudo stdbuf -o0 build/src/main $DIR/conf/server$proxy_ip_idx SRV $CTRL_IP $LPID" \
	   >$DIR/logs/server.$proxy_ip_idx &
    sleep 1
done

sleep 10
sudo stdbuf -o0 build/src/main $DIR/conf/client1 CLT $CTRL_IP $LPID >$DIR/logs/.client &
( tail -f -n0 $DIR/logs/.client & ) | grep -q "Done creating proclets"

sudo build/init_graph/init_graph $DIR/conf/client3

ssh $SRC_SERVER_IP "cd $DIR; sudo stdbuf -o0 ../../../bin/bench_real_mem_pressure conf/client2" >$DIR/logs/.pressure &
pid_pressure=$!
( tail -f -n0 $DIR/logs/.pressure & ) | grep -q "waiting for signal"

client_pids=
for client_idx in `seq 1 ${#CLIENT_IPS[@]}`
do
    client_ip=${CLIENT_IPS[`expr $client_idx - 1`]}
    ssh $client_ip "sudo $NU_DIR/caladan/iokerneld" &
    ssh $client_ip "mkdir -p `pwd`/build/bench/"
    scp build/bench/client $client_ip:`pwd`/build/bench/
    conf=$DIR/conf/client`expr $client_idx + 2`
    ssh $client_ip "sleep 5; cd `pwd`; sudo build/bench/client $conf" 1>$DIR/logs/client.$client_idx 2>&1 &
    sleep 1
    client_pids+=" $!"
done

ssh $SRC_SERVER_IP "sleep 15; sudo pkill -SIGHUP bench_real_mem"
wait $client_pids
ssh $SRC_SERVER_IP "sudo pkill -SIGHUP bench_real_mem"
wait $pid_pressure

for client_idx in `seq 1 ${#CLIENT_IPS[@]}`
do
    client_ip=${CLIENT_IPS[`expr $client_idx - 1`]}
    scp $client_ip:$SOCIAL_NET_DIR/timeseries $DIR/logs/timeseries.$client_idx
done
scp $SRC_SERVER_IP:$DIR/alloc_mem_traces $DIR/logs/
scp $SRC_SERVER_IP:$DIR/avail_mem_traces $DIR/logs/

mv src/main.cpp.bak src/main.cpp
mv bench/client.cpp.bak bench/client.cpp
mv src/states.hpp.bak src/states.hpp

sudo pkill -9 iokerneld; sudo pkill -9 main
for proxy_ip in ${PROXY_IPS[*]}
do
    ssh $proxy_ip "sudo pkill -9 iokerneld; sudo pkill -9 main; sudo pkill -9 bench;"
done
for client_ip in ${CLIENT_IPS[*]}
do
    ssh $client_ip "sudo pkill -9 iokerneld"
done
ssh $NGINX_IP "cd `pwd`; ./down_nginx.sh;"
ssh $NGINX_IP "sudo ip addr delete $NGINX_SERVER_CALADAN_IP_AND_MASK dev $NGINX_SERVER_NIC"
ssh $NGINX_IP "docker rm -vf $(docker ps -aq)"
ssh $NGINX_IP "docker rmi -f $(docker images -aq)"
ssh $NGINX_IP "docker volume prune -f"

unset_bridge $CONTROLLER_ETHER
unset_bridge $CLIENT1_ETHER

for proxy_ip_idx in `seq 1 ${#PROXY_IPS[@]}`
do
    proxy_ip=${PROXY_IPS[`expr $proxy_ip_idx - 1`]}
    ether=`cat $DIR/conf/server$proxy_ip_idx | grep host_mac | awk '{print $2}'`
    ssh $proxy_ip "cd $DIR; source ../../shared.sh; unset_bridge $ether"    
done
