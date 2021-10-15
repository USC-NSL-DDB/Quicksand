#!/bin/bash

source ../../shared.sh

DIR=`pwd`
mkdir logs
rm -rf logs/*

NGINX_SERVER_IP=$SERVER7_IP
SOCIAL_NET_DIR=`pwd`/../../../app/socialNetwork/orig/

NGINX_SERVER_100G_IP=$(to_100g_addr $NGINX_SERVER_IP)
CALADAN_IP=`echo $NGINX_SERVER_100G_IP | sed "s/\(.*\)\.\(.*\)\.\(.*\)\.\(.*\)/\1\.\2\.\3\.254/g"`
sed "s/host_addr.*/host_addr $CALADAN_IP/g" -i conf/client.conf

cd $SOCIAL_NET_DIR
sed "s/constexpr static char kHostIP.*/constexpr static char kHostIP[] = \"$NGINX_SERVER_100G_IP\";/g" \
    -i src/Client/client.cpp
./build.sh

ssh $NGINX_SERVER_IP "sudo apt-get update; sudo apt-get install -y python3-pip; pip3 install aiohttp"
ssh $NGINX_SERVER_IP "sudo service irqbalance stop"
ssh $NGINX_SERVER_IP "cd `pwd`/../../../caladan/scripts/; sudo ./set_irq_affinity 0-47 mlx5"
SET_NIC_CMD="sudo ifconfig $DPDK_NIC down;"
SET_NIC_CMD+="sudo ethtool -C $DPDK_NIC adaptive-rx off;"
SET_NIC_CMD+="sudo ethtool -C $DPDK_NIC adaptive-tx off;"
SET_NIC_CMD+="sudo ethtool -C $DPDK_NIC rx-usecs 0;"
SET_NIC_CMD+="sudo ethtool -C $DPDK_NIC rx-frames 0;"
SET_NIC_CMD+="sudo ethtool -C $DPDK_NIC tx-usecs 0;"
SET_NIC_CMD+="sudo ethtool -C $DPDK_NIC tx-frames 0;"
SET_NIC_CMD+="sudo ethtool -N $DPDK_NIC rx-flow-hash udp4 sdfn;"
SET_NIC_CMD+="sudo sysctl net.ipv4.tcp_syncookies=1;"
SET_NIC_CMD+="sudo ifconfig $DPDK_NIC up;"
ssh $NGINX_SERVER_IP "$SET_NIC_CMD"
ssh $NGINX_SERVER_IP "cd $SOCIAL_NET_DIR; ./install_docker.sh"

mops=( 0.002 0.003 0.005 0.007 0.009 )

for mop in ${mops[@]}
do
    cd $SOCIAL_NET_DIR
    ssh $NGINX_SERVER_IP "cd $SOCIAL_NET_DIR; ./up.sh"
    sleep 15
    ssh $NGINX_SERVER_IP "cd $SOCIAL_NET_DIR; python3 scripts/init_social_graph.py"
    sed "s/constexpr static double kTargetMops.*/constexpr static double kTargetMops = $mop;/g" -i src/Client/client.cpp
    cd build
    make clean
    make -j
    sudo $NU_DIR/caladan/iokerneld &
    sleep 5
    cd ..
    sudo build/src/Client/client $DIR/conf/client.conf 1>$DIR/logs/$mop 2>&1
    sudo pkill -9 iokerneld
    ssh $NGINX_SERVER_IP "cd $SOCIAL_NET_DIR; ./down.sh"
    ssh $NGINX_SERVER_IP 'docker rm -vf $(docker ps -aq)'
    ssh $NGINX_SERVER_IP 'docker rmi -f $(docker images -aq)'
    ssh $NGINX_SERVER_IP "docker volume prune -f"
done

ssh $NGINX_SERVER_IP "cd $SOCIAL_NET_DIR; ./down.sh;"
