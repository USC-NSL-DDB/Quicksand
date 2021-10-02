#!/bin/bash

source ../../shared.sh

DIR=`pwd`
mkdir logs
rm -rf logs/*

NGINX_SERVER_IP=$SERVER7_IP
SOCIAL_NET_DIR=`pwd`/../../../app/socialNetwork/orig/

NGINX_SERVER_SIB_IP=$(to_sib_addr $NGINX_SERVER_IP)
CALADAN_IP=`echo $NGINX_SERVER_SIB_IP | sed "s/\(.*\)\.\(.*\)\.\(.*\)\.\(.*\)/\1\.\2\.\3\.254/g"`
sed "s/host_addr.*/host_addr $CALADAN_IP/g" -i conf/client.conf

cd $SOCIAL_NET_DIR
sed "s/constexpr static char kLocalHost.*/constexpr static char kLocalHost[] = \"$NGINX_SERVER_SIB_IP\";/g" \
    -i src/Client/client.cpp
./build.sh

ssh $NGINX_SERVER_IP "sudo apt-get install -y python3-pip; pip3 install aiohttp"

#mops=( 0.002 0.003 0.005 0.007 0.009 )
mops=( 0.007 0.009 )

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
    ssh $NGINX_SERVER_IP "docker rm -vf $(docker ps -aq)"
    ssh $NGINX_SERVER_IP "docker rmi -f $(docker images -aq)"
    ssh $NGINX_SERVER_IP "docker volume prune -f"
done

ssh $NGINX_SERVER_IP "cd $SOCIAL_NET_DIR; ./down.sh;"
