#!/bin/bash

source ../../shared.sh

DIR=`pwd`
mkdir logs
rm -rf logs/*

DOCKER_MASTER_IP=${REMOTE_SERVER_IPS[0]}
SOCIAL_NET_DIR=`pwd`/../../../app/socialNetwork/orig/

cd $SOCIAL_NET_DIR
./build.sh

mops=( 0.02 0.03 0.04 0.04 )

for num_worker_nodes in `seq 1 4`
do
    mop=${mops[`expr $num_worker_nodes - 1`]}

    ssh $DOCKER_MASTER_IP "cd $SOCIAL_NET_DIR; ./install_docker.sh"
    join_cmd=`ssh $DOCKER_MASTER_IP "docker swarm init --advertise-addr $DOCKER_MASTER_IP" | grep token | head -n 1`
    for i in `seq 1 $num_worker_nodes`
    do
	ssh ${REMOTE_SERVER_IPS[$i]} "cd $SOCIAL_NET_DIR; ./install_docker.sh"
	ssh ${REMOTE_SERVER_IPS[$i]} "$join_cmd"
    done

    cd $SOCIAL_NET_DIR
    ssh $DOCKER_MASTER_IP "cd $SOCIAL_NET_DIR; docker stack deploy --compose-file=docker-compose-swarm.yml socialnet"
    sleep 60

    for i in `seq 1 $num_worker_nodes`
    do
	ip=${REMOTE_SERVER_IPS[$i]}
	has_openresty=`ssh $ip "docker container ls | grep openresty | wc -l"`
	has_composepost=`ssh $ip "docker container ls | grep ComposePostService | wc -l"`
	has_usertimeline=`ssh $ip "docker container ls | grep UserTimelineService | wc -l"`
	has_socialgraph=`ssh $ip "docker container ls | grep SocialGraphService | wc -l"`
	has_hometimeline=`ssh $ip "docker container ls | grep HomeTimelineService | wc -l"`
	if [[ $has_openresty -ne 0 ]]; then
	    ssh $ip "sudo apt-get update; sudo apt-get install -y python3-pip; pip3 install aiohttp"
	    ssh $ip "cd $SOCIAL_NET_DIR; python3 scripts/init_social_graph.py"
	fi
	if [[ $has_composepost -ne 0 ]]; then
	    composepost_ip=$(to_100g_addr $ip)
	fi
	if [[ $has_usertimeline -ne 0 ]]; then
	    usertimeline_ip=$(to_100g_addr $ip)
	fi
	if [[ $has_socialgraph -ne 0 ]]; then
	    socialgraph_ip=$(to_100g_addr $ip)
	fi
	if [[ $has_hometimeline -ne 0 ]]; then
	    hometimeline_ip=$(to_100g_addr $ip)
	fi
    done

    sed "s/constexpr static char kUserTimeLineIP.*/constexpr static char kUserTimeLineIP[] = \"$usertimeline_ip\";/g" \
	-i src/ClientSwarm/client_swarm.cpp
    sed "s/constexpr static char kHomeTimeLineIP.*/constexpr static char kHomeTimeLineIP[] = \"$hometimeline_ip\";/g" \
	-i src/ClientSwarm/client_swarm.cpp
    sed "s/constexpr static char kComposePostIP.*/constexpr static char kComposePostIP[] = \"$composepost_ip\";/g" \
	-i src/ClientSwarm/client_swarm.cpp
    sed "s/constexpr static char kSocialGraphIP.*/constexpr static char kSocialGraphIP[] = \"$socialgraph_ip\";/g" \
	-i src/ClientSwarm/client_swarm.cpp
    sed "s/constexpr static double kTargetMops.*/constexpr static double kTargetMops = $mop;/g" \
	-i src/ClientSwarm/client_swarm.cpp
    cd build
    make clean
    make -j
    sudo $NU_DIR/caladan/iokerneld &
    sleep 5
    cd ..
    sudo build/src/ClientSwarm/client_swarm $DIR/conf/client.conf 1>$DIR/logs/$num_worker_nodes 2>&1
    sudo pkill -9 iokerneld

    ssh $DOCKER_MASTER_IP "docker stack rm socialnet"
    sleep 15
    for i in `seq 1 $num_worker_nodes`
    do
	ssh ${REMOTE_SERVER_IPS[$i]} "docker swarm leave --force"
    done
    ssh $DOCKER_MASTER_IP "docker swarm leave --force"
    for i in `seq 0 $num_worker_nodes`
    do
	ip=${REMOTE_SERVER_IPS[$i]}
	ssh $ip 'docker rm -vf $(docker ps -aq)'
	ssh $ip 'docker rmi -f $(docker images -aq)'
	ssh $ip "docker volume prune -f"
    done
done
