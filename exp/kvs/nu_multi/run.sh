#!/bin/bash

source ../../shared.sh

mkdir logs
rm -rf logs/*

set_bridge $CONTROLLER_ETHER
set_bridge $CLIENT1_ETHER

for num_worker_nodes in `seq 1 4`
do
    sudo $NU_DIR/caladan/iokerneld &
    sleep 5
    sudo ./server conf/controller CTL 18.18.1.3 &
    sleep 5
    sed "s/constexpr uint32_t kNumProxies.*/constexpr uint32_t kNumProxies = $num_worker_nodes;/g" \
	-i server.cpp
    sed "s/constexpr uint32_t kNumProxies.*/constexpr uint32_t kNumProxies = $num_worker_nodes;/g" \
	-i client.cpp
    make -j
    for i in `seq 1 $num_worker_nodes`
    do
	client_ip=${SERVER_IPS[`expr $i - 1`]}
	scp client $client_ip:`pwd`/

	server_ip=${SERVER_IPS[`expr $i - 1 + $num_worker_nodes`]}
	scp server $server_ip:`pwd`/
        ssh $server_ip "sudo $NU_DIR/caladan/iokerneld" &
	sleep 5

	conf=conf/server$i
	ssh $server_ip "cd `pwd`; sudo ./server $conf SRV 18.18.1.3" &
    done
    sleep 5
    sudo ./server conf/client1 CLT 18.18.1.3 >logs/.tmp &
    ( tail -f -n0 logs/.tmp & ) | grep -q "finish initing"
    for i in `seq 1 $num_worker_nodes`
    do
	client_ip=${SERVER_IPS[`expr $i - 1`]}
	if [ $i -ne 1 ]; then
	    ssh $client_ip "sudo $NU_DIR/caladan/iokerneld" &
	fi
	sleep 5
	conf=conf/client`expr $i + 1`
	ssh $client_ip "cd `pwd`; sudo ./client $conf" >logs/$num_worker_nodes.$i &
    done
    sleep 15
    for i in `seq 1 $num_worker_nodes`
    do
	client_ip=${SERVER_IPS[`expr $i - 1`]}
	ssh $client_ip "sudo pkill -9 iokerneld; sudo pkill -9 client;"
    done
    for i in `seq 1 $num_worker_nodes`
    do
	server_ip=${SERVER_IPS[`expr $i - 1 + $num_worker_nodes`]}
	ssh $server_ip "sudo pkill -9 iokerneld; sudo pkill -9 server;"
    done
    sudo pkill -9 iokerneld
    sudo pkill -9 server
    sleep 10
done

unset_bridge $CONTROLLER_ETHER
unset_bridge $CLIENT1_ETHER

rm logs/.tmp
