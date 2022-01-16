#!/bin/bash

source ../../shared.sh

mkdir logs
rm -rf logs/*

CTRL_IP=18.18.1.3
LPID=1

set_bridge $CONTROLLER_ETHER
set_bridge $CLIENT1_ETHER

num_worker_nodes=19
mops=( 1 15 30 45 60 75 90 105 120 135 150 155 160 165 170 )

for mop in ${mops[*]}
do
    sudo $NU_DIR/caladan/iokerneld &
    sleep 5
    sudo $NU_DIR/bin/ctrl_main conf/controller &
    sleep 5
    sed "s/constexpr double kTargetMops =.*/constexpr double kTargetMops = $mop;/g" \
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
	ssh $server_ip "cd `pwd`; sudo ./server $conf SRV $CTRL_IP $LPID" &
    done
    sleep 5
    sudo ./server conf/client1 CLT $CTRL_IP $LPID >logs/.tmp &
    ( tail -f -n0 logs/.tmp & ) | grep -q "finish initing"
    client_pids=    
    for i in `seq 1 $num_worker_nodes`
    do
	client_ip=${SERVER_IPS[`expr $i - 1`]}
	if [ $i -ne 1 ]; then
	    ssh $client_ip "sudo $NU_DIR/caladan/iokerneld" &
	fi
	sleep 5
	conf=conf/client`expr $i + 1`
	ssh $client_ip "cd `pwd`; sudo ./client $conf" >logs/$mop.$i &
	client_pids+=" $!"
    done
    wait $client_pids
    for i in `seq 1 $num_worker_nodes`
    do
	client_ip=${SERVER_IPS[`expr $i - 1`]}
	ssh $client_ip "sudo pkill -9 iokerneld;"
    done
    for i in `seq 1 $num_worker_nodes`
    do
	server_ip=${SERVER_IPS[`expr $i - 1 + $num_worker_nodes`]}
	ssh $server_ip "sudo pkill -9 iokerneld; sudo pkill -9 server;"
    done
    sudo pkill -9 iokerneld
    sudo pkill -9 server
    sudo pkill -9 ctrl_main
    sleep 10
done

unset_bridge $CONTROLLER_ETHER
unset_bridge $CLIENT1_ETHER

rm logs/.tmp
