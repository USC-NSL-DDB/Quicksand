#!/bin/bash

source ../../shared.sh

mkdir logs
rm -rf logs/*

CTRL_IP=18.18.1.3
LPID=1

set_bridge $CONTROLLER_ETHER
set_bridge $CLIENT1_ETHER

SRC_SERVER_IP=$SERVER18_IP

num_worker_nodes=17
mop=170

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
    conf=conf/server$i
    ether=`cat $conf | grep host_mac | awk '{print $2}'`
    ssh $server_ip "cd $NU_DIR/exp; source shared.sh; set_bridge $ether"
    ssh $server_ip "sleep 5; cd `pwd`; sudo stdbuf -o0 ./server $conf SRV $CTRL_IP $LPID" >logs/server.$i &
    sleep 1
done

sleep 5
sudo ./server conf/client1 CLT $CTRL_IP $LPID >logs/.tmp &
( tail -f -n0 logs/.tmp & ) | grep -q "finish initing"

ssh $SRC_SERVER_IP "cd `pwd`; sudo stdbuf -o0 ../../../bin/bench_real_mem_pressure conf/client0" >logs/.pressure &
pressure_pid=$!
( tail -f -n0 logs/.pressure & ) | grep -q "waiting for signal"

client_pids=
for i in `seq 1 $num_worker_nodes`
do
    client_ip=${SERVER_IPS[`expr $i - 1`]}
    if [ $i -ne 1 ]; then
	ssh $client_ip "sudo $NU_DIR/caladan/iokerneld" &
    fi
    conf=conf/client`expr $i + 1`
    ssh $client_ip "sleep 5; cd `pwd`; sudo ./client $conf" >logs/$mop.$i &
    client_pids+=" $!"
    sleep 1
done

sleep 40
ssh $SRC_SERVER_IP "sudo pkill -SIGHUP bench_real_mem"
wait $client_pids
ssh $SRC_SERVER_IP "sudo pkill -SIGHUP bench_real_mem"
wait $pressure_pid

for i in `seq 1 $num_worker_nodes`
do
    client_ip=${SERVER_IPS[`expr $i - 1`]}
    ssh $client_ip "sudo pkill -9 iokerneld;"
done

for i in `seq 1 $num_worker_nodes`
do
    server_ip=${SERVER_IPS[`expr $i - 1 + $num_worker_nodes`]}
    conf=conf/server$i
    ether=`cat $conf | grep host_mac | awk '{print $2}'`
    ssh $server_ip "cd $NU_DIR/exp; source shared.sh; unset_bridge $ether"
    ssh $server_ip "sudo pkill -9 iokerneld; sudo pkill -9 server;"
done 
sudo pkill -9 iokerneld
sudo pkill -9 server
sudo pkill -9 ctrl_main

unset_bridge $CONTROLLER_ETHER
unset_bridge $CLIENT1_ETHER
