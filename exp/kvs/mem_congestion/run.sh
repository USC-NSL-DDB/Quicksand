#!/bin/bash

source ../../shared.sh
CTRL_IP=18.18.1.3
LPID=1

mkdir logs
rm -rf logs/*

set_bridge $CONTROLLER_ETHER
set_bridge $CLIENT1_ETHER

SRC_SERVER_IP=$SERVER2_IP
DEST_SERVER_IP=$SERVER3_IP
CLIENT_IP=$SERVER4_IP

make clean
make -j

scp server $SRC_SERVER_IP:`pwd`
scp server $DEST_SERVER_IP:`pwd`
scp client $CLIENT_IP:`pwd`

sudo $NU_DIR/caladan/iokerneld &
sleep 5
sudo $NU_DIR/bin/ctrl_main conf/controller &
ssh $SRC_SERVER_IP "cd `pwd`; source ../../shared.sh; set_bridge $SERVER1_ETHER"
ssh $SRC_SERVER_IP "sudo cset shield --exec -- $NU_DIR/caladan/iokerneld" &
sleep 5
ssh $SRC_SERVER_IP "cd `pwd`; sudo cset shield --exec -- ./server conf/server1 SRV $CTRL_IP $LPID" >logs/.src &
sleep 5
sudo ./server conf/client1 CLT $CTRL_IP $LPID &
( tail -f -n0 logs/.server & ) | grep -q "finish initing"
ssh $DEST_SERVER_IP "cd `pwd`; source ../../shared.sh; set_bridge $SERVER2_ETHER"
ssh $DEST_SERVER_IP "sudo cset shield --exec -- $NU_DIR/caladan/iokerneld" &
sleep 5
ssh $DEST_SERVER_IP "cd `pwd`; sudo cset shield --exec -- ./server conf/server2 SRV $CTRL_IP $LPID" >logs/.dest &
sleep 5
sudo pkill -SIGHUP server
ssh $SRC_SERVER_IP "cd `pwd`; sudo cset shield --exec -- stdbuf -o0 ../../../bin/bench_real_mem_pressure conf/client3" >logs/.pressure &
pid_pressure=$!
( tail -f -n0 logs/.pressure & ) | grep -q "waiting for signal"
ssh $CLIENT_IP "sudo $NU_DIR/caladan/iokerneld" &
sleep 5
ssh $CLIENT_IP "cd `pwd`; sudo stdbuf -o0 ./client conf/client2" >logs/client &
pid_client=$!
sleep 25
ssh $SRC_SERVER_IP "sudo pkill -SIGHUP bench_real_mem"
wait $pid_client
ssh $SRC_SERVER_IP "sudo pkill -SIGHUP bench_real_mem"
wait $pid_pressure
scp $SRC_SERVER_IP:`pwd`/alloc_mem_traces logs/
scp $SRC_SERVER_IP:`pwd`/avail_mem_traces logs/

sudo pkill -9 iokerneld
sudo pkill -9 server
sudo pkill -9 ctrl_main
ssh $SRC_SERVER_IP "sudo pkill -9 iokerneld"
ssh $SRC_SERVER_IP "sudo pkill -9 server"
ssh $DEST_SERVER_IP "sudo pkill -9 iokerneld"
ssh $DEST_SERVER_IP "sudo pkill -9 server"
ssh $CLIENT_IP "sudo pkill -9 iokerneld"

unset_bridge $CONTROLLER_ETHER
unset_bridge $CLIENT1_ETHER
ssh $SRC_SERVER_IP "cd `pwd`; source ../../shared.sh; unset_bridge $SERVER1_ETHER"
ssh $DEST_SERVER_IP "cd `pwd`; source ../../shared.sh; unset_bridge $SERVER2_ETHER"
