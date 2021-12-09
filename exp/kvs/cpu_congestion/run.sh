#!/bin/bash

source ../../shared.sh

rm -rf logs
mkdir logs

set_bridge $CONTROLLER_ETHER
set_bridge $CLIENT1_ETHER

SRC_SERVER_IP=$SERVER3_IP
DEST_SERVER_IP=$SERVER4_IP

DIR=`pwd`

cd $DIR
make clean
make -j

scp server $SRC_SERVER_IP:`pwd`
scp server $DEST_SERVER_IP:`pwd`

sudo $NU_DIR/caladan/iokerneld &
sleep 5
sudo ./server conf/controller CTL 18.18.1.3 &
ssh $SRC_SERVER_IP "cd `pwd`; source ../../shared.sh; set_bridge $SERVER1_ETHER"
ssh $SRC_SERVER_IP "sudo $NU_DIR/caladan/iokerneld" &
sleep 5
ssh $SRC_SERVER_IP "cd `pwd`; sudo ./server conf/server1 SRV 18.18.1.3" &
sleep 5
sudo ./server conf/client1 CLT 18.18.1.3 >logs/.server &
( tail -f -n0 logs/.server & ) | grep -q "waiting for signal"
ssh $DEST_SERVER_IP "cd `pwd`; source ../../shared.sh; set_bridge $SERVER2_ETHER"
ssh $DEST_SERVER_IP "sudo $NU_DIR/caladan/iokerneld" &
sleep 5
ssh $DEST_SERVER_IP "cd `pwd`; sudo ./server conf/server2 SRV 18.18.1.3" &
sleep 5
sudo pkill -SIGHUP server
ssh $SRC_SERVER_IP "cd `pwd`; sudo stdbuf -o0 ../../../bin/bench_real_cpu_pressure conf/client3" >logs/.pressure &
( tail -f -n0 logs/.pressure & ) | grep -q "waiting for signal"
sudo ./client conf/client2 >logs/client &
( tail -f -n0 logs/client & ) | grep -q "Start benchmarking"
sleep 1
ssh $SRC_SERVER_IP "sudo pkill -SIGHUP bench_real_cpu"
sleep 5

sudo pkill -9 client
sudo pkill -9 iokerneld
sudo pkill -9 server
ssh $SRC_SERVER_IP "sudo pkill -9 iokerneld"
ssh $SRC_SERVER_IP "sudo pkill -9 server"
ssh $SRC_SERVER_IP "sudo pkill -9 bench"
ssh $DEST_SERVER_IP "sudo pkill -9 iokerneld"
ssh $DEST_SERVER_IP "sudo pkill -9 server"

unset_bridge $CONTROLLER_ETHER
unset_bridge $CLIENT1_ETHER
ssh $SRC_SERVER_IP "cd `pwd`; source ../../shared.sh; unset_bridge $SERVER1_ETHER"
ssh $DEST_SERVER_IP "cd `pwd`; source ../../shared.sh; unset_bridge $SERVER2_ETHER"

