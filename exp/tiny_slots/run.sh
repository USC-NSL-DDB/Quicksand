#!/bin/bash

source ../../shared.sh

mops=( 1 2 3 4 5 6 7 8 9 10 11 12 12.2 12.4 12.6 12.8 )

CTRL_IP=18.18.1.3
LPID=1

mkdir logs
rm -rf logs/*

set_bridge $CONTROLLER_ETHER
set_bridge $CLIENT1_ETHER

make clean
make -j
scp server $SERVER2_IP:`pwd`/

for mop in ${mops[@]}
do
    sed "s/constexpr double kTargetMops =.*/constexpr double kTargetMops = $mop;/g" \
	-i client.cpp
    make clean
    make -j    
    sleep 5
    sudo $NU_DIR/caladan/iokerneld &
    ssh $SERVER2_IP "sudo $NU_DIR/caladan/iokerneld" &
    sleep 5
    sudo $NU_DIR/bin/ctrl_main conf/controller &
    sleep 5
    ssh $SERVER2_IP "cd `pwd`; sudo ./server conf/server1 SRV $CTRL_IP $LPID" &
    sleep 5
    sudo ./server conf/client1 CLT $CTRL_IP $LPID >logs/.tmp &
    ( tail -f logs/.tmp & ) | grep -q "finish initing"
    sudo ./client conf/client2 1>logs/$mop 2>&1
    sudo pkill -9 iokerneld
    sudo pkill -9 server
    sudo pkill -9 ctrl_main
    ssh $SERVER2_IP "sudo pkill -9 iokerneld"
    ssh $SERVER2_IP "sudo pkill -9 server"
done

unset_bridge $CONTROLLER_ETHER
unset_bridge $CLIENT1_ETHER

rm logs/.tmp
