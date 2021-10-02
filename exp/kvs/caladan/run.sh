#!/bin/bash

source ../../shared.sh

mops=( 1 2 3 4 5 6 7 8 9 10 11 12 12.2 12.4 12.6 12.8 )

mkdir logs
rm -rf logs/*

for mop in ${mops[@]}
do
    sleep 5
    sudo $NU_DIR/caladan/iokerneld &
    ssh $SERVER2_IP "sudo $NU_DIR/caladan/iokerneld" &
    sleep 5
    ssh $SERVER2_IP "cd `pwd`; sudo ./server conf/server1" &
    sleep 5
    sed "s/constexpr double kTargetMops =.*/constexpr double kTargetMops = $mop;/g" \
	-i client.cpp
    make clean
    make -j
    sudo ./client conf/client1 1>logs/$mop 2>&1
    sudo pkill -9 iokerneld
    ssh $SERVER2_IP "sudo pkill -9 iokerneld"
    ssh $SERVER2_IP "sudo pkill -9 server"
done
