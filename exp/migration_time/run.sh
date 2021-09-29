#!/bin/bash

source ../shared.sh

heap_sizes=( 65536 131072 262144 524288 1048576 2097152 4194304 8388608 16777216 )

mkdir logs
rm -rf logs/*

set_bridge $CONTROLLER_ETHER
set_bridge $CLIENT1_ETHER
set_bridge $SERVER1_ETHER

for heap_size in ${heap_sizes[@]}
do
    sleep 5
    sudo $NU_DIR/caladan/iokerneld &
    ssh $SERVER2_IP "sudo $NU_DIR/caladan/iokerneld" &    
    sleep 5
    sed "s/constexpr uint32_t kObjSize = .*/constexpr uint32_t kObjSize = $heap_size;/g" -i main.cpp
    make
    scp main $SERVER2_IP:`pwd`
    sudo ./main conf/controller CTL 18.18.1.3 &
    sleep 5
    sudo ./main conf/server1 SRV 18.18.1.3 1>logs/$heap_size 2>&1 &
    ssh $SERVER2_IP "cd `pwd`; sudo ./main conf/server2 SRV 18.18.1.3" &
    sleep 5
    sudo ./main conf/client1 CLT 18.18.1.3 &
    sleep 10
    sudo pkill -9 iokerneld
    sudo pkill -9 main
    ssh $SERVER2_IP "sudo pkill -9 iokerneld"
    ssh $SERVER2_IP "sudo pkill -9 main"    
done

unset_bridge $CONTROLLER_ETHER
unset_bridge $CLIENT1_ETHER
unset_bridge $SERVER1_ETHER
