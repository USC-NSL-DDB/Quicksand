#!/bin/bash

source ../../shared.sh

delays=( 100 1000 2000 3000 4000 5000 6000 7000 8000 9000 10000 )

mkdir logs
rm -rf logs/*

cp -r ../../../app/socialNetwork/thrift/ .
cd thrift
./bootstrap.sh
./configure --enable-caladanthreads=yes --enable-caladantcp=yes \
            --with-caladan=$NU_DIR/caladan  \
            --enable-shared=no --enable-tests=no --enable-tutorial=no \
	    --with-libevent=no
make -j
cd ..

make clean
make -j

for delay in ${delays[@]}
do
    sleep 5
    sudo $NU_DIR/caladan/iokerneld &
    ssh $SERVER2_IP "sudo $NU_DIR/caladan/iokerneld" &    
    sleep 5
    sed "s/constexpr uint32_t kDelayNs = .*/constexpr uint32_t kDelayNs = $delay;/g" -i server.cpp
    make
    scp server $SERVER2_IP:`pwd`
    ssh $SERVER2_IP "cd `pwd`; sudo ./server conf/server2 SRV 18.18.1.3" &
    sleep 5
    sudo ./client conf/client1 CLT 18.18.1.3 1>logs/$delay 2>&1 &
    sleep 10
    sudo pkill -9 iokerneld
    sudo pkill -9 client
    ssh $SERVER2_IP "sudo pkill -9 iokerneld"
    ssh $SERVER2_IP "sudo pkill -9 server"
done
