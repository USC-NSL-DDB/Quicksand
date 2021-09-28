#!/bin/bash

source ../../shared.sh

delays=( 100 1000 2000 3000 4000 5000 6000 7000 8000 9000 10000 )

mkdir logs
rm -rf logs/*

version=0.12.0 && git clone -b $version https://github.com/apache/thrift.git
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

sed "s/constexpr auto kIp = .*/constexpr auto kIp = \"$SERVER2_IP\";/g" -i client.cpp

for delay in ${delays[@]}
do
    sleep 5
    sed "s/constexpr uint32_t kDelayNs = .*/constexpr uint32_t kDelayNs = $delay;/g" -i server.cpp
    make
    scp server $SERVER2_IP:`pwd`
    ssh $SERVER2_IP "cd `pwd`; sudo ./server" &
    sleep 5
    sudo ./client 1>logs/$delay 2>&1 &
    sleep 10
    sudo pkill -9 client
    ssh $SERVER2_IP "sudo pkill -9 server"
done
