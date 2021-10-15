#!/bin/bash

source ../../shared.sh

MEMCACHED_IP=$SERVER2_IP
MEMCACHED_100G_IP=$(to_100g_addr $MEMCACHED_IP)
DIR=`pwd`
rm -rf log

ssh $MEMCACHED_IP "sudo service irqbalance stop"
ssh $MEMCACHED_IP "cd `pwd`/../../../caladan/scripts/; sudo ./set_irq_affinity 0-47 mlx5"

SET_NIC_CMD="sudo ifconfig $DPDK_NIC down;"
SET_NIC_CMD+="sudo ethtool -C $DPDK_NIC adaptive-rx off;"
SET_NIC_CMD+="sudo ethtool -C $DPDK_NIC adaptive-tx off;"
SET_NIC_CMD+="sudo ethtool -C $DPDK_NIC rx-usecs 0;"
SET_NIC_CMD+="sudo ethtool -C $DPDK_NIC rx-frames 0;"
SET_NIC_CMD+="sudo ethtool -C $DPDK_NIC tx-usecs 0;"
SET_NIC_CMD+="sudo ethtool -C $DPDK_NIC tx-frames 0;"
SET_NIC_CMD+="sudo ethtool -N $DPDK_NIC rx-flow-hash udp4 sdfn;"
SET_NIC_CMD+="sudo sysctl net.ipv4.tcp_syncookies=1;"
SET_NIC_CMD+="sudo ifconfig $DPDK_NIC up;"

ssh $MEMCACHED_IP "$SET_NIC_CMD"
ssh $MEMCACHED_IP "sudo apt-get install -y libevent-dev"
ssh $MEMCACHED_IP "cd `pwd`; wget http://www.memcached.org/files/memcached-1.6.12.tar.gz"
ssh $MEMCACHED_IP "cd `pwd`; tar xvf memcached-1.6.12.tar.gz"
ssh $MEMCACHED_IP "cd `pwd`/memcached-1.6.12; ./configure; make -j"
ssh $MEMCACHED_IP "cd `pwd`/memcached-1.6.12; \
                   sudo nice -n -20 ./memcached -u zainruan -t 48 -U 8888 -p 8888 \
                                                -c 32768 -m 32000 -b 32768 \
                                                -o hashpower=28,no_hashexpand,lru_crawler,lru_maintainer,idle_timeout=0" &
sudo $NU_DIR/caladan/iokerneld &
sleep 5
cd ../../../caladan/apps/synthetic/
curl https://sh.rustup.rs -sSf | sh -s -- -y
source $HOME/.cargo/env
rustup default nightly
sudo apt-get install -y clang
cargo clean
cargo build --release
sudo target/release/synthetic $MEMCACHED_100G_IP:8888 --config $DIR/conf/client --mode runtime-client \
     --protocol memcached --transport tcp --mpps 6 --samples 20 --threads 48 --start_mpps 0.2 \
     --runtime=5 --nvalues=107374182 --key_size 20 --value_size 2 >$DIR/log
sudo pkill -9 iokerneld
ssh $MEMCACHED_IP "sudo pkill -9 memcached"
