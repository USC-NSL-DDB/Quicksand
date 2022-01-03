#!/bin/bash

source ../../shared.sh

set_bridge $CONTROLLER_ETHER
set_bridge $CLIENT1_ETHER

SRC_SERVER_IP=$SERVER3_IP
DEST_SERVER_IP=$SERVER4_IP

DIR=`pwd`
CTRL_IP=18.18.1.3
LPID=1

cd $NU_DIR/app/phoenix++-1.0/
cp Defines.mk Defines.mk.bak
sed "s/CFLAGS = \(.*\)/CFLAGS = -DBSP -DBSP_PRINT_STAT \1/g" -i Defines.mk
make clean
make -j

cd tests/matrix_multiply
cp matrix_multiply.cpp matrix_multiply.cpp.bak
cp $DIR/matrix_multiply.cpp .
make clean
make -j
cp matrix_multiply $DIR/main

mv matrix_multiply.cpp.bak matrix_multiply.cpp
make clean
cd ../../
mv Defines.mk.bak Defines.mk
make clean

cd $DIR
sudo $NU_DIR/caladan/iokerneld &
sleep 5
sudo $NU_DIR/bin/ctrl_main conf/controller $CTRL_IP &

sleep 5
scp main $SRC_SERVER_IP:`pwd`
ssh $SRC_SERVER_IP "cd $DIR; source ../../shared.sh; set_bridge $SERVER1_ETHER"
ssh $SRC_SERVER_IP "sudo $NU_DIR/caladan/iokerneld" &
sleep 5
ssh $SRC_SERVER_IP "cd `pwd`; sudo ../../../bin/bench_real_cpu_pressure conf/client2" &
sleep 5
ssh $SRC_SERVER_IP "cd `pwd`; sudo stdbuf -o0 ./main conf/server1 SRV $CTRL_IP $LPID" &
sleep 5
sudo ./main conf/client1 CLT $CTRL_IP $LPID -- 10000 &

pid_client=$!
sleep 5
scp main $DEST_SERVER_IP:`pwd`
ssh $DEST_SERVER_IP "cd $DIR; source ../../shared.sh; set_bridge $SERVER2_ETHER"
ssh $DEST_SERVER_IP "sudo $NU_DIR/caladan/iokerneld" &
sleep 5
ssh $DEST_SERVER_IP "cd `pwd`; sudo ./main conf/server2 SRV $CTRL_IP $LPID" &
sleep 5
ssh $SRC_SERVER_IP "sudo taskset -c 0 bash -c 'sleep 0.4; pkill -SIGHUP bench'" &
sleep 0.1
sudo pkill -x -SIGHUP main
wait $pid_client

sudo pkill -9 iokerneld; sudo pkill -9 main
ssh $SRC_SERVER_IP "sudo pkill -9 iokerneld; sudo pkill -9 main; sudo pkill -9 bench;"
ssh $DEST_SERVER_IP "sudo pkill -9 iokerneld; sudo pkill -9 main;"

unset_bridge $CONTROLLER_ETHER
unset_bridge $CLIENT1_ETHER
ssh $SRC_SERVER_IP "cd $DIR; source ../../shared.sh; unset_bridge $SERVER1_ETHER"
ssh $DEST_SERVER_IP "cd $DIR; source ../../shared.sh; unset_bridge $SERVER2_ETHER"
