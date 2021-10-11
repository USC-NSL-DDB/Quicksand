#!/bin/bash

source ../../shared.sh

set_bridge $CONTROLLER_ETHER
set_bridge $CLIENT1_ETHER

SRC_SERVER_IP=$SERVER3_IP
DEST_SERVER_IP=$SERVER4_IP

DIR=`pwd`
cd ../../../
cp inc/nu/monitor.hpp inc/nu/monitor.hpp.bak
sed "s/constexpr static bool kMonitorCPUCongestion.*/constexpr static bool kMonitorCPUCongestion = true;/g" \
    -i inc/nu/monitor.hpp
make clean
make -j

cd $DIR
cd ../../../app/phoenix++-1.0/
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

cd $NU_DIR
mv inc/nu/monitor.hpp.bak inc/nu/monitor.hpp
make clean
make -j

cd $DIR
sudo $NU_DIR/caladan/iokerneld &
sleep 5
sudo ./main conf/controller CTL 18.18.1.3 &
sleep 5
scp main $SRC_SERVER_IP:`pwd`
ssh $SRC_SERVER_IP "cd $DIR; source ../../shared.sh; set_bridge $SERVER1_ETHER"
ssh $SRC_SERVER_IP "sudo $NU_DIR/caladan/iokerneld" &
sleep 5
ssh $SRC_SERVER_IP "cd `pwd`; sudo ../../../bin/bench_real_cpu_pressure conf/client2" &
sleep 5
ssh $SRC_SERVER_IP "cd `pwd`; sudo stdbuf -o0 ./main conf/server1 SRV 18.18.1.3" &
sleep 5
sudo ./main conf/client1 CLT 18.18.1.3 4000 &
pid_client=$!
sleep 5
scp main $DEST_SERVER_IP:`pwd`
ssh $DEST_SERVER_IP "cd $DIR; source ../../shared.sh; set_bridge $SERVER2_ETHER"
ssh $DEST_SERVER_IP "sudo $NU_DIR/caladan/iokerneld" &
sleep 5
ssh $DEST_SERVER_IP "cd `pwd`; sudo ./main conf/server2 SRV 18.18.1.3" &
sleep 5
ssh $SRC_SERVER_IP "sudo taskset -c 0 bash -c 'sleep 0.4; pkill -SIGHUP bench'" &
sleep 0.1
sudo pkill -SIGHUP main
wait $pid_client

sudo pkill -9 iokerneld; sudo pkill -9 main
ssh $SRC_SERVER_IP "sudo pkill -9 iokerneld; sudo pkill -9 main; sudo pkill -9 bench;"
ssh $DEST_SERVER_IP "sudo pkill -9 iokerneld; sudo pkill -9 main;"

unset_bridge $CONTROLLER_ETHER
unset_bridge $CLIENT1_ETHER
ssh $SRC_SERVER_IP "cd $DIR; source ../../shared.sh; unset_bridge $SERVER1_ETHER"
ssh $DEST_SERVER_IP "cd $DIR; source ../../shared.sh; unset_bridge $SERVER2_ETHER"
