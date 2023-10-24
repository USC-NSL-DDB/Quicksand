#!/bin/bash

source ../../shared.sh

if [ ! -f ../baseline/phoenix++-1.0/tests/matrix_multiply/matrix_file_A.txt ]; then
    echo "Please run after executing the baseline script."
fi

if [ ! -f ../baseline/phoenix++-1.0/tests/matrix_multiply/matrix_file_B.txt ]; then
    echo "Please run after executing the baseline script."
fi

mkdir logs
rm -rf logs/*

set_bridge $CONTROLLER_ETHER
set_bridge $CLIENT1_ETHER

DIR=`pwd`
CTRL_IP=18.18.1.3
LPID=1
NUM_WORKER_SERVERS=31
SRC_SERVER_IP=$SERVER2_IP
BACKUP_SERVER_IP=$SERVER32_IP

scp_pids=
for ip in ${REMOTE_SERVER_IPS[*]}
do
    scp ../baseline/phoenix++-1.0/tests/matrix_multiply/matrix_file_A.txt \
	$ip:`pwd`/../baseline/phoenix++-1.0/tests/matrix_multiply &
    scp_pids+=" $!"
    scp ../baseline/phoenix++-1.0/tests/matrix_multiply/matrix_file_B.txt \
	$ip:`pwd`/../baseline/phoenix++-1.0/tests/matrix_multiply &
    scp_pids+=" $!"
done
wait $scp_pids

cd $NU_DIR/app/phoenix++-1.0/
make -j
cd tests/matrix_multiply/
mv matrix_multiply.cpp matrix_multiply.cpp.bak
cp $DIR/matrix_multiply.cpp .
sed "s/constexpr uint32_t kNumWorkerNodes.*/constexpr uint32_t kNumWorkerNodes = $NUM_WORKER_SERVERS;/g" \
    -i matrix_multiply.cpp
make clean
make -j
cp matrix_multiply $DIR/main

mv matrix_multiply.cpp.bak matrix_multiply.cpp
make clean

cd $DIR
for ip in ${REMOTE_SERVER_IPS[*]}
do
    scp main $ip:`pwd`
done

sudo $NU_DIR/caladan/iokerneld &
for i in `seq 1 $((NUM_WORKER_SERVERS-1))`
do
    ip=${REMOTE_SERVER_IPS[`expr $i - 1`]}
    ssh $ip "sudo $NU_DIR/caladan/iokerneld" &
done
sleep 5

sudo stdbuf -o0 $NU_DIR/bin/ctrl_main conf/controller >logs/controller &
sleep 5

for i in `seq 1 $((NUM_WORKER_SERVERS-1))`
do
    ip=${REMOTE_SERVER_IPS[`expr $i - 1`]}
    conf=conf/server$i
    ssh $ip "cd `pwd`; sudo stdbuf -o0 ./main $conf SRV $CTRL_IP $LPID" 1>logs/server.$i 2>&1 &
done
sleep 5

ssh $SRC_SERVER_IP "cd `pwd`; sudo ../../../bin/bench_real_cpu_pressure conf/client0" &
sleep 5

sudo stdbuf -o0 ./main conf/client1 CLT $CTRL_IP $LPID -- 4200 200000 200000 4200 1>logs/$NUM_WORKER_SERVERS 2>&1 &
client_pid=$!
( tail -f logs/$NUM_WORKER_SERVERS & ) | grep -q "waiting for signal"

ssh $BACKUP_SERVER_IP "sudo $NU_DIR/caladan/iokerneld" &
sleep 5
ssh $BACKUP_SERVER_IP "cd `pwd`; sudo stdbuf -o0 ./main conf/server$NUM_WORKER_SERVERS SRV $CTRL_IP $LPID" \
    1>logs/server.$NUM_WORKER_SERVERS 2>&1 &
sleep 5

ssh $SRC_SERVER_IP "sleep 9; sudo pkill -SIGHUP bench" &
sudo pkill -x -SIGHUP main
wait $client_pid

for i in `seq 1 $NUM_WORKER_SERVERS`
do
    ip=${REMOTE_SERVER_IPS[`expr $i - 1`]}
    ssh $ip "sudo pkill -9 iokerneld; sudo pkill -9 main; sudo pkill -9 bench"
done
sudo pkill -9 iokerneld
sudo pkill -9 main

unset_bridge $CONTROLLER_ETHER
unset_bridge $CLIENT1_ETHER
