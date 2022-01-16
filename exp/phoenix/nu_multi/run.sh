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

for ip in ${REMOTE_SERVER_IPS[*]}
do
    scp ../baseline/phoenix++-1.0/tests/matrix_multiply/matrix_file_A.txt \
	$ip:`pwd`/../baseline/phoenix++-1.0/tests/matrix_multiply
    scp ../baseline/phoenix++-1.0/tests/matrix_multiply/matrix_file_B.txt \
	$ip:`pwd`/../baseline/phoenix++-1.0/tests/matrix_multiply
done

MAT_MUL_SRC_DIR=$NU_DIR/app/phoenix++-1.0/tests/matrix_multiply
mv $MAT_MUL_SRC_DIR/matrix_multiply.cpp $MAT_MUL_SRC_DIR/matrix_multiply.cpp.bak
cp matrix_multiply.cpp $MAT_MUL_SRC_DIR/matrix_multiply.cpp

for num_worker_servers in `seq 1 30`
do
    cd $NU_DIR/app/phoenix++-1.0/
    make clean
    make -j
    cd tests/matrix_multiply/
    sed "s/constexpr uint32_t kNumWorkerNodes.*/constexpr uint32_t kNumWorkerNodes = $num_worker_servers;/g" \
	-i matrix_multiply.cpp
    make clean
    make -j
    cp matrix_multiply $DIR/main
    cd $DIR
    for ip in ${REMOTE_SERVER_IPS[*]}
    do
	scp main $ip:`pwd`
    done
    sleep 5
    sudo $NU_DIR/caladan/iokerneld &
    for i in `seq 1 $num_worker_servers`
    do
	ip=${REMOTE_SERVER_IPS[`expr $i - 1`]}
	ssh $ip "sudo $NU_DIR/caladan/iokerneld" &
    done
    sleep 5
    sudo $NU_DIR/bin/ctrl_main conf/controller &
    sleep 5
    for i in `seq 1 $num_worker_servers`
    do
	ip=${REMOTE_SERVER_IPS[`expr $i - 1`]}
	conf=conf/server$i
	ssh $ip "cd `pwd`; sudo ./main $conf SRV $CTRL_IP $LPID" &
    done
    sleep 5
    sudo ./main conf/client1 CLT $CTRL_IP $LPID -- 10000 0 1>logs/$num_worker_servers 2>&1
    sudo pkill -9 iokerneld
    sudo pkill -9 main
    for i in `seq 1 $num_worker_servers`
    do
	ip=${REMOTE_SERVER_IPS[`expr $i - 1`]}
	ssh $ip "sudo pkill -9 iokerneld"	
	ssh $ip "sudo pkill -9 main"	    
    done
done

mv $MAT_MUL_SRC_DIR/matrix_multiply.cpp.bak $MAT_MUL_SRC_DIR/matrix_multiply.cpp

unset_bridge $CONTROLLER_ETHER
unset_bridge $CLIENT1_ETHER
