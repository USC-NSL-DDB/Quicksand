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
cd ../../../app/phoenix++-1.0/
make clean
make -j
cd tests/matrix_multiply/
make clean
make -j
cp matrix_multiply $DIR/main
cd $DIR
for ip in ${REMOTE_SERVER_IPS[*]}
do    
    scp main $ip:`pwd`
done

for num_worker_servers in `seq 1 4`
do
    sleep 5
    sudo $NU_DIR/caladan/iokerneld &
    for i in `seq 1 $num_worker_servers`
    do
	ip=${REMOTE_SERVER_IPS[`expr $i - 1`]}
	ssh $ip "sudo $NU_DIR/caladan/iokerneld" &
    done
    sleep 5
    sudo ./main conf/controller CTL 18.18.1.3 &
    sleep 5
    for i in `seq 1 $num_worker_servers`
    do
	ip=${REMOTE_SERVER_IPS[`expr $i - 1`]}
	conf=conf/server`expr $i + 1`
	ssh $ip "cd `pwd`; sudo ./main $conf SRV 18.18.1.3" &
    done
    sleep 5
    sudo ./main conf/client1 CLT 18.18.1.3 4000 0 1>logs/$num_worker_servers 2>&1
    sudo pkill -9 iokerneld
    sudo pkill -9 main
    for i in `seq 1 $num_worker_servers`
    do
	ip=${REMOTE_SERVER_IPS[`expr $i - 1`]}
	ssh $ip "sudo pkill -9 iokerneld"	
	ssh $ip "sudo pkill -9 main"	    
    done
done

unset_bridge $CONTROLLER_ETHER
unset_bridge $CLIENT1_ETHER

