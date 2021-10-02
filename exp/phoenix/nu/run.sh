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
scp main $SERVER2_IP:`pwd`

for num_threads in `seq 1 46`
do
    sleep 5
    sudo $NU_DIR/caladan/iokerneld &
    ssh $SERVER2_IP "sudo $NU_DIR/caladan/iokerneld" &    
    sleep 5
    sed "s/runtime_kthreads.*/runtime_kthreads $num_threads/g" -i conf/server2
    scp conf/server2 $SERVER2_IP:`pwd`/conf
    sudo ./main conf/controller CTL 18.18.1.3 &
    sleep 5
    ssh $SERVER2_IP "cd `pwd`; sudo ./main conf/server2 SRV 18.18.1.3" &
    sleep 5
    sudo ./main conf/client1 CLT 18.18.1.3 4000 0 1>logs/$num_threads 2>&1
    sudo pkill -9 iokerneld
    sudo pkill -9 main
    ssh $SERVER2_IP "sudo pkill -9 iokerneld"
    ssh $SERVER2_IP "sudo pkill -9 main"    
done

unset_bridge $CONTROLLER_ETHER
unset_bridge $CLIENT1_ETHER
