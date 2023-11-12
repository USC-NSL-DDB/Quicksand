#!/bin/bash

source ../../shared.sh

LPID=1
CTL_IDX=8
SRV_STARTING_IDX=1
MAX_NUM_SRVS=7
DIR=`pwd`

function get_srv_idx() {
    echo $(expr $SRV_STARTING_IDX + $1 - 1)
}

cd ../../../app/sort/quicksand/
sed "s/\(.*\)kUseNormalDistribution =.*/\1kUseNormalDistribution = true;/g" -i quicksand.cpp
make clean
make -j

for i in `seq 1 $MAX_NUM_SRVS`
do
    distribute quicksand $(get_srv_idx $i)
done

for num_srvs in `seq 1 $MAX_NUM_SRVS`
do
    start_iokerneld $CTL_IDX
    for i in `seq 1 $num_srvs`
    do
	start_iokerneld $(get_srv_idx $i)
    done
    sleep 5

    start_ctrl $CTL_IDX

    sleep 5

    for i in `seq 2 $num_srvs`
    do
	start_server quicksand $(get_srv_idx $i) $LPID 1>$DIR/logs/log.$num_srvs.srv.$i 2>&1 &	
    done
    sleep 5

    start_main_server quicksand $(get_srv_idx 1) $LPID 1>$DIR/logs/log.$num_srvs.main 2>&1
    cleanup

    sleep 30 # ensure that the kernel page prezeroing is finished
done
