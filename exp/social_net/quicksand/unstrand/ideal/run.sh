#!/bin/bash

source ../../../../shared.sh

LPID=1
CTL_IDX=8
SRV_STARTING_IDX=1
NUM_SRVS=3
MAIN_SRV_IDX=$SRV_STARTING_IDX
CLIENT_IDX=$(expr $SRV_STARTING_IDX + $NUM_SRVS)
MOPS=0.9
DIR=`pwd`
SOCIAL_NET_DIR=$DIR/../../../../../app/socialNetwork/quicksand/
APP_MEM=12288
NU_LOW_MEM=1024

function get_srv_idx() {
    echo $(expr $SRV_STARTING_IDX + $1 - 1)
}

cd $SOCIAL_NET_DIR
sed "s/constexpr static double kTargetMops.*/constexpr static double kTargetMops = $MOPS;/g" -i src/client.hpp
mkdir -p build
cd build
make clean
make -j
cd ..

start_iokerneld $CTL_IDX
for i in `seq 1 $NUM_SRVS`
do
    start_iokerneld $(get_srv_idx $i)
    run_cmd $(get_srv_idx $i) "mkdir -p `pwd`/build/src"
    distribute build/src/main $(get_srv_idx $i)
done
start_iokerneld $CLIENT_IDX
distribute build/src/main $CLIENT_IDX
sleep 5

start_ctrl $CTL_IDX
sleep 5

for i in `seq 1 $NUM_SRVS`
do
    srv_idx=$(get_srv_idx $i)
    if [ $srv_idx -ne $MAIN_SRV_IDX ]
    then
	start_server build/src/main $(get_srv_idx $i) $LPID >$DIR/logs/srv.$i 2>&1 &
    fi
done
start_server_isol_with_ip build/src/main $CLIENT_IDX $LPID 0 0 18.18.1.100 >$DIR/logs/client 2>&1 &
sleep 5

mem_antagonist=$NU_DIR/bin/bench_real_mem_pressure
mem_target=$(expr $APP_MEM + $NU_LOW_MEM)
for i in `seq 1 $NUM_SRVS`
do
    srv_idx=$(get_srv_idx $i)    
    run_program $mem_antagonist $srv_idx $DIR/antagonist$i.conf $mem_target 0 >$DIR/logs/antagonist.$i &    
done

for i in `seq 1 $NUM_SRVS`
do
    ( tail -f $DIR/logs/antagonist.$i & ) | grep -q "waiting for signal"
done
sleep 5

MAIN_LOG=$DIR/logs/srv.$MAIN_SRV_IDX
start_main_server build/src/main $MAIN_SRV_IDX $LPID >$MAIN_LOG 2>&1

cleanup
sleep 5
