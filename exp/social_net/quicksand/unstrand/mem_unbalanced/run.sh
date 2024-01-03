#!/bin/bash

source ../../../../shared.sh

LPID=1
CTL_IDX=8
SRV_STARTING_IDX=1
NUM_SRVS=6
MAIN_SRV_IDX=$SRV_STARTING_IDX
CLIENT_IDX=$(expr $SRV_STARTING_IDX + $NUM_SRVS)
MOPS=0.9
DIR=`pwd`
SOCIAL_NET_DIR=$DIR/../../../../../app/socialNetwork/quicksand/
KS_FIRST_HALF=13
KS_LATTER_HALF=13
APP_MEM_FIRST_HALF=1024
APP_MEM_LATTER_HALF=11264
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
kses=()
for i in `seq 1 $NUM_SRVS`
do
    srv_idx=$(get_srv_idx $i)    
    if [ $i -le $(expr $NUM_SRVS / 2) ]
    then
	ks=$KS_FIRST_HALF
    else
	ks=$KS_LATTER_HALF
    fi
    kses+=( $ks )
    cores=$(run_cmd $srv_idx \
	    "lscpu -p  |
             awk -F ',' '{if (\$4 == 0) print \$1, \$2}' |
             sort -k 2 -n |
             head -n $(expr $ks + 2) |
             awk '{print \$1}' |
             xargs | tr ' ' ','")
    start_iokerneld $srv_idx ias $cores
    run_cmd $(get_srv_idx $i) "mkdir -p `pwd`/build/src"
    distribute build/src/main $srv_idx
done
start_iokerneld $CLIENT_IDX
distribute build/src/main $CLIENT_IDX
sleep 5

start_ctrl $CTL_IDX
sleep 5

for i in `seq 1 $NUM_SRVS`
do
    srv_idx=$(get_srv_idx $i)
    ks=${kses[`expr $i - 1`]}
    if [ $srv_idx -ne $MAIN_SRV_IDX ]
    then
	start_server build/src/main $(get_srv_idx $i) $LPID $ks >$DIR/logs/srv.$i 2>&1 &
    else
	main_ks=$ks
    fi
done
start_server_isol_with_ip build/src/main $CLIENT_IDX $LPID 0 0 18.18.1.100 >$DIR/logs/client 2>&1 &
sleep 5

mem_antagonist=$NU_DIR/bin/bench_real_mem_pressure
mem_target_first_half=$(expr $APP_MEM_FIRST_HALF + $NU_LOW_MEM)
mem_target_latter_half=$(expr $APP_MEM_LATTER_HALF + $NU_LOW_MEM)
for i in `seq 1 $NUM_SRVS`
do
    srv_idx=$(get_srv_idx $i)    
    if [ $i -le $(expr $NUM_SRVS / 2) ]
    then
	mem_target=$mem_target_first_half
    else
	mem_target=$mem_target_latter_half	
    fi    
    run_program $mem_antagonist $srv_idx $DIR/antagonist$i.conf $mem_target 0 >$DIR/logs/antagonist.$i &    
done

for i in `seq 1 $NUM_SRVS`
do
    ( tail -f $DIR/logs/antagonist.$i & ) | grep -q "waiting for signal"
done
sleep 5

MAIN_LOG=$DIR/logs/srv.$MAIN_SRV_IDX
start_main_server build/src/main $MAIN_SRV_IDX $LPID $main_ks >$MAIN_LOG 2>&1

cleanup
sleep 5
