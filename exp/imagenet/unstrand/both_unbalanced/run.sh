#!/bin/bash

source ../../../shared.sh

LPID=1
CTL_IDX=8
SRV_STARTING_IDX=1
NUM_SRVS=6
MAIN_SRV_IDX=$(expr $SRV_STARTING_IDX + $NUM_SRVS - 1)
GPU_IDX=$(expr $MAIN_SRV_IDX + 1)
KS_FIRST_HALF=6
KS_LATTER_HALF=20
GPU_KS=0
GPU_SPIN_KS=0
DIR=`pwd`
SRV1_APP_MEM=1024
NU_LOW_MEM=1024

function get_srv_idx() {
    echo $(expr $SRV_STARTING_IDX + $1 - 1)
}

source ../../setup_opencv.sh
cp dataloader.hpp dataloader.hpp.bak
sed "s/\(.*\)kNumScaleDownGPUs = .*/\1kNumScaleDownGPUs = 10;/g" -i dataloader.hpp
make clean
make -j
mv dataloader.hpp.bak dataloader.hpp

run_cmd $MAIN_SRV_IDX $DIR/../../setup_images.sh

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
    distribute distributed $srv_idx
done
start_iokerneld $GPU_IDX
distribute distributed $GPU_IDX

sleep 5

start_ctrl $CTL_IDX

sleep 5

LD_LIBRARY_PATH=`pwd`/opencv/install/lib \
	        start_server_isol_with_ip distributed $GPU_IDX $LPID $GPU_KS \
	        $GPU_SPIN_KS 18.18.1.100 >$DIR/logs/gpu 2>&1 &
for i in `seq 1 $NUM_SRVS`
do
    srv_idx=$(get_srv_idx $i)
    ks=${kses[`expr $i - 1`]}
    if [ $srv_idx -ne $MAIN_SRV_IDX ]
    then
	LD_LIBRARY_PATH=`pwd`/opencv/install/lib \
		       start_server distributed $(get_srv_idx $i) $LPID $ks >$DIR/logs/srv.$i 2>&1 &
    fi
done

sleep 5

mem_antagonist=$NU_DIR/bin/bench_real_mem_pressure
mem_target=$(expr $SRV1_APP_MEM + $NU_LOW_MEM)
for i in `seq 1 $(expr $NUM_SRVS / 2)`
do
    srv_idx=$(get_srv_idx $i)
    run_program $mem_antagonist $srv_idx $DIR/antagonist$i.conf $mem_target 0 >$DIR/logs/antagonist.$i &
done

for i in `seq 1 $(expr $NUM_SRVS / 2)`
do
    ( tail -f $DIR/logs/antagonist.$i & ) | grep -q "waiting for signal"
done

sleep 5

ks=${kses[`expr $MAIN_SRV_IDX - 1`]}
LD_LIBRARY_PATH=`pwd`/opencv/install/lib \
	       start_main_server distributed $MAIN_SRV_IDX $LPID $ks >$DIR/logs/srv.main 2>&1
