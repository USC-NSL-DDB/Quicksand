#!/bin/bash

source ../../../shared.sh

LPID=1
CTL_IDX=8
SRV_STARTING_IDX=1
NUM_SRVS=6
MAIN_SRV_IDX=$(expr $SRV_STARTING_IDX + $NUM_SRVS - 1)
GPU_IDX=$(expr $MAIN_SRV_IDX + 1)
KS=0
SPIN_KS=0
DIR=`pwd`

function get_srv_idx() {
    echo $(expr $SRV_STARTING_IDX + $1 - 1)
}

source ../../setup_opencv.sh
cp dataloader.hpp dataloader.hpp.bak
sed "s/\(.*\)kNumScaleDownGPUs = .*/\1kNumScaleDownGPUs = 5;/g" -i dataloader.hpp
cp $NU_DIR/inc/nu/impl/dis_executor.ipp $NU_DIR/inc/nu/impl/dis_executor.ipp.bak
sed "s/\(.*\)kEnableLogging = .*/\1kEnableLogging = true;/g" \
    -i $NU_DIR/inc/nu/impl/dis_executor.ipp
make clean
make -j
mv dataloader.hpp.bak dataloader.hpp
mv $NU_DIR/inc/nu/impl/dis_executor.ipp.bak $NU_DIR/inc/nu/impl/dis_executor.ipp

run_cmd $MAIN_SRV_IDX $DIR/../../setup_images.sh

start_iokerneld $CTL_IDX
for i in `seq 1 $NUM_SRVS`
do
    start_iokerneld $(get_srv_idx $i)
    distribute distributed $(get_srv_idx $i)
done
start_iokerneld $GPU_IDX
distribute distributed $GPU_IDX

sleep 5

start_ctrl $CTL_IDX

sleep 5

LD_LIBRARY_PATH=`pwd`/opencv/install/lib \
	        start_server_isol_with_ip distributed $GPU_IDX $LPID $KS \
	        $SPIN_KS 18.18.1.100 >$DIR/logs/gpu 2>&1 &
for i in `seq 1 $NUM_SRVS`
do
    srv_idx=$(get_srv_idx $i)
    if [ $srv_idx -ne $MAIN_SRV_IDX ]
    then
	LD_LIBRARY_PATH=`pwd`/opencv/install/lib \
		       start_server distributed $(get_srv_idx $i) $LPID >$DIR/logs/srv.$i 2>&1 &
    fi
done

sleep 5

LD_LIBRARY_PATH=`pwd`/opencv/install/lib \
	       start_main_server distributed $MAIN_SRV_IDX $LPID >$DIR/logs/srv.main 2>&1
