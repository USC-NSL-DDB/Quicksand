#!/bin/bash

source ../shared.sh

LPID=1
CTL_IDX=2
SRV_IDX=1
DIR=`pwd`
KS=0
SPIN_KS=0
VIDEO_PREFIX="00"
APP_ARGS="-- ../input/workspace/$VIDEO_PREFIX bunny${VIDEO_PREFIX}_"

cd ../../app/excamera/
./scripts/setup.sh

start_iokerneld $CTL_IDX
start_iokerneld $SRV_IDX
sleep 5

start_ctrl $CTL_IDX
sleep 5

distribute input/workspace/$VIDEO_PREFIX $SRV_IDX
cd bin
start_main_server xc_single_batch $SRV_IDX $LPID $KS $SPIN_KS $APP_ARGS \
		  1>$DIR/logs/log 2>&1

cleanup
