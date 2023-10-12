#!/bin/bash

source ../../shared.sh

ELEM_SIZES=( 100 1000 10000 )
DELAYS=( 0 1 2 3 4 5 6 7 8 9 10 20 30 40 50 60 70 80 90 100 )
LPID=1
CTL_IDX=1
SRV0_IDX=2
SRV1_IDX=3
KS0=6
KS1=20
SRV1_APP_MEM=1024
NU_LOW_MEM=1024

make clean

for elem_size in ${ELEM_SIZES[@]}
do
    for delay in ${DELAYS[@]}
    do
	sed "s/\(constexpr uint64_t kElementSize = \).*/\1$elem_size;/g" -i main.cpp
	sed "s/\(constexpr uint64_t kDelayUs = \).*/\1$delay;/g" -i main.cpp
	make

	distribute main $SRV0_IDX
	distribute main $SRV1_IDX
	distribute main $SRV1_IDX
	mem_antagonist=$NU_DIR/bin/bench_real_mem_pressure
	distribute $mem_antagonist $SRV1_IDX

	start_iokerneld $CTL_IDX
	start_iokerneld $SRV0_IDX
	start_iokerneld $SRV1_IDX
	sleep 5

	start_ctrl $CTL_IDX
	sleep 5

	start_server main $SRV0_IDX $LPID $KS0 1>logs/$elem_size.$delay.0 2>&1 &
	sleep 5

	mem_target=$(expr $SRV1_APP_MEM + $NU_LOW_MEM)
	antagonist_log=logs/antagonist
	run_program $mem_antagonist $SRV1_IDX antagonist.conf $mem_target 0 >$antagonist_log &
	( tail -f -n0 $antagonist_log & ) | grep -q "waiting for signal"
	start_main_server main $SRV1_IDX $LPID $KS1 1>logs/$elem_size.$delay.1 2>&1

	cleanup
	sleep 5
    done
done
