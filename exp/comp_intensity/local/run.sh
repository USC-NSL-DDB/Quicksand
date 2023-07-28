#!/bin/bash

source ../../shared.sh

ELEM_SIZES=( 100 1000 10000 )
DELAYS=( 0 1 2 3 4 5 6 7 8 9 10 20 30 40 50 )
LPID=1
CTL_IDX=1
SRV_IDX=2
KS=26

make clean

for elem_size in ${ELEM_SIZES[@]}
do
    for delay in ${DELAYS[@]}
    do
	sed "s/\(constexpr uint64_t kElementSize = \).*/\1$elem_size;/g" -i main.cpp
	sed "s/\(constexpr uint64_t kDelayUs = \).*/\1$delay;/g" -i main.cpp
	make

	distribute main $SRV_IDX

	start_iokerneld $CTL_IDX
	start_iokerneld $SRV_IDX
	sleep 5

	start_ctrl $CTL_IDX
	sleep 5

	start_main_server main $SRV_IDX $LPID $KS 1>logs/$elem_size.$delay 2>&1

	cleanup
	sleep 5
    done
done
