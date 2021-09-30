#!/bin/bash

DIR=`pwd`
mkdir logs
rm logs/*

cd ../phoenix++-1.0
make clean
make -j
cd tests/matrix_multiply

./matrix_multiply 4000

for num_threads in `seq 1 46`
do
    export MR_NUMTHREADS=$num_threads
    ./matrix_multiply 4000 0 1>$DIR/logs/$num_threads 2>&1
done
