#!/bin/bash

DIR=`pwd`
mkdir logs
rm logs/*

cd ../phoenix++-1.0
make clean
make -j
cd tests/kmeans

for num_threads in `seq 1 30`
do
    export MR_NUMTHREADS=$num_threads
    ./kmeans 1>$DIR/logs/$num_threads 2>&1 &
    pid=$!
    ( tail -f -n0 $DIR/logs/$num_threads & ) | grep -q "iter = 10"
    kill -9 $pid
    sleep 5
done
