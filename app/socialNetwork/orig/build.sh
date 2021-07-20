#!/bin/bash

version=0.12.0 && git clone -b $version https://github.com/apache/thrift.git
cd thrift
./bootstrap.sh
./configure
make -j
cd ..

mkdir build
cd build
cmake ..
make -j
cd ..

