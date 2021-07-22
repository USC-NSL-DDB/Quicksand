#!/bin/bash

cd thrift
./bootstrap.sh
./configure --enable-caladanthreads=yes --enable-caladantcp=yes \
            --with-caladan=`pwd`/../../../caladan/  \
            --enable-shared=no --enable-tests=no --enable-tutorial=no
make -j
cd ..

mkdir build
cd build
cmake ..
make -j
cd ..

