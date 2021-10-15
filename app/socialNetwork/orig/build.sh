#!/bin/bash

pushd ../thrift
./bootstrap.sh
./configure --enable-caladanthreads=yes --enable-caladantcp=yes \
            --with-caladan=`pwd`/../../../caladan/  \
            --enable-shared=no --enable-tests=no --enable-tutorial=no \
	    --with-libevent=no
make -j
popd

mkdir build
cd build
cmake ..
make -j
cd ..
