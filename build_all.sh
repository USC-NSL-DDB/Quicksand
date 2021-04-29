#!/bin/bash

export glibc_install="$(pwd)/glibc/build/install"
git clone git://sourceware.org/git/glibc.git
cd glibc
git checkout glibc-2.32
mkdir build
cd build
LD_LIBRARY_PATH_BAK=$LD_LIBRARY_PATH
unset LD_LIBRARY_PATH
../configure --prefix "$glibc_install" --enable-static-nss
make -j `nproc`
make install -j `nproc`
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH_BAK
cd ../..

cd caladan
./build.sh
cd ..
make clean
make -j
