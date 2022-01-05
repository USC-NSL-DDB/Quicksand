#!/bin/bash

if [[ ! -v NODE_TYPE ]]; then
    echo 'Please set env var $NODE_TYPE, 
supported list: [c6525-100g, c6525-25g, xl170, xl170-uswitch, other]'
    exit 1
fi

if [ $NODE_TYPE == "c6525-100g" ]; then
    patch -p1 -d caladan/ < caladan/build/cloudlab_c6525.patch
fi

if [ $NODE_TYPE == "c6525-25g" ]; then
    patch -p1 -d caladan/ < caladan/build/cloudlab_c6525.patch
fi

if [ $NODE_TYPE == "xl170" ]; then
    patch -p1 -d caladan/ < caladan/build/cloudlab_xl170.patch
    patch -p1 -d caladan/ < caladan/build/connectx-4.patch    
fi

if [ $NODE_TYPE == "xl170-uswitch" ]; then
    patch -p1 -d caladan/ < caladan/build/cloudlab_xl170_uswitch.patch
    patch -p1 -d caladan/ < caladan/build/connectx-4.patch    
fi

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
