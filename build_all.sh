#!/bin/bash

# Config instance type.
if [[ ! -v NODE_TYPE ]]; then
    echo 'Please set env var $NODE_TYPE, 
supported list: [c6525-100g, c6525-25g, xl170, xl170-uswitch, other]'
    exit 1
fi

# Patch source files.
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

patch -p1 -d caladan/ < caladan/build/runtime_fdb.patch

# Build caladan.
cd caladan
./build.sh
cd ..

# Build Nu.
make clean
make -j

# Setup Nu.
./setup.sh

