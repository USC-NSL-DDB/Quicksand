#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PROJ_DIR=$SCRIPT_DIR/..

# Step 1: install dependencies.
sudo apt-get -y install ffmpeg libx264-dev
mkdir -p $PROJ_DIR/bin
cd $PROJ_DIR/bin
curl -LJO https://github.com/excamera/excamera-static-bins/raw/master/vpxenc -o vpxenc
chmod +x vpxenc
curl -LJO https://github.com/excamera/excamera-static-bins/raw/master/vpxdec -o vpxdec
chmod +x vpxdec

# Step 2: build alfalfa
cd $PROJ_DIR/alfalfa
./autogen.sh
./configure X264_LIBS=/lib/x86_64-linux-gnu/libx264.a
make -j $(nproc)
sudo make install

# Step 3: build our ExCamera implementation
cd $PROJ_DIR
make clean
make -j

# Step 4: setup input
mkdir -p $PROJ_DIR/input
cd $PROJ_DIR/input
# Big Buck Bunny
curl https://media.xiph.org/video/derf/y4m/big_buck_bunny_480p24.y4m.xz -o bunny_480p.y4m.xz
xz -d bunny_480p.y4m.xz
mkdir bunny_480p
# 96 (6 * 16 in ExCamera) frames per segment
ffmpeg -i bunny_480p.y4m -f segment -segment_time 4 bunny_480p/bunny%2d.y4m

# Step 5: preprocess input
cd $SCRIPT_DIR
./preprocess.py

