#!/bin/bash

sudo apt update && sudo apt install -y cmake g++ wget unzip

cd opencv
rm -rf build
mkdir build
rm -rf install
mkdir install

cd build
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=../install ..
cmake --build . --target install -j

