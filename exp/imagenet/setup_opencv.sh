#!/bin/bash

IMAGENET_DIR=../../../../app/imagenet/
cd $IMAGENET_DIR
if [ ! -d opencv/install ]
then
    echo "Setting up opencv..."
    ./build_opencv.sh
fi
