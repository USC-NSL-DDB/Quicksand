#!/bin/bash

IMAGENET_DIR=../../../../app/imagenet/
cd $IMAGENET_DIR
if [ ! -d train_images ]
then
    echo "Setting up input images..."
    mkdir train_images
    for i in `seq 1 3000`
    do
	cp -r images_base train_images/$i
    done
fi
