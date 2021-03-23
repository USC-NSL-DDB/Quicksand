#!/bin/bash

make submodules -j
make clean && make -j
pushd ksched
make clean && make -j
popd
pushd bindings/cc/
make -j
popd
sudo ./scripts/setup_machine.sh
