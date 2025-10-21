#!/bin/bash

# start iokerneld with DEBUGGER-aware mode
echo "Starting IOKernel"
sudo ./caladan/iokerneld ias dbg > .iokernel.log 2>&1  &
sleep 5

# start ctrl node with proxy node (used by DDB to query the ctrl node)
echo "Starting ctrl node"
sudo ./bin/ctrl_main > .ctrl_main.log 2>&1  &
sleep 5
echo "Starting ctrl proxy node (used by DDB for migration support)"
sudo ./bin/ctrl_proxy > .ctrl_proxy.log 2>&1  &
sleep 5

