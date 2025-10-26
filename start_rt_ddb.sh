#!/bin/bash

SKIP_CTRL=false
for arg in "$@"; do
    if [ "$arg" = "-h" ] || [ "$arg" = "--help" ]; then
        echo "Usage: $0 [-nc|--no_ctrl] [-h|--help]"
        echo "  -nc, --no_ctrl: Skip starting ctrl node and proxy"
        echo "  -h, --help: Show this help message"
        exit 0
    fi

    if [ "$arg" = "-nc" ] || [ "$arg" = "--no_ctrl" ]; then
        SKIP_CTRL=true
        break
    fi
done

for arg in "$@"; do
done

# start iokerneld with DEBUGGER-aware mode
echo "Starting IOKernel"
sudo ./caladan/iokerneld ias dbg > .iokernel.log 2>&1  &
sleep 5

if [ "$SKIP_CTRL" = false ]; then
    # start ctrl node with proxy node (used by DDB to query the ctrl node)
    echo "Starting ctrl node"
    sudo ./bin/ctrl_main > .ctrl_main.log 2>&1  &
    sleep 5
    echo "Starting ctrl proxy node (used by DDB for migration support)"
    sudo ./bin/ctrl_proxy > .ctrl_proxy.log 2>&1  &
    sleep 5
fi
