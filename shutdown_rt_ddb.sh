#!/bin/bash

# Give a chance for graceful shutdown
sudo pkill -INT iokerneld
sudo pkill -INT ctrl_main
sudo pkill -INT ctrl_proxy

sleep 5
sudo pkill -9 iokerneld 2>&1 > /dev/null
sudo pkill -9 ctrl_main 2>&1 > /dev/null
sudo pkill -9 ctrl_proxy 2>&1 > /dev/null

