#!/bin/bash

DPDK_NIC=ens1f0
CONTROLLER_ETHER=1E:CF:16:43:AF:95
CLIENT1_ETHER=1E:CF:16:43:AF:94
SERVER1_ETHER=1E:CF:16:43:AF:96
SERVER2_ETHER=1E:CF:16:43:AF:97
SERVER2_IP=10.10.2.1
SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
NU_DIR=$SCRIPT_DIR/..

function set_bridge {
    sudo bridge fdb add $1 self dev $DPDK_NIC 2>/dev/null
}

function unset_bridge {
    sudo bridge fdb delete $1 self dev $DPDK_NIC 2>/dev/null
}
