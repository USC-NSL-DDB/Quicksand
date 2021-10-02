#!/bin/bash

DPDK_NIC=ens1f0
CONTROLLER_ETHER=1E:CF:16:43:AF:95
CLIENT1_ETHER=1E:CF:16:43:AF:94
SERVER1_ETHER=1E:CF:16:43:AF:96
SERVER2_ETHER=1E:CF:16:43:AF:97
SERVER2_IP=10.10.2.1
SERVER3_IP=10.10.2.2
SERVER4_IP=10.10.2.4
SERVER5_IP=10.10.2.5
SERVER6_IP=10.10.2.6
SERVER7_IP=10.10.2.7
REMOTE_SERVER_IPS=( $SERVER2_IP $SERVER3_IP $SERVER4_IP $SERVER5_IP $SERVER6_IP $SERVER7_IP )
SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
NU_DIR=$SCRIPT_DIR/..

function set_bridge {
    sudo bridge fdb add $1 self dev $DPDK_NIC 2>/dev/null
}

function unset_bridge {
    sudo bridge fdb delete $1 self dev $DPDK_NIC 2>/dev/null
}

function to_sib_addr {
    echo `echo $1 | sed "s/10\.10\.2\(.*\)/10\.10\.1\1/g"`
}
