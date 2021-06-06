#!/bin/bash

function check_param {
    if [[ ! -v DPDK_NIC ]]; then
        echo 'Please set env var $DPDK_NIC, e.g., export DPDK_NIC=enp5s0f0'
        exit 1
    fi
}

function setup_caladan {
    sudo ./caladan/scripts/setup_machine.sh    
}

function setup_jumbo_frame {
    sudo ifconfig $DPDK_NIC mtu 9000
}

function setup_trust_dscp {
    sudo mlnx_qos -i $DPDK_NIC --trust dscp
}

function setup_pfc {
    sudo ethtool -A $DPDK_NIC rx off tx off
    sudo mlnx_qos -i $DPDK_NIC -f 1,1,1,1,1,1,1,1 \
	                       -p 0,1,2,3,4,5,6,7 \
	                       --prio2buffer 0,1,1,1,1,1,1,1 \
			       -s strict,strict,strict,strict,strict,strict,strict,strict
}

function setup_dropless_rq {
    sudo ethtool --set-priv-flags $DPDK_NIC dropless_rq on
}

check_param
setup_caladan
setup_jumbo_frame
setup_trust_dscp
setup_pfc
setup_dropless_rq

