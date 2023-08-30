#!/bin/bash

SETUP_SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

function get_nic_dev {
    sudo bash -c "$SETUP_SCRIPT_DIR/caladan/iokerneld >.tmp 2>&1 &"
    ( tail -f -n0 .tmp & ) | grep -q "MAC"
    sudo pkill -9 iokerneld
    mac=`cat .tmp | grep "MAC" | sed "s/.*MAC: \(.*\)/\1/g" | tr " " ":"`
    sudo rm .tmp
    nic_dev=`ifconfig | grep "flags\|ether" | awk 'NR%2{printf "%s ",$0;next;}1' \
             | grep $mac | awk -F ':' '{print $1}'`
}

function setup_caladan {
    sudo $SETUP_SCRIPT_DIR/caladan/scripts/setup_machine.sh
}

function setup_jumbo_frame {
    sudo ifconfig $nic_dev mtu 9000
}

function setup_trust_dscp {
    sudo mlnx_qos -i $nic_dev --trust dscp
}

function setup_dropless_rq {
    sudo ethtool --set-priv-flags $nic_dev dropless_rq on
}

function setup_pfc {
    sudo ethtool -A $nic_dev rx off tx off
    sudo mlnx_qos -i $nic_dev -f 1,1,1,1,1,1,1,1 \
	                       -p 0,1,2,3,4,5,6,7 \
	                       --prio2buffer 0,0,0,0,0,0,0,0 \
			       -s strict,strict,strict,strict,strict,strict,strict,strict
    sudo mlnx_qos -i $nic_dev --buffer_size=524160,0,0,0,0,0,0,0
}

trap -- '' INT TERM

setup_caladan
get_nic_dev
setup_jumbo_frame
setup_trust_dscp
setup_pfc
setup_dropless_rq
