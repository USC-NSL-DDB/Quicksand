#!/bin/bash

function get_nic_dev {
    sudo caladan/iokerneld >.tmp 2>&1 &
    disown -r
    ( tail -f -n0 .tmp & ) | grep -q "MAC"
    sudo pkill -9 iokerneld
    mac=`cat .tmp | grep "MAC" | sed "s/.*MAC: \(.*\)/\1/g" | tr " " ":"`
    rm .tmp
    nic_dev=`ifconfig | grep "flags\|ether" | awk 'NR%2{printf "%s ",$0;next;}1' \
             | grep $mac | awk -F ':' '{print $1}'`
}

function setup_caladan {
    sudo ./caladan/scripts/setup_machine.sh    
}

function setup_jumbo_frame {
    sudo ifconfig $nic_dev mtu 9000
}

function setup_trust_dscp {
    sudo mlnx_qos -i $nic_dev --trust dscp
}

function setup_pfc {
    sudo ethtool -A $nic_dev rx off tx off
    sudo mlnx_qos -i $nic_dev -f 1,1,1,1,1,1,1,1 \
	                       -p 0,1,2,3,4,5,6,7 \
	                       --prio2buffer 0,1,1,1,1,1,1,1 \
			       -s strict,strict,strict,strict,strict,strict,strict,strict
}

function setup_dropless_rq {
    sudo ethtool --set-priv-flags $nic_dev dropless_rq on
}

function prune_fdb_table {
    sudo bridge fdb | grep $nic_dev | awk '{print $1, $2, $3}' | \
	xargs -d '\n' -I {} bash -c "sudo bridge fdb delete {}"
}

setup_caladan
get_nic_dev
setup_jumbo_frame
setup_trust_dscp
#setup_pfc
setup_dropless_rq
prune_fdb_table
