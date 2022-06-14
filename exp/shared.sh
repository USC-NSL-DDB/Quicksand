#!/bin/bash

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
NU_DIR=$SCRIPT_DIR/..
CALADAN_DIR=$NU_DIR/caladan

function ssh_ip() {
    srv_idx=$1
    echo "10.10.2."$srv_idx
}

function caladan_srv_ip() {
    srv_idx=$1
    echo "18.18.1."$(($srv_idx+1))
}

function caladan_clt_ip() {
    clt_idx=$1
    echo "18.18.1."$(($clt_idx+128))
}

function probe_num_nodes() {
    num_nodes=1
    while true
    do
	next_node=$(($num_nodes + 1))
	ping -c 1 $(ssh_ip $next_node) 1>/dev/null 2>&1
	if [ $? != 0 ]
	then
	    break
	fi
	num_nodes=$next_node
    done    
}

function cleanup() {
    for i in `seq 1 $num_nodes`
    do
	ssh $(ssh_ip $i) "sudo pkill -9 iokerneld; \
                          sudo pkill -9 ctrl_main;
                          sudo pkill -9 main;
                          sudo pkill -9 client;
                          sudo pkill -9 server;"
    done
}

function force_cleanup() {
    cleanup
    sudo pkill -9 run.sh
    exit 1
}

function start_iokerneld() {
    srv_idx=$1
    ssh $(ssh_ip $srv_idx) "sudo $CALADAN_DIR/iokerneld" &
}

function start_ctrl() {
    srv_idx=$1
    ssh $(ssh_ip $srv_idx) "sudo $NU_DIR/bin/ctrl_main" &
}

function start_server() {
    file_path=$1    
    file_full_path=$(readlink -f $file_path)
    srv_idx=$2
    ip=$(caladan_srv_ip $srv_idx)
    lpid=$3
    ssh $(ssh_ip $srv_idx) "sudo $file_full_path -s -l $lpid -i $ip"
}

function start_client() {
    file_path=$1    
    file_full_path=$(readlink -f $file_path)
    clt_idx=$2
    ip=$(caladan_clt_ip $clt_idx)
    lpid=$3
    ssh $(ssh_ip $clt_idx) "sudo $file_full_path -c -l $lpid -i $ip"
}

function distribute() {
    file_path=$1
    file_full_path=$(readlink -f $file_path)
    src_idx=$2
    scp $file_full_path $(ssh_ip $src_idx):$file_full_path
}

function prepare() {
    probe_num_nodes
    for i in `seq 1 $num_nodes`
    do
	ssh $(ssh_ip $i) "cd $NU_DIR; sudo ./setup.sh" &
    done
    wait
}

trap force_cleanup INT
trap cleanup EXIT

prepare
cleanup
sleep 5

rm -rf logs.bak
[ -d logs ] && mv logs logs.bak
mkdir logs
