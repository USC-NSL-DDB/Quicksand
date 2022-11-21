#!/bin/bash

cd ../..
source ./shared.sh

all_passed=1
CLIENT_IP="18.18.1.4"
SERVER1_IP="18.18.1.2"
SERVER2_IP="18.18.1.3"
LPID=1

function prepare {
    kill_iokerneld
    kill_controller
    sleep 5
    source ./setup.sh >/dev/null 2>&1
    sudo sync; sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"
}

function run_client {
    sudo stdbuf -o0 sh -c "ulimit -c unlimited; $1 -c -l $LPID -i $CLIENT_IP"
}

function run_server1 {
    sudo stdbuf -o0 sh -c "ulimit -c unlimited; $1 -s -l $LPID -i $SERVER1_IP"
}

function run_server2 {
    sudo stdbuf -o0 sh -c "ulimit -c unlimited; $1 -s -l $LPID -i $SERVER2_IP"
}

function run_test {
    BIN="$ROOT_PATH/app/imagenet/$1"

    run_controller 1>/dev/null 2>&1 &
    disown -r
    sleep 3

    run_server1 $BIN 1>/dev/null 2>&1 &
    disown -r
    sleep 3

    run_server2 $BIN 1>/dev/null 2>&1 &
    disown -r
    sleep 3    

    run_client $BIN
    ret=0

    kill_process $1
    kill_controller
    sleep 5

    sudo mv core core.$1 1>/dev/null 2>&1

    return $ret
}

function cleanup {
    kill_iokerneld
}

function force_cleanup {
    kill_process $1
    kill_controller
    cleanup
    exit 1
}

trap force_cleanup INT

prepare

rerun_iokerneld
run_test $1

cleanup
