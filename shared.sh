#!/bin/bash

ROOT_PATH="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
CALADAN_PATH=$ROOT_PATH/caladan

function say_failed() {
    echo -e "----\e[31mFailed\e[0m"
}

function say_passed() {
    echo -e "----\e[32mPassed\e[0m"
}

function assert_success {
    if [[ $? -ne 0 ]]; then
        say_failed
        exit -1
    fi
}

function kill_process {
    pid=`pgrep $1`
    if [ -n "$pid" ]; then
	{ sudo kill $pid && sudo wait $pid; } 2>/dev/null
    fi
}

function kill_iokerneld {
    kill_process iokerneld
}

function run_iokerneld {
    kill_iokerneld
    sleep 3
    sudo $CALADAN_PATH/iokerneld $@ > /dev/null 2>&1 &
    disown -r
    assert_success
    sleep 5
}

function rerun_iokerneld {
    kill_iokerneld
    run_iokerneld simple
}

function run_client_prog {
    sudo stdbuf -o0 sh -c "$1 $ROOT_PATH/conf/client1 CLT"
}

function run_server_prog {
    sudo stdbuf -o0 sh -c "$1 $ROOT_PATH/conf/server$2 SRV"
}

function run_controller_prog {
    sudo stdbuf -o0 sh -c "$1 $ROOT_PATH/conf/controller CTL"
}

