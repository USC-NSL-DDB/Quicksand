#!/bin/bash

source shared.sh

all_passed=1

local_unit_tests=("test_slab")

function prepare {
    if [[ ! -v DPDK_NIC ]]; then
	echo 'Please set env var $DPDK_NIC, e.g., export DPDK_NIC=enp5s0f0'
	exit 1
    fi
    sudo bridge fdb add 1E:CF:16:43:AF:94 self dev $DPDK_NIC 2>/dev/null
    sudo bridge fdb add 1E:CF:16:43:AF:95 self dev $DPDK_NIC 2>/dev/null
    sudo bridge fdb add 1E:CF:16:43:AF:96 self dev $DPDK_NIC 2>/dev/null
    sudo bridge fdb add 1E:CF:16:43:AF:97 self dev $DPDK_NIC 2>/dev/null
}

function run_test {
    BIN="$ROOT_PATH/bin/$1"

    run_controller_prog $BIN >/dev/null 2>&1 &
    disown -r
    sleep 3

    run_server_prog $BIN 1 >/dev/null 2>&1 &
    disown -r
    sleep 3

    run_server_prog $BIN 2 >/dev/null 2>&1 &
    disown -r
    sleep 3    

    run_client_prog $BIN 2>/dev/null | grep -q "Passed"
    ret=$?

    kill_process test_
    sleep 3
    return $ret
}

function run_local_unit_test {
    run_client_prog ./bin/$1 2>/dev/null | grep -q "Passed"
}

function run_single_test {
    echo "Running test $1..."
    rerun_iokerneld
    if [[ " ${local_unit_tests[@]} " =~ " $1 " ]]; then
	run_local_unit_test $1
    else
        run_test $1
    fi
    if [[ $? == 0 ]]; then
        say_passed
    else
        say_failed
        all_passed=0
    fi
}

function run_all_tests {
    TESTS=`ls bin | grep test_`
    for test in $TESTS
    do
        run_single_test $test
    done
}

function cleanup {
    kill_iokerneld
    sudo bridge fdb delete 1E:CF:16:43:AF:94 self dev $DPDK_NIC 2>/dev/null
    sudo bridge fdb delete 1E:CF:16:43:AF:95 self dev $DPDK_NIC 2>/dev/null
    sudo bridge fdb delete 1E:CF:16:43:AF:96 self dev $DPDK_NIC 2>/dev/null
    sudo bridge fdb delete 1E:CF:16:43:AF:97 self dev $DPDK_NIC 2>/dev/null
}

prepare
run_all_tests
cleanup

if [[ $all_passed -eq 1 ]]; then
    exit 0
else
    exit 1
fi
