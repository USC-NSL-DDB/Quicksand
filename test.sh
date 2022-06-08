#!/bin/bash

source shared.sh

all_passed=1

local_unit_tests=("test_slab" "test_perf" "test_tcp_poll")

function prepare {
    source setup.sh >/dev/null 2>&1
    sudo sync; sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"
}

function run_test {
    BIN="$ROOT_PATH/bin/$1"

    run_controller 1>/dev/null 2>&1 &
    disown -r
    sleep 3

    run_server 1 $BIN 1>/dev/null 2>&1 &
    disown -r
    sleep 3

    run_server 2 $BIN 1>/dev/null 2>&1 &
    disown -r
    sleep 3    

    run_client $BIN 2>/dev/null | grep -q "Passed"
    ret=$?

    kill_controller
    kill_process test_
    sleep 5
    return $ret
}

function run_local_unit_test {
    run_client ./bin/$1 2>/dev/null | grep -q "Passed"
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
    prune_fdb_table
}

function force_cleanup {
    kill_controller
    kill_process test_
    cleanup
    exit 1
}

trap force_cleanup INT

prepare
run_all_tests
cleanup

if [[ $all_passed -eq 1 ]]; then
    exit 0
else
    exit 1
fi
