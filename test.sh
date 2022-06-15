#!/bin/bash

source shared.sh

all_passed=1
CLIENT_IP="18.18.1.4"
SERVER1_IP="18.18.1.2"
SERVER2_IP="18.18.1.3"
LPID=1

function prepare {
    kill_iokerneld
    kill_controller
    sleep 5
    source setup.sh >/dev/null 2>&1
    sudo sync; sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"
}

function run_client {
    sudo stdbuf -o0 sh -c "$1 -c -l $LPID -i $CLIENT_IP"
}

function run_server1 {
    sudo stdbuf -o0 sh -c "$1 -s -l $LPID -i $SERVER1_IP"
}

function run_server2 {
    sudo stdbuf -o0 sh -c "$1 -s -l $LPID -i $SERVER2_IP"
}

function run_test {
    BIN="$ROOT_PATH/bin/$1"

    run_controller 1>/dev/null 2>&1 &
    disown -r
    sleep 3

    run_server1 $BIN 1>/dev/null 2>&1 &
    disown -r
    sleep 3

    run_server2 $BIN 1>/dev/null 2>&1 &
    disown -r
    sleep 3    

    run_client $BIN 2>/dev/null | grep -q "Passed"
    ret=$?

    kill_controller
    kill_process test_
    sleep 5
    return $ret
}

function run_single_test {
    echo "Running test $1..."
    rerun_iokerneld
    run_test $1
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
