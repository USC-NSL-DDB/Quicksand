#!/bin/bash

source shared.sh

USAGE="Usage: $0 [tests_prefix]
"

all_passed=1
CLIENT_IP="18.18.1.4"
SERVER1_IP="18.18.1.2"
SERVER2_IP="18.18.1.3"
LPID=1

tests_prefix=
while (( "$#" )); do
	case "$1" in
		-h|--help) echo "$USAGE" >&2 ; exit 0 ;;
		*) tests_prefix=$1 ; shift ;;
	esac
done

function prepare {
    kill_iokerneld
    kill_controller
    sleep 5
    source setup.sh >/dev/null 2>&1
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

    run_client $BIN 2>/dev/null | tee log
    cat log | grep -q "Passed"
    ret=$?

    kill_process test_
    kill_controller
    sleep 5

    sudo mv core core.$1 1>/dev/null 2>&1

    return $ret
}

function run_tests {
    TESTS=`ls bin | grep $1`
    for test in $TESTS
    do
	echo "Running test $test..."
	rerun_iokerneld
	run_test $test
	if [[ $? == 0 ]]; then
            say_passed
	else
            say_failed
            all_passed=0
	fi
    done
}

function cleanup {
    kill_iokerneld
}

function force_cleanup {
    kill_process test_
    kill_controller
    cleanup
    exit 1
}

trap force_cleanup INT

prepare
if [[ -z $tests_prefix ]]; then
    run_tests test_
else
    run_tests $tests_prefix
fi
cleanup

if [[ $all_passed -eq 1 ]]; then
    exit 0
else
    exit 1
fi
