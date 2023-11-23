#!/bin/bash

EXP_SHARED_SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
NU_DIR=$EXP_SHARED_SCRIPT_DIR/..
CALADAN_DIR=$NU_DIR/caladan
SSH_IP_PREFIX=zg0
SSH_IP_SUFFIX_STARTING=1

source $EXP_SHARED_SCRIPT_DIR/../setup.sh

function my_ssh() {
    ssh -oStrictHostKeyChecking=no $@
}

function ssh_ip() {
    ssh_ip_suffix=$(expr $1 + $SSH_IP_SUFFIX_STARTING - 1)
    echo $SSH_IP_PREFIX$ssh_ip_suffix
}

function caladan_srv_ip() {
    srv_idx=$1
    echo "18.18.1."$(($srv_idx+1))
}

function probe_num_nodes() {
    num_nodes=1
    while true
    do
	ping -c 1 $(ssh_ip $(expr $num_nodes + 1)) 1>/dev/null 2>&1
    	if [ $? != 0 ]
    	then
    	    break
    	fi
	num_nodes=$(expr $num_nodes + 1)
    done
}

function executable_file_path() {
    if [[ "$1" = /* ]]
    then
	echo $1
    else
	echo "./$1"
    fi
}

function start_iokerneld() {
    srv_idx=$1
    args=${@:2}
    my_ssh $(ssh_ip $srv_idx) "sudo $CALADAN_DIR/iokerneld $args" &
}

function start_ctrl() {
    srv_idx=$1
    my_ssh $(ssh_ip $srv_idx) "sudo stdbuf -o0 $NU_DIR/bin/ctrl_main" &
}

function __start_server() {
    file_path=$(executable_file_path $1)
    srv_idx=$2
    lpid=$3
    main=$4
    if [[ $5 -eq 0 ]]
    then
	ks_cmd=""
    else
	ks_cmd="-k $5"
    fi
    if [[ $6 -eq 0 ]]
    then
	spin_ks=0
    else
	spin_ks=$6
    fi
    if [[ $7 -eq 0 ]]
    then
        isol_cmd=""
    else
	isol_cmd="--isol"
    fi
    if [[ $8 -eq 0 ]]
    then
	ip=$(caladan_srv_ip $srv_idx)
    else
	ip=$8
    fi
    app_args="${@:9}"
    nu_libs_name=".nu_libs_$BASHPID"
    rm -rf .nu_libs_tmp
    mkdir .nu_libs_tmp
    cp `ldd $file_path | grep "=>" | awk  '{print $3}' | xargs` .nu_libs_tmp
    my_ssh $(ssh_ip $srv_idx) "rm -rf $nu_libs_name"
    scp -r .nu_libs_tmp $(ssh_ip $srv_idx):`pwd`/$nu_libs_name

    if [[ $main -eq 0 ]]
    then
	my_ssh $(ssh_ip $srv_idx) "cd `pwd`;
                                sudo LD_LIBRARY_PATH=$nu_libs_name stdbuf -o0 $file_path -l $lpid -i $ip $ks_cmd -p $spin_ks $isol_cmd -d $app_args"
    else
	my_ssh $(ssh_ip $srv_idx) "cd `pwd`;
                                sudo LD_LIBRARY_PATH=$nu_libs_name stdbuf -o0 $file_path -m -l $lpid -i $ip $ks_cmd -p $spin_ks $isol_cmd -d $app_args"
    fi
}

function start_server() {
    __start_server $1 $2 $3 0 $4 $5 0 0 ${@:6}
}

function start_server_isol() {
    __start_server $1 $2 $3 0 $4 $5 1 0 ${@:6}
}

function start_server_isol_with_ip() {
    __start_server $1 $2 $3 0 $4 $5 1 $6 ${@:7}
}

function start_main_server() {
    __start_server $1 $2 $3 1 $4 $5 0 0 ${@:6}
}

function start_main_server_isol() {
    __start_server $1 $2 $3 1 $4 $5 1 0 ${@:6}
}

function run_program() {
    file_path=$(executable_file_path $1)
    srv_idx=$2
    args=${@:3}
    my_ssh $(ssh_ip $srv_idx) "cd `pwd`; sudo $file_path $args"
}

function run_cmd() {
    srv_idx=$1
    cmd=${@:2}
    my_ssh $(ssh_ip $srv_idx) "cd `pwd`; $cmd"
}

function distribute() {
    file_path=$1
    file_full_path=$(readlink -f $file_path)
    src_idx=$2
    cp -r $file_full_path .distribute
    scp -r .distribute $(ssh_ip $src_idx):$file_full_path
    rm -rf .distribute
}

function prepare() {
    probe_num_nodes
    cleanup
    sleep 5
    for i in `seq 1 $num_nodes`
    do
	my_ssh $(ssh_ip $i) "cd $NU_DIR; sudo ./setup.sh" &
    done
    wait $(jobs -p)
}

function rebuild_caladan_and_nu() {
    pushd $CALADAN_DIR
    make clean
    make -j`nproc`
    cd bindings/cc
    make clean
    make -j`nproc`
    cd ../../..
    make clean
    make -j`nproc`
    popd
}

function caladan_use_small_rto() {
    small_rto=1
    pushd $CALADAN_DIR/runtime/net
    cp tcp.h tcp.h.bak
    sed "s/#define TCP_ACK_TIMEOUT.*/#define TCP_ACK_TIMEOUT ONE_MS/g" -i tcp.h
    sed "s/#define TCP_OOQ_ACK_TIMEOUT.*/#define TCP_OOQ_ACK_TIMEOUT ONE_MS/g" -i tcp.h
    sed "s/#define TCP_ZERO_WND_TIMEOUT.*/#define TCP_ZERO_WND_TIMEOUT ONE_MS/g" -i tcp.h
    sed "s/#define TCP_RETRANSMIT_TIMEOUT.*/#define TCP_RETRANSMIT_TIMEOUT ONE_MS/g" -i tcp.h
    rebuild_caladan_and_nu
    popd
}

function caladan_use_default_rto() {
    pushd $CALADAN_DIR/runtime/net
    git checkout -- tcp.h
    rebuild_caladan_and_nu
    popd
}

function disable_kernel_bg_prezero() {
    for i in `seq 1 $num_nodes`
    do
	my_ssh $(ssh_ip $i) "sudo bash -c \"echo 1000000 > /sys/kernel/mm/zero_page/delay_millisecs\""
    done
}

function enable_kernel_bg_prezero() {
    for i in `seq 1 $num_nodes`
    do
	my_ssh $(ssh_ip $i) "sudo bash -c \"echo 1000 > /sys/kernel/mm/zero_page/delay_millisecs\""
    done
}

function cleanup_server() {
    my_ssh $(ssh_ip $1) "pnames=( iokerneld ctrl_main main client server synthetic memcached \
                                  kmeans python3 BackEndService bench distributed quicksand xc_ );
                         for name in \${pnames[@]}; do
                             sudo pkill -U 0 -9 \$name;
                         done"
    if [ -n "$nic_dev" ]
    then
        my_ssh $(ssh_ip $1) "sudo bridge fdb | grep $nic_dev | awk '{print $1}' | \
                             xargs -I {} bash -c \"sudo bridge fdb delete {} dev $nic_dev\""
    fi
    my_ssh $(ssh_ip $1) "cd `pwd`; rm -rf .nu_libs*"
}

function cleanup() {
    for i in `seq 1 $num_nodes`
    do
	cleanup_server $i &
    done
    wait $(jobs -p)
}

function force_cleanup() {
    echo -e "\nPlease wait for proper cleanups..."
    cleanup
    for i in `seq 1 $num_nodes`
    do
	if [ -n "$nic_dev" ]
	then
            my_ssh $(ssh_ip $i) "sudo ip addr show $nic_dev | grep \"inet \" | tail -n +2 | \
                                 awk '{print \$2}' | xargs -I {} sudo ip addr delete {} dev $nic_dev" &
	fi
    done
    wait $(jobs -p)
    sudo pkill -9 run.sh
    exit 1
}

trap force_cleanup INT
trap cleanup EXIT

prepare
sleep 5

rm -rf logs.bak
[ -d logs ] && mv logs logs.bak
mkdir logs
