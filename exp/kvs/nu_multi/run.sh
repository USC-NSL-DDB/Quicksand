#!/bin/bash

source ../../shared.sh

CTL_IDX=32
SRV_START_IDX=1
LPID=1
KS=26

DIR=`pwd`

get_srv_idx() {
    echo `expr $1 + $SRV_START_IDX - 1`
}

get_clt_idx() {
    echo `expr $(get_srv_idx $1) + $num_srvs`
}

for num_srvs in `seq 1 15`
do
    num_clts=`expr $num_srvs + 1`

    start_iokerneld $CTL_IDX
    for i in `seq 1 $num_srvs`
    do
	start_iokerneld $(get_srv_idx $i)
    done
    for i in `seq 1 $num_clts`
    do
	start_iokerneld $(get_clt_idx $i)
    done
    sleep 5

    start_ctrl $CTL_IDX
    sleep 5

    sed "s/constexpr uint32_t kNumProxies.*/constexpr uint32_t kNumProxies = $num_srvs;/g" \
	-i server.cpp
    sed "s/constexpr uint32_t kNumProxies.*/constexpr uint32_t kNumProxies = $num_srvs;/g" \
	-i client.cpp
    make -j

    for i in `seq 1 $num_srvs`
    do
	srv_idx=$(get_srv_idx $i)
	distribute server $srv_idx

	if [[ $i -ne $num_srvs ]]
	then
	    start_server server $srv_idx $LPID $KS $KS &
	else
	    sleep 5
	    start_main_server server $srv_idx $LPID $KS $KS >logs/.tmp &
	fi
    done
    ( tail -f logs/.tmp & ) | grep -q "finish initing"
    rm logs/.tmp

    for i in `seq 1 $num_clts`
    do
	clt_idx=$(get_clt_idx $i)
	distribute client $clt_idx

	run_program client $clt_idx $DIR/conf/client$i >logs/$num_srvs.$i &
    done
    sleep 45

    cleanup
    sleep 5
done
