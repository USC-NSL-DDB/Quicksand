#!/bin/bash

source ../shared.sh
CTRL_IP=18.18.1.3
LPID=1

heap_sizes=( 65536 131072 262144 524288 1048576 2097152 4194304 8388608 16777216 )

mkdir logs
rm -rf logs/*

SRC_SERVER=$SERVER2_IP
DEST_SERVER=$SERVER3_IP

ssh $SRC_SERVER "source `pwd`/../shared.sh; set_bridge $CONTROLLER_ETHER"
ssh $SRC_SERVER "source `pwd`/../shared.sh; set_bridge $CLIENT1_ETHER"
ssh $SRC_SERVER "source `pwd`/../shared.sh; set_bridge $SERVER1_ETHER"
pushd $NU_DIR
sed "s/constexpr static bool kEnableLogging = .*/constexpr static bool kEnableLogging = true;/g" \
    -i src/migrator.cpp
make -j
popd

make clean

for heap_size in ${heap_sizes[@]}
do
    sleep 5
    ssh $SRC_SERVER "sudo cset shield --exec -- $NU_DIR/caladan/iokerneld" &
    ssh $DEST_SERVER "sudo cset shield --exec -- $NU_DIR/caladan/iokerneld" &
    sleep 5
    sed "s/constexpr uint32_t kObjSize = .*/constexpr uint32_t kObjSize = $heap_size;/g" -i main.cpp
    make
    scp main $SRC_SERVER:`pwd`
    scp main $DEST_SERVER:`pwd`    
    ssh $SRC_SERVER "sudo cset shield --exec -- $NU_DIR/bin/ctrl_main `pwd`/conf/controller CTL" &
    sleep 5
    ssh $SRC_SERVER "cd `pwd`; sudo cset shield --exec -- ./main conf/server1 SRV $CTRL_IP $LPID" 1>logs/$heap_size.src 2>&1 &
    ssh $DEST_SERVER "cd `pwd`; sudo cset shield --exec -- ./main conf/server2 SRV $CTRL_IP $LPID" 1>logs/$heap_size.dest 2>&1 &
    sleep 5
    ssh $SRC_SERVER "cd `pwd`; sudo cset shield --exec -- ./main conf/client1 CLT $CTRL_IP $LPID"
    ssh $SRC_SERVER "sudo pkill -9 iokerneld"
    ssh $SRC_SERVER "sudo pkill -9 main"
    ssh $DEST_SERVER "sudo pkill -9 iokerneld"
    ssh $DEST_SERVER "sudo pkill -9 main"
done

ssh $SRC_SERVER "source `pwd`/../shared.sh; unset_bridge $CONTROLLER_ETHER"
ssh $SRC_SERVER "source `pwd`/../shared.sh; unset_bridge $CLIENT1_ETHER"
ssh $SRC_SERVER "source `pwd`/../shared.sh; unset_bridge $SERVER1_ETHER"
pushd $NU_DIR
sed "s/constexpr static bool kEnableLogging = .*/constexpr static bool kEnableLogging = false;/g" \
    -i src/migrator.cpp
make -j
popd
