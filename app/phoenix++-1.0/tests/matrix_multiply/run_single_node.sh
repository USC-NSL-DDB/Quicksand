#!/bin/bash

for i in `seq 1 46`
do
    sudo pkill -9 iokerneld
    sudo pkill -9 matrix
    sleep 5
    sed "s/runtime_kthreads .*/runtime_kthreads $i/g" -i /mnt/nu/conf/server1
    sudo /mnt/nu/caladan/iokerneld &
    sleep 5
    sudo ./matrix_multiply /mnt/nu/conf/controller CTL 18.18.1.3 &
    sleep 3
    sudo ./matrix_multiply /mnt/nu/conf/server1 SRV 18.18.1.3 &
    sleep 3
    sudo ./matrix_multiply /mnt/nu/conf/client1 CLT 18.18.1.3 4000 1>logs/$i 2>&1
done
