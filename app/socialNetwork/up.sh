#!/bin/bash

NU_CTRL_IP=18.18.1.3

docker run -d --rm --hostname dns.mageddo -p 5380:5380 \
       -v /var/run/docker.sock:/var/run/docker.sock \
       -v /etc/resolv.conf:/etc/resolv.conf \
       -v `pwd`/config/dns.json:/app/conf/config.json \
       defreitas/dns-proxy-server

pushd ymls
docker-compose -f nginx-thrift.yml create
docker-compose -f nginx-thrift.yml start
popd

sudo ../../caladan/iokerneld >logs/iokerneld 2>&1 &
sleep 5

sudo build/src/BackEndService ../../conf/controller CTL $NU_CTRL_IP >logs/BackEndService.ctl 2>&1 &
sleep 3
sudo build/src/BackEndService ../../conf/server1 SRV $NU_CTRL_IP >logs/BackEndService.srv 2>&1 &
sleep 3
sudo build/src/BackEndService ../../conf/client1 CLT $NU_CTRL_IP >logs/BackEndService.clt 2>&1 &
