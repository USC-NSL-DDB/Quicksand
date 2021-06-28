#!/bin/bash

NU_CTRL_IP=18.18.1.3

docker run -d --rm --hostname dns.mageddo -p 5380:5380 \
       -v /var/run/docker.sock:/var/run/docker.sock \
       -v /etc/resolv.conf:/etc/resolv.conf \
       -v `pwd`/config/dns.json:/app/conf/config.json \
       defreitas/dns-proxy-server

pushd ymls
docker-compose -f jaeger.yml create
docker-compose -f social-graph-mongodb.yml create
docker-compose -f social-graph-redis.yml create
docker-compose -f home-timeline-redis.yml create
docker-compose -f post-storage-mongodb.yml create
docker-compose -f post-storage-memcached.yml create
docker-compose -f user-timeline-mongodb.yml create
docker-compose -f user-timeline-redis.yml create
docker-compose -f url-shorten-mongodb.yml create
docker-compose -f url-shorten-memcached.yml create
docker-compose -f user-mongodb.yml create
docker-compose -f user-memcached.yml create
docker-compose -f media-mongodb.yml create
docker-compose -f media-frontend.yml create
docker-compose -f nginx-thrift.yml create
docker-compose -f jaeger.yml start
docker-compose -f social-graph-mongodb.yml start
docker-compose -f social-graph-redis.yml start
docker-compose -f home-timeline-redis.yml start
docker-compose -f post-storage-mongodb.yml start
docker-compose -f post-storage-memcached.yml start
docker-compose -f user-timeline-mongodb.yml start
docker-compose -f user-timeline-redis.yml start
docker-compose -f url-shorten-mongodb.yml start
docker-compose -f url-shorten-memcached.yml start
docker-compose -f user-mongodb.yml start
docker-compose -f user-memcached.yml start
docker-compose -f media-mongodb.yml start
docker-compose -f media-frontend.yml start
docker-compose -f nginx-thrift.yml start
popd

sudo ../../caladan/iokerneld >logs/iokerneld 2>&1 &
sleep 5

sudo build/src/ComposePostService/ComposePostService ../../conf/controller CTL $NU_CTRL_IP >logs/ComposePostService.ctl 2>&1 &
sleep 3
sudo build/src/ComposePostService/ComposePostService ../../conf/server1 SRV $NU_CTRL_IP >logs/ComposePostService.srv 2>&1 &
sleep 3
sudo build/src/ComposePostService/ComposePostService ../../conf/client1 CLT $NU_CTRL_IP >logs/ComposePostService.clt 2>&1 &

sudo build/src/FrontEndProxy/FrontEndProxy ../../conf/client2 >logs/FrontEndProxy 2>&1 &
sudo build/src/SocialGraphService/SocialGraphService >logs/SocialGraphService 2>&1 &
sudo build/src/HomeTimelineService/HomeTimelineService >logs/HomeTimelineService 2>&1 &
sudo build/src/PostStorageSerivce/PostStorageService >logs/PostStorageService 2>&1 &
sudo build/src/UserTimelineService/UserTimelineService >logs/UserTimelineService 2>&1 &
sudo build/src/UrlShortenService/UrlShortenService >logs/UrlShortenService 2>&1 &
sudo build/src/UserService/UserService >logs/UserService 2>&1 &
sudo build/src/MediaService/MediaService >logs/MediaService 2>&1 &
sudo build/src/TextService/TextService >logs/TextService 2>&1 &
sudo build/src/UserMentionService/UserMentionService >logs/UserMentionService 2>&1 &
