#!/bin/bash

docker ps | awk '{print $1}' | grep -v CON | xargs docker stop
docker ps | awk '{print $1}' | grep -v CON | xargs docker kill
ps aux | grep Service | awk '{print $2}' | xargs sudo kill -9
