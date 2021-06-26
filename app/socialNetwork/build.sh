#!/bin/bash

sudo apt-get install libssl-dev libz-dev luarocks -y
sudo apt-get install libmongoc-dev libmemcached-dev -y
sudo apt-get install automake bison flex libtool libssl-dev -y
sudo apt-get install libgtest-dev -y
luarocks install luasocket --local

mkdir build
pushd build

git clone https://github.com/jbeder/yaml-cpp.git
cd yaml-cpp
mkdir build
cd build
cmake -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=true ..
make -j
sudo make install
cd ../..

git clone https://github.com/nlohmann/json.git
cd json
mkdir build
cd build
cmake -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=true ..
make -j
sudo make install
cd ../..

git clone https://github.com/opentracing/opentracing-cpp.git
cd opentracing-cpp
git submodule init
git submodule update
mkdir build
cd build
cmake -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=true ..
make -j
sudo make install
cd ../..

version=0.12.0 && git clone -b $version https://github.com/apache/thrift.git
cd thrift
./bootstrap.sh
./configure
make -j
sudo make install
cd ..

git clone https://github.com/jaegertracing/jaeger-client-cpp.git
cd jaeger-client-cpp
git submodule init
git submodule update
mkdir build
cd build
CXXFLAGS="-Wno-error=deprecated-copy" cmake -DHUNTER_ENABLED=OFF \
	-DBUILD_TESTING=OFF -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=true ..
make -j
sudo make install
cd ../..

git clone https://github.com/arun11299/cpp-jwt
cd cpp-jwt
mkdir build
cd build
cmake ..
make -j
sudo make install
cd ../..

git clone https://github.com/alanxz/rabbitmq-c.git
cd rabbitmq-c
mkdir build
cd build
cmake ..
make -j
sudo make install
cd ../../

git clone https://github.com/alanxz/SimpleAmqpClient
cd SimpleAmqpClient
mkdir build
cd build
cmake ..
make -j
sudo make install
cd ../..

git clone https://github.com/redis/hiredis
cd hiredis
make USE_SSL=1 -j
sudo make USE_SSL=1 install
cd ..

git clone https://github.com/sewenew/redis-plus-plus
cd redis-plus-plus
mkdir build
cd build
cmake -DREDIS_PLUS_PLUS_USE_TLS=ON ..
make -j
sudo sudo make install
make install
cd ../..

git clone https://github.com/CopernicaMarketingSoftware/AMQP-CPP.git
cd AMQP-CPP
mkdir build
cd build
cmake ..
make -j
sudo make install
cd ../..

cmake ..
make -j

popd
