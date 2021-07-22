#!/bin/bash

sudo apt-get install libssl-dev libz-dev luarocks -y
sudo apt-get install automake bison flex libtool libssl-dev -y
sudo apt-get install libgtest-dev -y
luarocks install luasocket --local

mkdir build
pushd build

git clone https://github.com/nlohmann/json.git
cd json
mkdir build
cd build
cmake -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=true ..
make -j
sudo make install
cd ../..

cd ..
cd thrift
./bootstrap.sh
./configure --enable-caladanthreads=yes --enable-caladantcp=yes \
            --with-caladan=`pwd`/../../../caladan/  \
            --enable-shared=no --enable-tests=no --enable-tutorial=no
make -j
cd ../build

git clone https://github.com/arun11299/cpp-jwt
cd cpp-jwt
mkdir build
cd build
cmake ..
make -j
sudo make install
cd ../..

cmake ..
make -j

sudo apt-get update
sudo apt install apt-transport-https ca-certificates curl gnupg-agent -y
sudo apt install software-properties-common -y
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
sudo apt update
sudo apt install docker-ce docker-ce-cli containerd.io -y

sudo curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" \
     -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

sudo groupadd docker
sudo usermod -aG docker $USER
exec sudo su -l $USER

popd
