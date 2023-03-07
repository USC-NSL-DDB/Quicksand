cd alfalfa

./autogen.sh
./configure
make -j $(nproc)
sudo make install

cd ../bin
./download.sh

cd ../samples
./prepare.sh
