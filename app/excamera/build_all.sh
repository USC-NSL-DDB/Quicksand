cd alfalfa

./autogen.sh
./configure
make -j $(nproc)
sudo make install

