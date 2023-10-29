sudo apt-get -y install ffmpeg libx264-dev

cd alfalfa

./autogen.sh
./configure X264_LIBS=/lib/x86_64-linux-gnu/libx264.a
make -j $(nproc)
sudo make install

cd ../bin
./prepare.sh

cd ../samples
./prepare.sh
