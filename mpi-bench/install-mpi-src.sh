mkdir openmpi
cd openmpi
wget https://www.open-mpi.org/software/ompi/v3.0/downloads/openmpi-3.0.0.tar.gz
tar -xf openmpi-3.0.0.tar.gz
cd openmpi-3.0.0
./configure --prefix=/usr/local --enable-mpi-thread-multiple
sudo make all install
echo 'export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH' >> $HOME/.bashrc
sudo ldconfig

# sudo make uninstall
