#works on "Ubuntu 20.04.6 LTS" focal, github workspace https://github.com/github/codespaces-blank
wget https://github.com/TA-Lib/ta-lib/releases/download/v0.6.3/ta-lib-0.6.3-src.tar.gz -O ./TA_Lib-0.6.3.tar.gz
tar -xzf TA_Lib-0.6.3.tar.gz
cd ta-lib-0.6.3
./configure --prefix=/usr
make
sudo make install
cd ..
pip install TA-Lib