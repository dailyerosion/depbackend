set -x
ls /

sudo ln -s `pwd` /opt/depbackend
mkdir _data
sudo ln -s _data /i
mkdir -p /i/0/cli/097x035
cp tests/data/097.50x035.50.cli /i/0/cli/097x035/
