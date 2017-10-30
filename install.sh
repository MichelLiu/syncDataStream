#!/bin/bash

yum install -y mysql-devel python-devel

pip install pytz
pip install flask
pip install pymongo
pip install pinyin
pip install threadpool
pip install requests
pip install pinyin

cd dependents

cd DBUtils-1.2
python setup.py install
cd ../

cd mns_python_sdk
python setup.py install
cd ../

cd MySQL-python-1.2.5
python setup.py install
cd ../

cd ../