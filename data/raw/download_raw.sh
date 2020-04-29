#!/bin/bash

curl -O http://codeandbeer.org/virtual/BigData/Datasets/measures.tgz
curl -O http://codeandbeer.org/virtual/BigData/Datasets/cryptocurrencypricehistory.tgz
curl -O http://codeandbeer.org/virtual/BigData/Datasets/iris.data

echo "Unzipping measures.tgz and cryptocurrencypricehistory.tgz..."
tar zxvf measures.tgz
tar zxvf cryptocurrencypricehistory.tgz
echo "done"

echo -n "Removing uneeded files..."
rm measures.tgz
rm cryptocurrencypricehistory.tgz
echo "done"
