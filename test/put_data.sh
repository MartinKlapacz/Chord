#!/bin/bash

PORT=5501

for i in {1..50}
do
   python dht_client.py -a 127.0.0.1 -p $PORT -s -k $i -d hello
done
