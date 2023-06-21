#!/bin/bash

PORT=5501

for i in {100..1000}
do
   python dht_client.py -a 127.0.0.1 -p $PORT -s -k $i -d hello
done
