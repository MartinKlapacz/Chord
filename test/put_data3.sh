#!/bin/bash

PORT=5501

for i in {101..150}
do
   python dht_client.py -a 127.0.0.1 -p $PORT -s -k $i -d hello$RANDOM
done
