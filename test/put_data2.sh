#!/bin/bash

PORT=5501

for i in {51..100}
do
   python3 dht_client.py -a 127.0.0.1 -p $PORT -s -k $RANDOM -d hello$RANDOM
done
