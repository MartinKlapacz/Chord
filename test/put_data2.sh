#!/bin/bash

PORT=5501

for i in {50..100}
do
   python dht_client.py -a 127.0.0.1 -p $PORT -s -k $RANDOM -d hello$RANDOM
done
