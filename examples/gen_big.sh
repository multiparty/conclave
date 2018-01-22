#!/bin/bash

sudo mkdir /mnt/taxi/300000000

sudo python3 gen_util.py /mnt/taxi/300000000/in1 2 300000000 300000000 'a,b'

hadoop fs -put /mnt/bench/300000000 ~/bench

sudo rm -r /mnt/taxi/300000000
