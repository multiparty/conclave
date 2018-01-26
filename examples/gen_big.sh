#!/bin/bash

sudo mkdir /mnt/bench/150000000

sudo python3 gen_util.py /mnt/bench/150000000/in1 2 150000000 150000000 'a,b'

hadoop fs -put /mnt/bench/150000000 ~/bench

sudo rm -r /mnt/bench/150000000
