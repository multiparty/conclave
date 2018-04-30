#!/bin/bash

write_data () {
sudo mkdir /mnt/bench/$1
sudo python3 gen_util.py /mnt/bench/$1/in1 2 $1 $1 'a,b'
}

sizes=( 2 4 8 16 32 64 128 256 512 1028 2056 )

sudo mkdir /mnt/bench

for i in "${sizes[@]}"
do
  :
  write_data $i
done