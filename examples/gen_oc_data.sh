#!/bin/bash

write_data () {
sudo mkdir /mnt/bench/$1
sudo python3 gen_util.py /mnt/bench/$1/in1 4 $1 $1 'a,b'
hadoop fs -put /mnt/bench/$1 ~/bench/
}

sizes=( 10 100 200 300 400 500 600 700 800 900 )

sudo mkdir /mnt/bench

for i in "${sizes[@]}"
do
  :
  write_data $i
done