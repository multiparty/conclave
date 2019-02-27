#!/bin/bash

write_data () {
sudo mkdir /mnt/bench/$1
sudo python3 gen_util.py /mnt/bench/$1/in1 4 $1 $1 'a,b,c,d'
}

sizes=( 1500 3000 9000 30000 150000 300000 )

sudo mkdir /mnt/bench

for i in "${sizes[@]}"
do
  :
  write_data $i
done