#!/bin/bash

write_data () {
sudo mkdir /mnt/bench/$1
sudo python3 gen_util.py /mnt/bench/$1/in1 4 $1 $1 'a,b,c,d'
hadoop fs -put /mnt/bench/$1 ~/bench/
}

sizes=( 30 300 3000 30000 300000 3000000 30000000 )

for i in "${sizes[@]}"
do
  :
  write_data $i
done
