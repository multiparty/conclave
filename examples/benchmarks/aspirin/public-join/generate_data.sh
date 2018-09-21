#!/bin/bash

for size in 10 100 1000 10000 100000 500000;
do
    OUT=/mnt/shared/aspirin_data/${size}/;
    python3 gen_data.py -n ${size} -d ${size} -r 0.2 -o ${OUT};
done
