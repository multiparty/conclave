#!/bin/bash
for size in 10 100 1000 10000 100000;
do
    for party in 1 2 3;
        do
            OUT=/mnt/shared/agg_data/${size}/in${party}.csv
            python3 ../../../../gen_util.py ${OUT} 2 ${size} ${size} "a","b" 0
        done
done
