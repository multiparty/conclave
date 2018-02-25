#!/bin/bash
export PYTHONPATH=${HOME}/conclave
CC=${HOME}/conclave
for size in 10 100 1000 10000 100000 1000000;
do
    for party in 1 2 3;
        do
            OUT=/mnt/shared/hybrid_agg_data/${size}/in${party}.csv
            python3 ${CC}/examples/gen_util.py ${OUT} 2 ${size} ${size} ${party} 0
        done
done
