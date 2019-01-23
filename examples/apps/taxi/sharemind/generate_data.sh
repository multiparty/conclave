#!/bin/bash
for size in 10 100 1000 10000 100000;
do
    for party in 1 2 3;
        do
            OUT=/mnt/shared/fake_taxi_data/${size}/in${party}.csv
            python3 ../gen_taxi_data.py ${OUT} ${size} ${party} "companyID","price"
        done
done
