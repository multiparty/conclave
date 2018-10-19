#!/bin/bash
PARTY_ID=${1}
for size in 10 100 1000 10000 100000;
do
   bash run.sh ${PARTY_ID} fake_taxi_data/${size};
done