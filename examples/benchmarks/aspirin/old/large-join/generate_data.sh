#!/bin/bash

PARTY=${1}
if (( ${PARTY} == 1 ))
then
    MODE=medication
else
    MODE=diagnosis
fi
OUT=/mnt/shared/aspirin_data/large_join/;
python3 data_gen.py -n 100 -d 100 -r 0.2 -o ${OUT} -m ${MODE} -s ${PARTY} -i 10;
