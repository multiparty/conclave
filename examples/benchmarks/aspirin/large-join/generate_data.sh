#!/bin/bash

PARTY=${1}
if (( ${PARTY} == 1 ))
then
    MODE=medication
else
    MODE=diagnosis
fi
OUT=/mnt/shared/aspirin_data/large-query/;
python3 gen_data.py -n 12000 -d 10000 -r 0.2 -o ${OUT} -m ${MODE} -s ${PARTY};
