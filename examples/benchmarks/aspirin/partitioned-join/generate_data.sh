#!/bin/bash

PARTY=${1}
if (( ${PARTY} == 1 ))
then
    PREFIX=left
else
    PREFIX=right
fi

PARTY=${1}
OUT=/mnt/shared/aspirin_data/part_join/;
python3 data_gen.py -n 100 -l 90 -u 190 -r 0.2 -o ${OUT} -m "medication" -s ${PARTY} -f "${PREFIX}_medication.csv" -q;
python3 data_gen.py -n 200 -l 90 -u 190 -r 0.2 -o ${OUT} -m "diagnosis" -s ${PARTY} -f "${PREFIX}_diagnosis.csv" -q;
