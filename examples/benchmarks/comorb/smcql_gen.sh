#!/bin/bash

PARTY=${1}
SIZE=${2}

if (( ${PARTY} == 1 ))
then
    PREFIX=left
else
    PREFIX=right
fi

DIST_KEYS=$((10 * SIZE / 100))

OUT=/mnt/shared/comorb_data/smcql/${SIZE}/
python data_gen.py -n ${SIZE} -d ${DIST_KEYS} -s ${PARTY} -o ${OUT} -f "diagnoses.csv" -q
