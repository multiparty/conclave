#!/bin/bash

PARTY=${1}
SIZE=${2}

TOTAL_SIZE=$((SIZE * 2))
DIST=$((10 * TOTAL_SIZE / 100))
OVER=$((20 * DIST / 100))

if (( ${PARTY} == 1 ))
then
    OFF=0;
else
    OFF=$((DIST - OVER))
fi

LOWER=${OFF}
UPPER=$((DIST + OFF))
echo ${DIST} ${OVER} ${LOWER} ${UPPER}

OUT=/mnt/shared/aspirin_data/smcql/${SIZE}/
python3 data_gen.py -n ${SIZE} -l ${LOWER} -u ${UPPER} -r 0.2 -o ${OUT} -m "medication" -s ${PARTY} -f "medications.csv" -q
python3 data_gen.py -n ${SIZE} -l ${LOWER} -u ${UPPER} -r 0.2 -o ${OUT} -m "diagnosis" -s ${PARTY} -f "diagnoses.csv" -q
