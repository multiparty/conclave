#!/bin/bash

PARTY=${1}
SIZE=${2}

if (( ${SIZE} > 500000))
then
    DIST=500000
else
    DIST=${SIZE}
fi

OVER=$((2 * DIST / 100))

if (( ${PARTY} == 1 ))
then
    OFF=1;
else
    OFF=$((DIST - OVER + 1))
fi

LOWER=${OFF}
UPPER=$((DIST + OFF))
echo ${DIST} ${OVER} ${LOWER} ${UPPER}

OUT=/mnt/shared/aspirin_data/smcql/${SIZE}/
python data_gen.py -n ${SIZE} -l ${LOWER} -u ${UPPER} -r 0.2 -o ${OUT} -m "medication" -s $((PARTY + 10)) -f "medications.csv" -q
python data_gen.py -n ${SIZE} -l ${LOWER} -u ${UPPER} -r 0.2 -o ${OUT} -m "diagnosis" -s $((PARTY + 20)) -f "diagnoses.csv" -q
