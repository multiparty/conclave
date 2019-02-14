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
    OFF=1
    PREFIX=left
else
    OFF=$((DIST - OVER + 1))
    PREFIX=right
fi

LOWER=${OFF}
UPPER=$((DIST + OFF))
echo ${DIST} ${OVER} ${LOWER} ${UPPER}

OUT=/mnt/shared/aspirin_data/conclave/${SIZE}/
python3 data_gen.py -n ${SIZE} -l ${LOWER} -u ${UPPER} -r 0.2 -o ${OUT} -m "medication" -s $((PARTY + 10)) -f "${PREFIX}_medication.csv"
python3 data_gen.py -n ${SIZE} -l ${LOWER} -u ${UPPER} -r 0.2 -o ${OUT} -m "diagnosis" -s $((PARTY + 20)) -f "${PREFIX}_diagnosis.csv"
