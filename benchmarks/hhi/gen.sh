#!/bin/bash

PARTY=${1}
SIZE=${2}

OUT=/mnt/shared/hhi_data/${SIZE}/
mkdir -p ${OUT}

FN="in$PARTY.csv"

python3 data_gen.py --num_rows ${SIZE} --party ${PARTY} --output ${OUT} --file_name ${FN} --seed ${PARTY}
