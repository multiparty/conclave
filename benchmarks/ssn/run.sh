#!/bin/bash

PARTY_ID=$1
SIZE=$2
DATA_ROOT_DIR=comorb_data/conclave/${SIZE}
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# clean up data first
find /mnt/shared/${DATA_ROOT_DIR} \
    -type f -not -name left_diagnosis.csv -not -name right_diagnosis.csv \
    -print0 | xargs -0 rm --;

# run query
time python3 ${DIR}/workload.py ${PARTY_ID} ${DATA_ROOT_DIR}