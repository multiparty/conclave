#!/bin/bash

PARTY_ID=$1
SIZE=$2
DATA_ROOT_DIR=ssn_data/${SIZE}
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# clean up data first
find /mnt/shared/${DATA_ROOT_DIR} \
    -type f -not -name govreg.csv -not -name company0.csv -not -name company1.csv \
    -print0 | xargs -0 rm --;

# run query
time python ${DIR}/workload.py ${PARTY_ID} ${DATA_ROOT_DIR}
