#!/bin/bash
PARTY_ID=$1
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export PYTHONPATH=${HOME}/conclave
for size in 10 100 1000;
do
    time bash run.sh ${PARTY_ID} hybrid_agg_data/${size}
done