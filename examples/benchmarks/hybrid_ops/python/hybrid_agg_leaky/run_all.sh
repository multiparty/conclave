#!/bin/bash
PARTY_ID=$1
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
bash ${DIR}/run.sh "${PARTY_ID}" "hybrid_agg_data/10"
bash ${DIR}/run.sh "${PARTY_ID}" "hybrid_agg_data/100"
#for size in 10 100 1000;
#do
#    bash run.sh ${PARTY_ID} hybrid_agg_data/${size};
#done