#!/bin/bash
PARTY_ID=$1
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
for size in 10 100 1000 10000;
do
   bash run.sh ${PARTY_ID} aspirin_data/${size};
done
