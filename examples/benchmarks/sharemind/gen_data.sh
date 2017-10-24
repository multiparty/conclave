#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

SIZES=(10 100 1000 5000 10000 100000)
PLAYERS=(1 2 3)

for SIZE in ${SIZES[@]}; do
    for PLAYER in ${PLAYERS[@]}; do
        python3 ${DIR}/../../gen_util.py /tmp/shared/$SIZE/in$PLAYER.csv 2 $SIZE "a,b"
    done
done
