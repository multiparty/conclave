#!/bin/bash

# util for killing hanging workflow execution
shopt -s extglob
kill_c_procs() {
    pgrep -P $$
    pkill -P $$
    sleep 0.1
    exit 0
}
trap kill_c_procs INT

TEST_SUB_DIR=$1
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"/${TEST_SUB_DIR}
BACKEND=$2
KEEP_FILES=$3

echo ${BACKEND}

if [ -z ${BACKEND} ]; then
    BACKEND=sharemind
fi

# set up data
mkdir -p ${DIR}/data
if [ "$(ls -A ${DIR}/data)" ]; then
    rm ${DIR}/data/*.csv
else
    :
fi

# data needs custom pre-processing
if [ "$(ls -A ${DIR}/prep.py)" ]; then
    python3 ${DIR}/prep.py ${DIR}/input_data ${DIR}/data
else
	cp $DIR/input_data/*.csv ${DIR}/data/
fi


# run python workflow to generate expected results
python ${DIR}/simple.py 1

if [ ${BACKEND} == "sharemind" ]; then
    declare -a PARTIES=(1 2 3)
else
    declare -a PARTIES=(1 2)
fi

# run real workflow
for i in "${PARTIES[@]}";
do
    python ${DIR}/real.py ${i} ${BACKEND} &
done
wait

# verify results
python ${DIR}/check.py ${DIR}/data/expected.csv ${DIR}/data/actual_open.csv

# clean up again
if [ -z ${KEEP_FILES} ]; then
    rm ${DIR}/data/*.csv
else
    echo "Keeping Files"
fi
