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
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"/${TEST_SUB_DIR}/obliv-c/
# export PYTHONPATH=${PYTHONPATH}:/${HOME}/Desktop/conclave

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
	cp $DIR/input_data/*.csv $DIR/data/
fi


# run python workflow to generate expected results
python3.5 ${DIR}/simple.py 1

# run real workflow
for i in 1 2;
do
    python3 ${DIR}/real.py ${i} &
done
wait

# verify results
python3 ${DIR}/check.py ${DIR}/data/expected.csv ${DIR}/data/actual_open.csv

# clean up again
rm ${DIR}/data/*.csv
