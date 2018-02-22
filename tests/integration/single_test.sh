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
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"/$TEST_SUB_DIR
export PYTHONPATH=${HOME}/Desktop/conclave

# set up data
if [ "$(ls -A $DIR/data)" ]; then
    rm $DIR/data/*.csv
else
    :
fi

cp $DIR/input_data/*.csv $DIR/data/

# run python workflow to generate expected results
python3 $DIR/simple.py 1

# run real workflow
for i in {1..3};
do
    python3 $DIR/real.py $i &
done
wait

# verify results
python3 $DIR/check.py $DIR/data/expected.csv $DIR/data/actual_open.csv

# clean up again
rm $DIR/data/*.csv
