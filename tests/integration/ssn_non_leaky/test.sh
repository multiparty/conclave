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

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export PYTHONPATH=${HOME}/Desktop/conclave

# set up data
rm $DIR/data/*.csv
cp $DIR/input_data/*.csv $DIR/data/

# run python workflow to generate expected results
python3 simple_ssn.py 1

# run real workflow
for i in 1 2 3;
do
    python3 real_ssn.py $i &
done
wait

# verify results
python3 check.py data/expected.csv data/actual_open.csv
