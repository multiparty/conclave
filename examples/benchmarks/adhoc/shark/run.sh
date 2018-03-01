#!/bin/bash
SIZE=${1}
# TODO inject headers?
cd /mnt/shared/shark/
# clean up
rm -rf /mnt/shared/shark/data/*.csv
# initial input and shuffle
bash input_scripts/initial_input_1.sh ${SIZE}
bash submit_scripts/submit_shuffle_inputs_1.sh
# local index join and encoding
python3 python/index_join_1.py
# first shark join phase
bash input_scripts/indexes_and_flags_2.sh
bash submit_scripts/submit_shark_join_1.sh
# local arranging
python3 python/arrange_2.py
# last step
bash input_scripts/ordering_3.sh
bash submit_scripts/submit_shark_join_2.sh
# done