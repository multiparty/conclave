#!/bin/bash
SIZE=${1}
cd /mnt/shared/ssn_hybrid/
# clean up
rm -rf /mnt/shared/ssn_hybrid/data/*.csv
# initial input and shuffle
docker exec -ti sharemind-client sh -c "cd /mnt/shared/ && bash ssn_hybrid/input_scripts/initial_input_1.sh $SIZE"
docker exec -ti sharemind-client sh -c "export LD_LIBRARY_PATH=/usr/local/sharemind/lib/ && cd /mnt/shared/ && bash ssn_hybrid/submit_scripts/submit_shuffle_inputs_1.sh"
# local index join and encoding
python3 python/index_join_1.py
# first ssn_hybrid join phase
docker exec -ti sharemind-client sh -c "cd /mnt/shared/ && bash ssn_hybrid/input_scripts/indexes_and_flags_2.sh"
docker exec -ti sharemind-client sh -c "export LD_LIBRARY_PATH=/usr/local/sharemind/lib/ && cd /mnt/shared/ && bash ssn_hybrid/submit_scripts/submit_shark_join_1.sh"
# local arranging
python3 python/arrange_2.py
# last step
docker exec -ti sharemind-client sh -c "cd /mnt/shared/ && bash ssn_hybrid/input_scripts/ordering_3.sh"
docker exec -ti sharemind-client sh -c "export LD_LIBRARY_PATH=/usr/local/sharemind/lib/ && cd /mnt/shared/ && bash ssn_hybrid/submit_scripts/submit_shark_join_2.sh"
# done