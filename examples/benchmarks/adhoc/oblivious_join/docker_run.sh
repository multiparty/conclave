#!/bin/bash
SIZE=${1}
cd /mnt/shared/oblivious_join/
# clean up
rm -rf /mnt/shared/oblivious_join/data/*.csv
# initial input and shuffle
docker exec -ti sharemind-client sh -c "cd /mnt/shared/ && bash oblivious_join/input_scripts/input.sh $SIZE"
docker exec -ti sharemind-client sh -c "export LD_LIBRARY_PATH=/usr/local/sharemind/lib/ && cd /mnt/shared/ && bash oblivious_join/submit_scripts/submit_join.sh"
# done