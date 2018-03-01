#!/bin/bash
SIZE=${1}
cd /mnt/shared/ssn_sharemind/
# clean up
rm -rf /mnt/shared/ssn_sharemind/data/*.csv
# initial input and shuffle
docker exec -ti sharemind-client sh -c "cd /mnt/shared/ && bash ssn_sharemind/input_scripts/input.sh $SIZE"
docker exec -ti sharemind-client sh -c "export LD_LIBRARY_PATH=/usr/local/sharemind/lib/ && cd /mnt/shared/ && bash ssn_sharemind/submit_scripts/submit_join.sh"
# done