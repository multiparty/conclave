#!/bin/bash
cd /mnt/shared/
./sm_compile_to_remote.sh /mnt/shared/shark/secrec/shark_join_1.sc 
./Submitter --bytecode shark_join_1.sb --rels-meta in1_indexes_and_flags:2 --rels-meta in2_indexes_and_flags:2 --output-path /mnt/shared/shark/data
