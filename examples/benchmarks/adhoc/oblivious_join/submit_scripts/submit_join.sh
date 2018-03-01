#!/bin/bash
cd /mnt/shared/
./sm_compile_to_remote.sh /mnt/shared/oblivious_join/secrec/join.sc
./Submitter --bytecode join.sb --rels-meta joined:3 --output-path /mnt/shared/oblivious_join/data
