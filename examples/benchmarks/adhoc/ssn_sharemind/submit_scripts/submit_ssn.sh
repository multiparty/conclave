#!/bin/bash
cd /mnt/shared/
./sm_compile_to_remote.sh /mnt/shared/ssn_sharemind/secrec/ssn.sc
./Submitter --bytecode join.sb --rels-meta joined:3 --output-path /mnt/shared/ssn_sharemind/data
