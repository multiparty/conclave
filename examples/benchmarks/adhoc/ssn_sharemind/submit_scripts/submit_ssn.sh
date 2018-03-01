#!/bin/bash
cd /mnt/shared/
./sm_compile_to_remote.sh /mnt/shared/ssn_sharemind/secrec/ssn.sc
./Submitter --bytecode ssn.sb --rels-meta ssn_result:2 --output-path /mnt/shared/ssn_sharemind/data
