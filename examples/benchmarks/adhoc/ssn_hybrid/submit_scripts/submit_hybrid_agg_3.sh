#!/bin/bash
cd /mnt/shared/
./sm_compile_to_remote.sh /mnt/shared/ssn_hybrid/secrec/hybrid_agg_3.sc
./Submitter --bytecode hybrid_agg_3.sb --rels-meta ssn_hybrid_result:2 --output-path /mnt/shared/ssn_hybrid/data
