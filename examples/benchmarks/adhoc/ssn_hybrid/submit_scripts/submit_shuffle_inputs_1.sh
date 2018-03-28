cd /mnt/shared/
./sm_compile_to_remote.sh /mnt/shared/ssn_hybrid/secrec/shuffle_inputs_1.sc && ./Submitter --bytecode shuffle_inputs_1.sb --rels-meta in1_keys:1 --rels-meta in2_keys:1 --output-path /mnt/shared/ssn_hybrid/data/
