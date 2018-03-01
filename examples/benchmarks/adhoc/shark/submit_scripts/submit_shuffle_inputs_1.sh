cd /mnt/shared/
./sm_compile_to_remote.sh /mnt/shared/shark/secrec/shuffle_inputs_1.sc && ./Submitter --bytecode shuffle_inputs_1.sb --rels-meta in1_keys:1 --rels-meta in2_keys:1 --output-path /mnt/shared/shark/data/
