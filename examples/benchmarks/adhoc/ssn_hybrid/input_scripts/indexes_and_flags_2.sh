#!/bin/bash
cd /mnt/shared
CSVImporter --mode overwrite --csv /mnt/shared/ssn_hybrid/data/in1_encoded.csv --model /mnt/shared/ssn_hybrid/schemas/in1_encoded.xml --separator c --force
CSVImporter --mode overwrite --csv /mnt/shared/ssn_hybrid/data/in2_encoded.csv --model /mnt/shared/ssn_hybrid/schemas/in2_encoded.xml --separator c --force
CSVImporter --mode overwrite --csv /mnt/shared/ssn_hybrid/data/num_lookups.csv --model /mnt/shared/ssn_hybrid/schemas/num_lookups.xml --separator c --force
