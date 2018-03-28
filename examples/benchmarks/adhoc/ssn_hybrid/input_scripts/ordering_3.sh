#!/bin/bash 
cd /mnt/shared 
CSVImporter --mode overwrite --csv /mnt/shared/ssn_hybrid/data/in1_shark_indexes.csv --model /mnt/shared/ssn_hybrid/schemas/in1_shark_indexes.xml --separator c --force
CSVImporter --mode overwrite --csv /mnt/shared/ssn_hybrid/data/in1_original_ordering.csv --model /mnt/shared/ssn_hybrid/schemas/in1_original_ordering.xml --separator c --force
CSVImporter --mode overwrite --csv /mnt/shared/ssn_hybrid/data/in2_shark_indexes.csv --model /mnt/shared/ssn_hybrid/schemas/in2_shark_indexes.xml --separator c --force
CSVImporter --mode overwrite --csv /mnt/shared/ssn_hybrid/data/in2_original_ordering.csv --model /mnt/shared/ssn_hybrid/schemas/in2_original_ordering.xml --separator c --force
