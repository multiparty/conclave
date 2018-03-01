#!/bin/bash 
cd /mnt/shared 
CSVImporter --mode overwrite --csv /mnt/shared/ssn_hybrid/data/sorted_by_key.csv --model /mnt/shared/ssn_hybrid/schemas/sorted_by_key.xml --separator c --force
CSVImporter --mode overwrite --csv /mnt/shared/ssn_hybrid/data/eq_flags.csv --model /mnt/shared/ssn_hybrid/schemas/eq_flags.xml --separator c --force
