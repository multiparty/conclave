#!/bin/bash
cd /mnt/shared
CSVImporter --mode overwrite --csv /mnt/shared/shark/data/in1_encoded.csv --model /mnt/shared/shark/schemas/in1_encoded.xml --separator c --force
CSVImporter --mode overwrite --csv /mnt/shared/shark/data/in2_encoded.csv --model /mnt/shared/shark/schemas/in2_encoded.xml --separator c --force
CSVImporter --mode overwrite --csv /mnt/shared/shark/data/num_lookups.csv --model /mnt/shared/shark/schemas/num_lookups.xml --separator c --force
