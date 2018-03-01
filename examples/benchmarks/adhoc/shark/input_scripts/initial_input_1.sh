#!/bin/bash
SIZE=${1}
cd /mnt/shared
CSVImporter --mode overwrite --csv /mnt/shared/hybrid_join_data/${SIZE}/in1.csv --model /mnt/shared/shark/schemas/in1.xml --separator c --force
CSVImporter --mode overwrite --csv /mnt/shared/hybrid_join_data/${SIZE}/in2.csv --model /mnt/shared/shark/schemas/in2.xml --separator c --force
