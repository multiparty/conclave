#!/bin/bash
SIZE=${1}
cd /mnt/shared
CSVImporter --mode overwrite --csv /mnt/shared/ssn_data_by_sizes/${SIZE}/govreg.csv --model /mnt/shared/ssn_hybrid/schemas/govreg.xml --separator c --force
CSVImporter --mode overwrite --csv /mnt/shared/ssn_data_by_sizes/${SIZE}/company0.csv --model /mnt/shared/ssn_hybrid/schemas/company0.xml --separator c --force
CSVImporter --mode overwrite --csv /mnt/shared/ssn_data_by_sizes/${SIZE}/company1.csv --model /mnt/shared/ssn_hybrid/schemas/company1.xml --separator c --force
