#!/bin/bash
SIZE=${1}
cd /mnt/shared
CSVImporter --mode overwrite --csv /mnt/shared/ssn_data/${SIZE}/govreg.csv --model /mnt/shared/ssn_sharemind/schemas/govreg.xml --separator c --force
CSVImporter --mode overwrite --csv /mnt/shared/ssn_data/${SIZE}/company0.csv --model /mnt/shared/ssn_sharemind/schemas/company0.xml --separator c --force
CSVImporter --mode overwrite --csv /mnt/shared/ssn_data/${SIZE}/company1.csv --model /mnt/shared/ssn_sharemind/schemas/company1.xml --separator c --force
