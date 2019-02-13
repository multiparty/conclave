#!/bin/bash

PARTY=${1}
SIZE=${2}

OUT=/mnt/shared/ssn_data/${SIZE}/
python3 data_gen.py --norealistic --scale ${SIZE} --output ${OUT}
