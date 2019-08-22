#!/bin/bash

SIZE=${1}

OUT=/mnt/shared/ssn_data/${SIZE}/
mkdir -p ${OUT}
python data_gen.py --norealistic --scale ${SIZE} --output ${OUT}
