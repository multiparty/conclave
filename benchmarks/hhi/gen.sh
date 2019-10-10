#!/bin/bash

PARTY=${1}
SIZE=${2}
HDFS_ROOT=${3}

OUT=/mnt/shared/hhi_data/${SIZE}/
mkdir -p ${OUT}

FN="in$PARTY.csv"

python data_gen.py --num_rows ${SIZE} --party ${PARTY} --output ${OUT} --file_name ${FN} --seed ${PARTY}

# also persist to HDFS if user specified an HDFS dir
if [ ! -z ${HDFS_ROOT} ]
then
    hadoop fs -mkdir -p ${HDFS_ROOT}/${SIZE}/
    hadoop fs -put -f ${OUT}/${FN} ${HDFS_ROOT}/${SIZE}/${FN}
fi
