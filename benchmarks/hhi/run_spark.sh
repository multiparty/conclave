#!/bin/bash

PARTY_ID=$1
SIZE=$2
HDFS_ROOT_DIR=$3/${SIZE}
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [ -z ${HDFS_ROOT_DIR} ]
then
    HDFS_ROOT_DIR=/user/ubuntu/
fi

hadoop fs -rm -r ${HDFS_ROOT_DIR}/local_rev*
hadoop fs -rm -r ${HDFS_ROOT_DIR}/scaled_down*
hadoop fs -rm -r ${HDFS_ROOT_DIR}/hhi_open*

# run query
time python ${DIR}/workload.py ${PARTY_ID} ${HDFS_ROOT_DIR} spark
