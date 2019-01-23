#!/bin/bash

PARTY=${1}
SIZE=${2}

cp /mnt/shared/aspirin_data/smcql/${SIZE}/*.csv conf/workload/testDB/${PARTY}/
sudo su smcql -c 'bash /home/ubuntu/smcql/conf/workload/testDB/create_test_dbs.sh'
