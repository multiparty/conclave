#!/bin/bash
KEY_PATH=${1}
IP=${2}
scp -i ${KEY_PATH} -r ${HOME}/Desktop/work/conclave/examples/benchmarks/adhoc/ssn_hybrid ubuntu@${IP}:/mnt/shared/