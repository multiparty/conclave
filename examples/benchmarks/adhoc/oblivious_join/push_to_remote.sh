#!/bin/bash
KEY_PATH=${1}
IP=${2}
scp -i ${KEY_PATH} -r ${HOME}/Desktop/work/conclave/examples/benchmarks/adhoc/oblivious_join ubuntu@${IP}:/mnt/shared/