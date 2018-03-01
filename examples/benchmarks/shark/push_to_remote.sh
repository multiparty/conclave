#!/bin/bash
KEY_PATH=${1}
IP=${2}
scp -i ${KEY_PATH} -r ${HOME}/Desktop/work/conclave/examples/benchmarks/shark ubuntu@${IP}:/mnt/shared/