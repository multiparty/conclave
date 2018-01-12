#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

export PYTHONPATH=${HOME}/conclave
export PATH=${PATH}:${HOME}/spark-2.2.0-bin-hadoop2.6/bin

if [[ $# -lt 2 ]]; then
  echo "usage: run-taxi.sh <party ID> <cluster prefix>"
  exit 1
fi

hadoop fs -rm -r /home/ubuntu/taxi/local_rev*
hadoop fs -rm -r /home/ubuntu/taxi/scaled_down*
hadoop fs -rm -r /home/ubuntu/taxi/hhi_open*

python3 ${DIR}/taxi.py $1 $2-spark-node-0:8020 /home/ubuntu spark://$2-spark-node-0:7077
