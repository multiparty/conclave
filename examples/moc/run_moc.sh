#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

export PYTHONPATH=${HOME}/conclave
export PATH=${PATH}:${HOME}/spark-2.2.0-bin-hadoop2.6/bin

if [[ $# -lt 2 ]]; then
  echo "usage: run-moc.sh <party ID> <cluster prefix>"
  exit 1
fi

hadoop fs -rmr /home/ubuntu/taxi/join2

python3 ${DIR}/moc_example.py $1 $2-spark-node-0:8020 /home/ubuntu spark://$2-spark-node-0:7077
