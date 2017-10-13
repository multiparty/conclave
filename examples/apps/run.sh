#!/bin/bash

export PYTHONPATH=${HOME}/salmon
export PATH=${PATH}:${HOME}/spark-2.2.0-bin-hadoop2.6/bin

if [[ $# -lt 2 ]]; then
  echo "usage: run-taxi.sh <party ID> <cluster prefix>"
  exit 1
fi

hadoop fs -rm -r /home/ubuntu/taxi-out/
python3 taxi.py $1 $2-spark-node-0:8020 /home/ubuntu spark://$2-spark-node-0:7077
