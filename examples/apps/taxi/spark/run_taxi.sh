#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

export PYTHONPATH=${HOME}/conclave
export PATH=${PATH}:${HOME}/spark-2.2.0-bin-hadoop2.6/bin

run_bench () {
    sudo python3 gen_data.py /mnt/taxi/bench/yellow1.csv $1 1
    sudo python3 gen_data.py /mnt/taxi/bench/yellow2.csv $1 2
    sudo python3 gen_data.py /mnt/taxi/bench/yellow3.csv $1 3
    hadoop fs -put /mnt/taxi/bench/* ~/taxi/

    sudo rm -r /mnt/taxi/bench/*

    start=`date +%s`
    spark-submit --conf "spark.local.dir=/mnt/hdfs/spark-tmp" --master spark://cc-spark-node-0:7077 /tmp/job-1/code/workflow.py hdfs://cc-spark-node-0:8020//home/ubuntu/taxi/yellow1 hdfs://cc-spark-node-0:8020//home/ubuntu/taxi/yellow2 hdfs://cc-spark-node-0:8020//home/ubuntu/taxi/yellow3 hdfs://cc-spark-node-0:8020//home/ubuntu/taxi/hhi
    end=`date +%s`
    runtime=$((end-start))
    echo "$1 -- $runtime" >> /tmp/runtime.txt

    hadoop fs -rm -r /home/ubuntu/taxi/*
}

sizes=( 10 100 1000 )

for i in "${sizes[@]}"
do
  :
  run_bench $i
done