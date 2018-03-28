#!/bin/bash

write_out () {
start=$(date +%s)
python3 $1 cb-spark-node-0:8020 ~/ssn/ $2 spark://cb-spark-node-0:7077
end=$(date +%s)
echo "runtime for $1 with $2 input rows was $((end - start))" >> bench.txt
}

sizes=( 10 100 1000 10000 100000 )

for i in "${sizes[@]}"
do
  :
  write_out ssn.py $i
  hadoop fs -rmr ~/ssn/ssn_sp$i
done
