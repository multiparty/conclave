#!/bin/bash

write_out () {
start=$(date +%s)
perflock python3 $1 ca-spark-node-0:8020 ~/bench/ $2 spark://ca-spark-node-0:7077
end=$(date +%s)
echo "runtime for $1 with $2 input rows was $((end - start))" >> bench.txt
}

sizes=( 30 300 3000 30000 300000 3000000 30000000 )

op=$1

for i in "${sizes[@]}"
do
  :
  write_out $op.py $i
  hadoop fs -rmr ~/bench/${op}_sp$i
done
