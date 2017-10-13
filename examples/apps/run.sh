hadoop fs -rm -r /home/ubuntu/taxi-out/
python3 taxi.py $1 $2-spark-node-0:8020 /home/ubuntu spark://$2-spark-node-0:7077
