# Running

First generate some data for each party:

```bash
bash gen.sh 1 100
bash gen.sh 2 100
bash gen.sh 3 100
```

This generates 100 row input relations for three parties (stored under `/mnt/shared/hhi_data/100/in{1|2|3}.csv`).

If you want to use Spark, the files need to be stored in HDFS. Run:

```bash
bash gen.sh 1 100 HDFS_ROOT
```

where `HDFS_ROOT` specifies the directory on HDFS under which the files are to be stored (for example `/user/ubuntu/`).

To run with Python as a local backend, for each party use (in separate terminals, or from different machines depending on your setup):

```bash
bash run.sh 1 100
bash run.sh 2 100
bash run.sh 3 100
```

Spark is also supported. Tested only with `Hadoop 2.6.0` and `Spark 2.2.0`. To use Spark, configure Spark master url etc. in `workload.py` first (if your spark master and HDFS namenode are mapped to a different host than your Conclave node).

Then run using `bash run_spark.sh 1 100`. If you're running a Conclave query on a single machine, consider using Spark for only one of the parties and Python for the others; it will make your life easier :)

Note that this benchmark (along with all others) assumes that party 1 is reachable at `ca-spark-node-0`, party 2 at `cb-spark-node-0`, and party 3 at `cc-spark-node-0`. You can modify your `/etc/hosts` file to map IP addresses to host addresses. To map the above to 127.0.0.1 (for a local run) include the following entry in your `/etc/hosts` file:

```bash
127.0.0.1	ca-spark-node-0 cb-spark-node-0 cc-spark-node-0
```

Most likely you already have a mapping for localhost, for example:

```bash
127.0.0.1	localhost
```

In that case, just append the node addresses after `localhost`.

You can also modify the party addresses inside `CodeGenConfig` by updating the `network_config` dict.
