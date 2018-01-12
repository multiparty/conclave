"""
Simple example workflow for MOC deployment of Conclave
"""
import salmon.lang as sal
from salmon.utils import *
from salmon.codegen.spark import SparkConfig
from salmon import CodeGenConfig
from salmon import generate_and_dispatch
import sys


def protocol():

    # define input columns
    cols_in_a = [
        defCol('a', 'INTEGER', [1]),
        defCol('b', 'INTEGER', [1]),
    ]
    cols_in_b = [
        defCol('a', 'INTEGER', [1]),
        defCol('c', 'INTEGER', [1]),
    ]
    cols_in_c = [
        defCol('a', 'INTEGER', [1]),
        defCol('d', 'INTEGER', [1])
    ]

    # instantiate input columns
    in1 = sal.create("in1", cols_in_a, {1})
    in2 = sal.create("in2", cols_in_b, {1})
    in3 = sal.create("in3", cols_in_c, {1})

    # operate on columns
    join1 = sal.join(in1, in2, 'join1', ['a'], ['a'])
    join2 = sal.join(join1, in3, 'join2', ['a'], ['a'])
    out = sal.collect(join2, 1)

    # return root nodes
    return {[in1, in2, in3]}


if __name__ == "__main__":

    if len(sys.argv) < 5:
        print("usage: moc_example.py <HDFS master node:port> <HDFS root dir> <Spark master url> <party_id>")
        sys.exit(1)

    hdfs_namenode = sys.argv[1]
    hdfs_root = sys.argv[2]
    spark_master_url = sys.argv[3]
    pid = sys.argv[4]

    workflow_name = "job-" + str(pid)

    network_config = {
        "pid": pid,
        "parties": {
            1: {"host": "ca-spark-node-0", "port": 9001},
            2: {"host": "cb-spark-node-0", "port": 9002},
            3: {"host": "cc-spark-node-0", "port": 9003}
        }
    }

    # init spark config
    spark_config = SparkConfig(spark_master_url)

    # add spark config and network config to master config
    conclave_config = CodeGenConfig(workflow_name) \
        .with_spark_config(spark_config)
    conclave_config.with_network_config(network_config)

    # add code path, input directory path, output directory path, pid, and name
    conclave_config.code_path = "/mnt/shared/" + workflow_name
    conclave_config.input_path = "hdfs://{}/{}/taxi".format(
        hdfs_namenode, hdfs_root)
    conclave_config.output_path = "hdfs://{}/{}/taxi".format(
        hdfs_namenode, hdfs_root)
    conclave_config.pid = pid
    conclave_config.name = workflow_name

    # run the protocol
    generate_and_dispatch(
        protocol,
        conclave_config,
        mpc_frameworks=None,
        local_frameworks=["spark"]
    )

