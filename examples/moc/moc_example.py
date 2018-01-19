"""
Simple example workflow for MOC deployment of Conclave
"""
import conclave.lang as sal
from conclave.utils import *
from conclave.codegen.spark import SparkConfig
from conclave import CodeGenConfig
from conclave import generate_and_dispatch
import sys


def protocol():
    """
    Define inputs and operations to be performed between them.
    """

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
    # NOTE: input file names will correspond to the 0th arg of each create call ("in1", "in2", etc.)
    in1 = sal.create("in1", cols_in_a, {1})
    in2 = sal.create("in2", cols_in_b, {1})
    in3 = sal.create("in3", cols_in_c, {1})

    # operate on columns
    # join in1 & in2 over the column 'a', name output relation 'join1'
    join1 = sal.join(in1, in2, 'join1', ['a'], ['a'])
    join2 = sal.join(join1, in3, 'join2', ['a'], ['a'])

    # collect leaf node
    out = sal.collect(join2, 1)

    # return root nodes
    return {in1, in2, in3}


if __name__ == "__main__":

    if len(sys.argv) < 5:
        print("usage: moc_example.py <party_id> <HDFS master node:port> <HDFS root dir> <Spark master url>")
        sys.exit(1)

    pid = int(sys.argv[1])
    hdfs_namenode = sys.argv[2]
    hdfs_root = sys.argv[3]
    spark_master_url = sys.argv[4]

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
        mpc_frameworks=["sharemind"],
        local_frameworks=["spark"]
    )

