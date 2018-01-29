"""
Simple example workflow for MOC deployment of Conclave
"""
import conclave.lang as sal
from conclave.utils import *
from conclave.config import SharemindCodeGenConfig, SparkConfig, CodeGenConfig, NetworkConfig
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

    pid = int(sys.argv[1])

    net_conf = NetworkConfig(
        ["ca-spark-node-0", "cb-spark-node-0", "cc-spark-node-0"],
        [8020, 8020, 8020],
        1
    )

    spark_conf = SparkConfig("spark://ca-spark-node-0:8020")
    sharemind_conf = SharemindCodeGenConfig("/mnt/data")
    conclave_config = CodeGenConfig("big_job"). \
        with_sharemind_config(sharemind_conf). \
        with_spark_config(spark_conf). \
        with_network_config(net_conf)

    conclave_config.pid = pid

    # run the protocol
    generate_and_dispatch(
        protocol,
        conclave_config,
        mpc_frameworks=["sharemind"],
        local_frameworks=["spark"]
    )

