import salmon.lang as sal
import salmon.dispatch as dis
from salmon.comp import dagonly
from salmon.utils import *
from salmon.codegen import spark
from salmon import CodeGenConfig
import sys


# top-level method that wraps conclave protocol
def join(namenode, root, master_url):

    # transforms protocol into a DAG that can be passed to codegen
    @dagonly
    def protocol():

        # define input columns
        colsInA = [
            defCol('a', 'INTEGER', [1]),
            defCol('b', 'INTEGER', [1]),
        ]
        colsInB = [
            defCol('a', 'INTEGER', [1]),
            defCol('c', 'INTEGER', [1]),
        ]

        # instantiate input columns
        in1 = sal.create("in1", colsInA, {[1]})
        in2 = sal.create("in2", colsInB, {[1]})

        # operate on columns
        join = sal.join(in1, in2, 'join1', ['a'], ['a'])
        out = sal.collect(join, 1)

        # return root nodes
        return {[in1, in2]}

    # generate dag
    dag = protocol()

    # config setup
    config = CodeGenConfig('join_spark')
    config.code_path = "/mnt/shared/" + config.name
    config.input_path = "hdfs://{}/{}" \
        .format(namenode, root)
    config.output_path = "hdfs://{}/{}/join_out" \
        .format(namenode, root)

    # call Spark codegen directly, rather than the top level codegen
    # class
    cg = spark.SparkCodeGen(config, dag)
    job = cg.generate(config.name, config.output_path)
    job_queue = [job]

    dis.dispatch_all(master_url, None, job_queue)


if __name__ == "__main__":

    hdfs_namenode = sys.argv[1]
    hdfs_root = sys.argv[2]
    spark_master_url = sys.argv[3]

    join(hdfs_namenode, hdfs_root, spark_master_url)

