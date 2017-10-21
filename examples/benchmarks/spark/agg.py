import salmon.lang as sal
import salmon.dispatch as dis
from salmon.comp import dagonly
from salmon.utils import *
from salmon.codegen import CodeGenConfig, spark
import sys


def agg(namenode, root, f_size, master_url):

    @dagonly
    def protocol():

        colsInA = [
            defCol('a', 'INTEGER', [1]),
            defCol('b', 'INTEGER', [1]),
            defCol('c', 'INTEGER', [1]),
            defCol('d', 'INTEGER', [1])
        ]

        in1 = sal.create("in1", colsInA, set([1]))
        agg1 = sal.aggregate(in1, 'agg1', ['a'], 'b', '+', 'b')

        return set([in1])

    dag = protocol()
    config = CodeGenConfig('agg_spark_{}'.format(f_size))

    config.code_path = "/mnt/shared/" + config.name
    config.input_path = "hdfs://{}/{}/{}"\
        .format(namenode, root, f_size)
    config.output_path = "hdfs://{}/{}/agg_sp{}"\
        .format(namenode, root, f_size)

    cg = spark.SparkCodeGen(config, dag)
    job = cg.generate(config.name, config.output_path)
    job_queue = [job]

    dis.dispatch_all(master_url, None, job_queue)


if __name__ == "__main__":

    hdfs_namenode = sys.argv[1]
    hdfs_root = sys.argv[2]
    # configurable benchmark size
    filesize = sys.argv[3]
    spark_master_url = sys.argv[4]

    agg(hdfs_namenode, hdfs_root, filesize, spark_master_url)



