import conclave.lang as sal
import conclave.dispatch as dis
from conclave.comp import dag_only
from conclave.utils import *
from conclave.codegen import spark
from conclave.config import CodeGenConfig, SparkConfig
import sys


def ssn(namenode, root, f_size, master_url):

    @dag_only
    def protocol():

        # define inputs
        colsInA = [
            defCol("a", "INTEGER", [2]),
            defCol("b", "INTEGER", [2]),
        ]
        in1 = sal.create("govreg", colsInA, {2})

        colsInB = [
            defCol("a", "INTEGER", [2]),
            defCol("d", "INTEGER", [2])
        ]
        in2 = sal.create("company0", colsInB, {2})

        colsInC = [
            defCol("a", "INTEGER", [2]),
            defCol("d", "INTEGER", [2])
        ]
        in3 = sal.create("company1", colsInC, {2})

        right_rel = sal.concat([in2, in3], 'cld')

        joined = sal.join(in1, right_rel, "joined", ["a"], ["a"])

        agg = sal.aggregate(joined, "agg", ["b"], "d", "+", "d")

        out = sal.collect(agg, 2)

        return set([in1, in2, in3])

    dag = protocol()

    spark_config = SparkConfig(master_url)

    config = CodeGenConfig('ssn_spark_{}'.format(f_size)).with_spark_config(spark_config)

    config.code_path = "/mnt/shared/" + config.name
    config.input_path = "hdfs://{}/{}/{}" \
        .format(namenode, root, f_size)
    config.output_path = "hdfs://{}/{}/ssn_sp{}" \
        .format(namenode, root, f_size)

    cg = spark.SparkCodeGen(config, dag)
    job = cg.generate(config.name, config.output_path)
    job_queue = [job]

    dis.dispatch_all(config, None, job_queue)


if __name__ == "__main__":

    hdfs_namenode = sys.argv[1]
    hdfs_root = sys.argv[2]
    # configurable benchmark size
    filesize = sys.argv[3]
    spark_master_url = sys.argv[4]

    ssn(hdfs_namenode, hdfs_root, filesize, spark_master_url)
