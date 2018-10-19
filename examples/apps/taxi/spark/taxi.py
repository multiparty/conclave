import sys
import conclave.lang as sal
from conclave.config import CodeGenConfig, SparkConfig
from conclave.codegen.spark import SparkCodeGen
from conclave.utils import *
from conclave.comp import dag_only


def generate(dag, cfg):

    cg = SparkCodeGen(cfg, dag)

    actual = cg.generate('code', '/tmp')


@dag_only
def protocol():

    cols_in_1 = [
        defCol("companyID", "INTEGER", [1]),
        defCol("price", "INTEGER", [1])
    ]
    cols_in_2 = [
        defCol("companyID", "INTEGER", [1]),
        defCol("price", "INTEGER", [1])
    ]
    cols_in_3 = [
        defCol("companyID", "INTEGER", [1]),
        defCol("price", "INTEGER", [1])
    ]

    yellow1 = sal.create("yellow1", cols_in_1, {1})
    yellow2 = sal.create("yellow2", cols_in_2, {1})
    yellow3 = sal.create("yellow3", cols_in_3, {1})
    
    cab_data = sal.concat([yellow1, yellow2, yellow3], "cab_data")

    selected_input = sal.project(
        cab_data, "selected_input", ["companyID", "price"])
    local_rev = sal.aggregate(selected_input, "local_rev", [
        "companyID"], "price", "+", "local_rev")
    scaled_down = sal.divide(
        local_rev, "scaled_down", "local_rev", ["local_rev", 1000])
    first_val_blank = sal.multiply(
        scaled_down, "first_val_blank", "companyID", ["companyID", 0])
    local_rev_scaled = sal.multiply(
        first_val_blank, "local_rev_scaled", "local_rev", ["local_rev", 100])
    total_rev = sal.aggregate(first_val_blank, "total_rev", [
        "companyID"], "local_rev", "+", "global_rev")
    local_total_rev = sal.join(local_rev_scaled, total_rev, "local_total_rev", [
        "companyID"], ["companyID"])
    market_share = sal.divide(local_total_rev, "market_share", "local_rev", [
        "local_rev", "global_rev"])
    market_share_squared = sal.multiply(market_share, "market_share_squared", "local_rev",
                                        ["local_rev", "local_rev", 1])
    hhi = sal.aggregate(market_share_squared, "hhi", [
        "companyID"], "local_rev", "+", "hhi")

    sal.collect(hhi, 1)

    return {yellow1, yellow2, yellow3}


if __name__ == "__main__":

    if len(sys.argv) < 5:
        print("usage: taxi.py <party ID> <HDFS master node:port> <HDFS root dir> <Spark master url>")
        sys.exit(1)

    pid = int(sys.argv[1])
    hdfs_namenode = sys.argv[2]
    hdfs_root = sys.argv[3]
    spark_master_url = sys.argv[4]

    workflow_name = "job-" + str(pid)
    spark_config = SparkConfig(spark_master_url)
    conclave_config = CodeGenConfig(workflow_name) \
        .with_spark_config(spark_config)
    conclave_config.code_path = "/tmp/" + workflow_name
    conclave_config.input_path = "hdfs://{}/{}/taxi".format(
        hdfs_namenode, hdfs_root)
    conclave_config.output_path = "hdfs://{}/{}/taxi".format(
        hdfs_namenode, hdfs_root)
    conclave_config.pid = pid
    conclave_config.name = workflow_name

    dag = protocol()

    generate(dag, conclave_config)
