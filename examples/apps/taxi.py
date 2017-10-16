import salmon.dispatch
import salmon.net
import salmon.lang as sal
from salmon.codegen import CodeGenConfig
from salmon.codegen.sharemind import SharemindCodeGenConfig
from salmon import codegen
from salmon.utils import *
import sys

def taxi(config, spark_master, sharemind_peer):

    def protocol():
        colsIn1 = [
            defCol("companyID", "INTEGER", [1]),
            defCol("price", "INTEGER", [1])
        ]
        in1 = sal.create("green1", colsIn1, set([1]))
        colsIn2 = [
            defCol("companyID", "INTEGER", [2]),
            defCol("price", "INTEGER", [2])
        ]
        in2 = sal.create("green2", colsIn2, set([2]))
        colsIn3 = [
            defCol("companyID", "INTEGER", [3]),
            defCol("price", "INTEGER", [3])
        ]
        in3 = sal.create("green3", colsIn3, set([3]))

        cab_data = sal.concat([in1, in2, in3], "cab_data")

        selected_input = sal.project(
            cab_data, "selected_input", ["companyID", "price"])
        local_rev = sal.aggregate(selected_input, "local_rev", [
                                  "companyID"], "price", "+", "local_rev")
        scaled_down = sal.divide(
            local_rev, "scaled_down", "local_rev", ["local_rev", 1])
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

        # return root nodes
        return set([in1, in2, in3])

    jobqueue = codegen(protocol, config)
    print(jobqueue)

    salmon.dispatch.dispatch_all(spark_master, sharemind_peer, jobqueue)


if __name__ == "__main__":

    if len(sys.argv) < 5:
        print("usage: taxi.py <party ID> <HDFS master node:port> <HDFS root dir> <Spark master url>")
        sys.exit(1)

    pid = int(sys.argv[1])
    hdfs_namenode = sys.argv[2]
    hdfs_root = sys.argv[3]
    spark_master_url = sys.argv[4]

    workflow_name = "job-" + str(pid)
    config = {
        "name": "taxi",
        "pid": pid,
        "delimiter": ",",
        "code_path": "/mnt/shared/" + workflow_name,
        "input_path": "hdfs://{}/{}/taxi".format(hdfs_namenode, hdfs_root),
        "output_path": "hdfs://{}/{}/taxi".format(hdfs_namenode, hdfs_root),
    }
    sm_cg_config = SharemindCodeGenConfig(workflow_name, "/mnt/shared")
    codegen_config = CodeGenConfig(
        workflow_name).with_sharemind_config(sm_cg_config)
    codegen_config.code_path = "/mnt/shared/" + workflow_name
    codegen_config.input_path = "hdfs://{}/{}/taxi".format(hdfs_namenode, hdfs_root)
    codegen_config.output_path = "hdfs://{}/{}/taxi".format(hdfs_namenode, hdfs_root)
    codegen_config.pid = pid
    codegen_config.name = workflow_name

    sharemind_config = {
        "pid": pid,
        "parties": {
            1: {"host": "ca-spark-node-0", "port": 9001},
            2: {"host": "cb-spark-node-0", "port": 9002},
            3: {"host": "cc-spark-node-0", "port": 9003}
        }
    }
    sm_peer = salmon.net.setup_peer(sharemind_config)

    taxi(codegen_config, spark_master_url, sm_peer)
