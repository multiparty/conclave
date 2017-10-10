import salmon.dispatch
import salmon.lang as sal
from salmon import codegen
from salmon.utils import *
import sys

def taxi(config):

    def protocol():
        colsIn1 = [
            defCol("companyID", "INTEGER", [1]),
            defCol("price", "INTEGER", [1])
        ]
        in1 = sal.create("in1", colsIn1, set([1]))
        colsIn2 = [
            defCol("companyID", "INTEGER", [2]),
            defCol("price", "INTEGER", [2])
        ]
        in2 = sal.create("in2", colsIn2, set([2]))
        colsIn3 = [
            defCol("companyID", "INTEGER", [3]),
            defCol("price", "INTEGER", [3])
        ]
        in3 = sal.create("in3", colsIn3, set([3]))

        cab_data = sal.concat([in1, in2, in3], "cab_data")

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
        # dummy projection to force non-mpc subdag
        hhi_only = sal.project(
            hhi, "hhi_only", ["companyID", "hhi"])

        sal.collect(hhi_only, 1)

        # return root nodes
        return set([in1, in2, in3])

    jobqueue = codegen(protocol, config)
    print(jobqueue)

    # XXX(malte): need to pass Sharemind config here
    salmon.dispatch.dispatch_all("spark://ca-spark-node-0:7077", None, jobqueue)


if __name__ == "__main__":

    if len(sys.argv) < 4:
        print("usage: taxi.py <party ID> <HDFS master node:port> <HDFS root dir>")
        sys.exit(1)

    pid = int(sys.argv[1])
    hdfs_namenode = sys.argv[2]
    hdfs_root = sys.argv[3]

    config = {
        "name": "taxi",
        "pid": pid,
        "delimiter": ",",
        "code_path": "/tmp/taxi-code",
        "input_path": "hdfs://{}/{}/taxi".format(hdfs_namenode, hdfs_root),
        "output_path": "hdfs://{}/{}/taxi-out".format(hdfs_namenode, hdfs_root),
    }

    taxi(config)
