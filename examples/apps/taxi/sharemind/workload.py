import os
import sys

import conclave.dag as ccdag
import conclave.lang as cc
from conclave import dispatch_jobs, SharemindCodeGen
from conclave.config import CodeGenConfig, SharemindCodeGenConfig
from conclave.utils import defCol


def protocol():
    colsIn1 = [
        defCol("companyID", "INTEGER", [1]),
        defCol("price", "INTEGER", [1])
    ]
    in1 = cc.create("in1", colsIn1, {1})
    colsIn2 = [
        defCol("companyID", "INTEGER", [2]),
        defCol("price", "INTEGER", [2])
    ]
    in2 = cc.create("in2", colsIn2, {2})
    colsIn3 = [
        defCol("companyID", "INTEGER", [3]),
        defCol("price", "INTEGER", [3])
    ]
    in3 = cc.create("in3", colsIn3, {3})

    cl1 = cc._close(in1, "cl1", {1, 2, 3})
    cl2 = cc._close(in2, "cl2", {1, 2, 3})
    cl3 = cc._close(in3, "cl3", {1, 2, 3})
    cab_data = cc.concat([cl1, cl2, cl3], "cab_data")

    selected_input = cc.project(cab_data, "selected_input", ["companyID", "price"])
    local_rev = cc.aggregate(selected_input, "local_rev", ["companyID"], "price", "+", "local_rev")
    scaled_down = cc.divide(local_rev, "scaled_down", "local_rev", ["local_rev", 1000])
    first_val_blank = cc.multiply(scaled_down, "first_val_blank", "companyID", ["companyID", 0])
    local_rev_scaled = cc.multiply(first_val_blank, "local_rev_scaled", "local_rev", ["local_rev", 100])
    total_rev = cc.aggregate(first_val_blank, "total_rev", ["companyID"], "local_rev", "+", "global_rev")
    local_total_rev = cc.join(local_rev_scaled, total_rev, "local_total_rev", ["companyID"], ["companyID"])
    market_share = cc.divide(local_total_rev, "market_share", "local_rev", ["local_rev", "global_rev"])
    market_share_squared = cc.multiply(market_share, "market_share_squared", "local_rev",
                                       ["local_rev", "local_rev", 1])
    hhi = cc.aggregate(market_share_squared, "hhi", ["companyID"], "local_rev", "+", "hhi")

    cc._open(hhi, "hhi_opened", 1)

    # return root nodes
    return {in1, in2, in3}


if __name__ == "__main__":
    pid = sys.argv[1]
    data_root = os.path.join("/mnt/shared", sys.argv[2])
    # define name for the workflow
    workflow_name = "sharemind-taxi-" + pid
    # configure conclave
    conclave_config = CodeGenConfig(workflow_name, int(pid))
    conclave_config.network_config = {
        "pid": int(pid),
        "parties": {
            1: {"host": "ca-spark-node-0", "port": 9001},
            2: {"host": "cb-spark-node-0", "port": 9002},
            3: {"host": "cc-spark-node-0", "port": 9003}
        }
    }
    conclave_config.all_pids = [1, 2, 3]
    sharemind_conf = SharemindCodeGenConfig("/mnt/shared", use_docker=True, use_hdfs=False)
    conclave_config.with_sharemind_config(sharemind_conf)
    current_dir = os.path.dirname(os.path.realpath(__file__))
    # point conclave to the directory where the generated code should be stored/ read from
    conclave_config.code_path = os.path.join("/mnt/shared", workflow_name)
    # point conclave to directory where data is to be read from...
    conclave_config.input_path = data_root
    # and written to
    conclave_config.output_path = data_root
    job = SharemindCodeGen(conclave_config, ccdag.OpDag(protocol()), conclave_config.pid).generate("sharemind-0", "")
    job_queue = [job]
    dispatch_jobs(job_queue, conclave_config)
