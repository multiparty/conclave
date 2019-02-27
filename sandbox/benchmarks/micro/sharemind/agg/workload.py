import os
import sys

import conclave.dag as ccdag
import conclave.lang as cc
from conclave import dispatch_jobs, SharemindCodeGen
from conclave.config import CodeGenConfig, SharemindCodeGenConfig
from conclave.utils import defCol


def protocol():
    colsIn1 = [
        defCol("a", "INTEGER", [1]),
        defCol("b", "INTEGER", [1])
    ]
    in1 = cc.create("in1", colsIn1, {1})
    colsIn2 = [
        defCol("a", "INTEGER", [2]),
        defCol("b", "INTEGER", [2])
    ]
    in2 = cc.create("in2", colsIn2, {2})
    colsIn3 = [
        defCol("a", "INTEGER", [3]),
        defCol("b", "INTEGER", [3])
    ]
    in3 = cc.create("in3", colsIn3, {3})

    cl1 = cc._close(in1, "cl1", {1, 2, 3})
    cl2 = cc._close(in2, "cl2", {1, 2, 3})
    cl3 = cc._close(in3, "cl3", {1, 2, 3})
    rel = cc.concat([cl1, cl2, cl3], "rel")
    res = cc.aggregate(rel, "agg", ["a"], "b", "+", "total")
    cc._open(res, "opened", 1)

    return {in1, in2, in3}


if __name__ == "__main__":
    pid = sys.argv[1]
    data_root = os.path.join("/mnt/shared", sys.argv[2])
    # define name for the workflow
    workflow_name = "sharemind-agg-micro" + pid
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
