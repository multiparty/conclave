import os
import sys

import conclave.lang as cc
from conclave import generate_code, dispatch_jobs
from conclave.config import CodeGenConfig, SharemindCodeGenConfig
from conclave.utils import defCol


def protocol():
    in1_columns = [
        defCol("a", "INTEGER", [1]),
        defCol("b", "INTEGER", [1]),
    ]
    in1 = cc.create("in1", in1_columns, {1})
    in1_dummy = cc.project(in1, "zzz_left_dummy", ["a", "b"])

    in2_columns = [
        defCol("c", "INTEGER", [2]),
        defCol("d", "INTEGER", [2])
    ]
    in2 = cc.create("in2", in2_columns, {2})
    in2_dummy = cc.project(in2, "in2_dummy", ["c", "d"])

    actual = cc.join(in1_dummy, in2_dummy, "actual", ["a"], ["c"])

    cc.collect(actual, 1)
    # create dag
    return {in1, in2}


if __name__ == "__main__":
    pid = sys.argv[1]
    data_root = sys.argv[2]
    # define name for the workflow
    workflow_name = "sharemind-join-oblivious-" + pid + "-" + data_root
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
    conclave_config.use_leaky_ops = False
    sharemind_conf = SharemindCodeGenConfig("/mnt/shared", use_docker=True, use_hdfs=False)
    conclave_config.with_sharemind_config(sharemind_conf)
    conclave_config.code_path = os.path.join("/mnt/shared", workflow_name)
    conclave_config.input_path = os.path.join("/mnt/shared", data_root)
    conclave_config.output_path = os.path.join("/mnt/shared", data_root)
    job_queue = generate_code(protocol, conclave_config, ["sharemind"], ["python"], apply_optimizations=True)
    dispatch_jobs(job_queue, conclave_config)
