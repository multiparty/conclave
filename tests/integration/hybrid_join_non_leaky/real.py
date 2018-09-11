import os
import sys

import conclave.lang as cc
from conclave import generate_code, dispatch_jobs
from conclave.config import CodeGenConfig, SharemindCodeGenConfig
from conclave.utils import defCol


def protocol():
    # define inputs
    left_cols = [
        defCol("a", "INTEGER", [1]),
        defCol("b", "INTEGER", [1]),
    ]
    left = cc.create("left", left_cols, {1})
    left_dummy = cc.project(left, "zzz_left_dummy", ["a", "b"])

    right_cols = [
        defCol("c", "INTEGER", [1], [2]),
        defCol("d", "INTEGER", [2])
    ]
    right = cc.create("right", right_cols, {2})
    right_dummy = cc.project(right, "right_dummy", ["c", "d"])

    actual = cc.join(left_dummy, right_dummy, "actual", ["a"], ["c"])

    cc.collect(actual, 1)
    # create dag
    return {left, right}


if __name__ == "__main__":
    pid = sys.argv[1]
    # define name for the workflow
    workflow_name = "hybrid-join-test-" + pid
    # configure conclave
    conclave_config = CodeGenConfig(workflow_name, int(pid))
    conclave_config.use_leaky_ops = True
    sharemind_conf = SharemindCodeGenConfig("/mnt/shared", use_docker=False, use_hdfs=False)
    conclave_config.with_sharemind_config(sharemind_conf)
    current_dir = os.path.dirname(os.path.realpath(__file__))
    # point conclave to the directory where the generated code should be stored/ read from
    conclave_config.code_path = os.path.join("/mnt/shared", workflow_name)
    # point conclave to directory where data is to be read from...
    conclave_config.input_path = os.path.join(current_dir, "data")
    # and written to
    conclave_config.output_path = os.path.join(current_dir, "data")
    # define this party's unique ID (in this demo there is only one party)
    job_queue = generate_code(protocol, conclave_config, ["sharemind"], ["python"], apply_optimizations=True)
    dispatch_jobs(job_queue, conclave_config)
