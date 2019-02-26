import os
import sys

import conclave.lang as cc
from conclave import generate_code, dispatch_jobs
from conclave.config import CodeGenConfig, SharemindCodeGenConfig
from conclave.utils import defCol


def protocol():
    left_one_cols = [
        defCol("a", "INTEGER", 1),
        defCol("b", "INTEGER", 1)
    ]
    left_one = cc.create("left_one", left_one_cols, {1})

    right_one_cols = [
        defCol("c", "INTEGER", 1),
        defCol("d", "INTEGER", 1)
    ]
    right_one = cc.create("right_one", right_one_cols, {1})

    left_two_cols = [
        defCol("a", "INTEGER", 1),
        defCol("b", "INTEGER", 1)
    ]
    left_two = cc.create("left_two", left_two_cols, {1})

    right_two_cols = [
        defCol("c", "INTEGER", 1),
        defCol("d", "INTEGER", 1)
    ]
    right_two = cc.create("right_two", right_two_cols, {1})

    left = cc.concat([left_one, left_two], "left")
    right = cc.concat([right_one, right_two], "right")

    cc.join(left, right, "expected", ["a"], ["c"])

    return {left_one, left_two, right_one, right_two}


if __name__ == "__main__":
    pid = sys.argv[1]
    # define name for the workflow
    workflow_name = "simple-oblivious-test-" + pid
    # configure conclave
    conclave_config = CodeGenConfig(workflow_name, int(pid))
    conclave_config.all_pids = [1]
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
    job_queue = generate_code(protocol, conclave_config, ["sharemind"], ["python"], apply_optimizations=False)
    dispatch_jobs(job_queue, conclave_config)
