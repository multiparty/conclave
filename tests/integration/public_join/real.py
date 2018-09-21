import os
import sys

import conclave.lang as cc
from conclave import generate_code, dispatch_jobs
from conclave.config import CodeGenConfig, SharemindCodeGenConfig
from conclave.utils import defCol


def protocol():
    left_cols = [
        defCol("a", "INTEGER", [1]),
        defCol("b", "INTEGER", [1])
    ]
    left = cc.create("left", left_cols, {1})
    left_dummy = cc.project(left, "left_dummy", ["a", "b"])

    right_cols = [
        defCol("c", "INTEGER", [2]),
        defCol("d", "INTEGER", [2])
    ]
    right = cc.create("right", right_cols, {2})
    right_dummy = cc.project(right, "right_dummy", ["c", "d"])

    left_join = cc._pub_join(left_dummy, "left_join", "a")
    right_join_proj = cc.project(
        cc._pub_join(right_dummy, "right_join", "c", is_server=False), "right_join_proj", ["d"]
    )
    actual = cc.concat_cols([left_join, right_join_proj], "actual")
    cc.collect(actual, 1)

    return {left, right}


if __name__ == "__main__":
    pid = sys.argv[1]
    try:
        use_leaky = sys.argv[2] == "-l"
    except Exception:
        use_leaky = False
    # define name for the workflow
    workflow_name = "real-oblivious-test-" + pid
    # configure conclave
    conclave_config = CodeGenConfig(workflow_name, int(pid))
    conclave_config.all_pids = [1, 2, 3]
    conclave_config.use_leaky_ops = use_leaky
    sharemind_conf = SharemindCodeGenConfig("/mnt/shared",
                                            use_docker=True,
                                            use_hdfs=False)
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
    # dispatch_jobs(job_queue, conclave_config)
