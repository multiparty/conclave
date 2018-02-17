import os
import sys

import conclave.lang as cc
from conclave import generate_code, dispatch_jobs
from conclave.config import CodeGenConfig, SharemindCodeGenConfig, NetworkConfig
from conclave.utils import defCol


def protocol():
    input_columns_left = [
        defCol("column_a", "INTEGER", [1]),
        defCol("column_b", "INTEGER", [1])
    ]
    left = cc.create("left", input_columns_left, {1})
    input_columns_right = [
        defCol("column_a", "INTEGER", [1]),
        defCol("column_c", "INTEGER", [1])
    ]
    right = cc.create("right", input_columns_right, {1})
    joined_expected = cc.join(left, right, "joined_expected", ["column_a"], ["column_a"])
    cc.sort_by(joined_expected, "expected", "column_a")

    return {left, right}


if __name__ == "__main__":
    pid = sys.argv[1]
    # define name for the workflow
    workflow_name = "hybrid-join-test-" + pid
    # configure conclave
    conclave_config = CodeGenConfig(workflow_name, int(pid))
    conclave_config.all_pids = [1]
    current_dir = os.path.dirname(os.path.realpath(__file__))
    # point conclave to the directory where the generated code should be stored/ read from
    conclave_config.code_path = os.path.join(current_dir, workflow_name)
    # point conclave to directory where data is to be read from...
    conclave_config.input_path = os.path.join(current_dir, "data")
    # and written to
    conclave_config.output_path = os.path.join(current_dir, "data")
    # define this party's unique ID (in this demo there is only one party)
    job_queue = generate_code(protocol, conclave_config, ["sharemind"], ["python"], apply_optimizations=False)
    dispatch_jobs(job_queue, conclave_config)
