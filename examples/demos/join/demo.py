import os

import conclave.lang as cc
from conclave import generate_and_dispatch
from conclave.config import CodeGenConfig
from conclave.utils import defCol


def protocol():
    input_columns_left = [
        defCol("column_a", "INTEGER", [1]),
        defCol("column_b", "INTEGER", [1])
    ]
    input_columns_right = [
        defCol("column_a", "INTEGER", [1]),
        defCol("column_b", "INTEGER", [1])
    ]
    left = cc.create("left", input_columns_left, {1})
    right = cc.create("right", input_columns_right, {1})
    cc._join_flags(left, right, "joined", ["column_a"], ["column_a"])
    return {left, right}


if __name__ == "__main__":
    # define name for the workflow
    workflow_name = "join"
    # configure conclave
    conclave_config = CodeGenConfig(workflow_name)
    # need the absolute path to current directory
    current_dir = os.path.dirname(os.path.realpath(__file__))
    # point conclave to the directory where the generated code should be stored/ read from
    conclave_config.code_path = os.path.join(current_dir, workflow_name)
    # point conclave to directory where data is to be read from...
    conclave_config.input_path = os.path.join(current_dir, "data")
    # and written to
    conclave_config.output_path = os.path.join(current_dir, "data")
    # define this party's unique ID (in this demo there is only one party)
    conclave_config.pid = 1
    # define all parties involved in this workflow
    conclave_config.all_pids = [1]
    # compile and execute protocol, specifying available mpc and local processing backends
    generate_and_dispatch(protocol, conclave_config, ["sharemind"], ["python"])
