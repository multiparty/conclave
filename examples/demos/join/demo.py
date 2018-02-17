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
        defCol("column_a", "INTEGER", [1], [2]),
        defCol("column_c", "INTEGER", [1])
    ]
    right = cc.create("right", input_columns_right, {2})

    # TODO so sad, partitioning still finicky
    left_projected = cc.project(left, "zzz-left_projected", ["column_a", "column_b"])
    left_projected.is_mpc = False
    left_projected.out_rel.stored_with = {1}

    right_projected = cc.project(right, "right_projected", ["column_a", "column_c"])
    right_projected.is_mpc = False
    right_projected.out_rel.stored_with = {2}

    left_closed = cc._close(left_projected, "left_closed", {1, 2, 3})
    left_closed.is_mpc = True

    right_closed = cc._close(right_projected, "right_closed", {1, 2, 3})
    right_closed.is_mpc = True

    left_shuffled = cc.shuffle(left_closed, "left_shuffled")
    left_shuffled.is_mpc = True

    left_persisted = cc._persist(left_shuffled, "left_persisted")
    left_persisted.is_mpc = True

    right_shuffled = cc.shuffle(right_closed, "right_shuffled")
    right_shuffled.is_mpc = True

    right_persisted = cc._persist(right_shuffled, "right_persisted")
    right_persisted.is_mpc = True

    left_keys_closed = cc.project(left_shuffled, "left_keys_closed", ["column_a"])
    left_keys_closed.is_mpc = True

    right_keys_closed = cc.project(right_shuffled, "right_keys_closed", ["column_a"])
    right_keys_closed.is_mpc = True

    left_keys_open = cc._open(left_keys_closed, "left_keys_open", 1)
    left_keys_open.is_mpc = True

    right_keys_open = cc._open(right_keys_closed, "right_keys_open", 1)
    right_keys_open.is_mpc = True

    left_dummy = cc.project(left_keys_open, "left_dummy", ["column_a"])
    left_dummy.is_mpc = False

    right_dummy = cc.project(right_keys_open, "right_dummy", ["column_a"])
    right_dummy.is_mpc = False

    flags = cc._join_flags(left_dummy, right_dummy, "flags", ["column_a"], ["column_a"])
    flags.is_mpc = False

    flags_closed = cc._close(flags, "flags_closed", {1, 2, 3})
    flags_closed.is_mpc = True

    joined = cc._flag_join(left_persisted, right_persisted, "joined", ["column_a"], ["column_a"], flags_closed)
    cc._open(joined, "joined_open", 1)

    return {left, right}


if __name__ == "__main__":
    pid = sys.argv[1]
    # define name for the workflow
    workflow_name = "hybrid-join-test-" + pid
    # configure conclave
    conclave_config = CodeGenConfig(workflow_name, int(pid))
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
    # dispatch_jobs(job_queue, conclave_config)
