import os
import sys

import conclave.lang as cc
from conclave import generate_code, dispatch_jobs
from conclave.config import CodeGenConfig, SharemindCodeGenConfig
from conclave.utils import defCol


def protocol():
    pid_col = "8"
    diagnosis_col = "16"

    cols_to_skip = 8
    num_diagnosis_cols = 13

    left_diagnosis_cols = [defCol(str(i + cols_to_skip), "INTEGER", 1) for i in range(num_diagnosis_cols)]
    left_diagnosis = cc.create("left_diagnosis", left_diagnosis_cols, {1})

    right_diagnosis_cols = [defCol(str(i + cols_to_skip), "INTEGER", 1) for i in range(num_diagnosis_cols)]
    right_diagnosis = cc.create("right_diagnosis", right_diagnosis_cols, {1})

    diagnosis = cc.concat([left_diagnosis, right_diagnosis], "diagnosis")

    cohort = cc.cc_filter(diagnosis, "cohort", pid_col, "==", scalar=93)
    counts = cc.aggregate(cohort, "counts", [diagnosis_col], "18", "SUM", "total")
    cc.sort_by(counts, "expected", "total")

    return {left_diagnosis, right_diagnosis}


if __name__ == "__main__":
    pid = sys.argv[1]
    # define name for the workflow
    workflow_name = "simple-comorb-test-" + pid
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
