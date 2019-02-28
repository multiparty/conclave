import os
import sys

import conclave.lang as cc
from conclave import generate_code, dispatch_jobs
from conclave.config import CodeGenConfig
from conclave.utils import defCol


def protocol():
    diagnosis_col = "12"
    num_diagnosis_cols = 13

    left_diagnosis_cols = [defCol(str(i), "INTEGER", 1) for i in range(num_diagnosis_cols)]
    left_diagnosis = cc.create("left_diagnosis", left_diagnosis_cols, {1})

    right_diagnosis_cols = [defCol(str(i), "INTEGER", 2) for i in range(num_diagnosis_cols)]
    right_diagnosis = cc.create("right_diagnosis", right_diagnosis_cols, {2})

    cohort = cc.concat([left_diagnosis, right_diagnosis], "cohort")
    counts = cc.aggregate_count(cohort, "counts", [diagnosis_col], "total")
    cc.collect(cc.sort_by(counts, "actual", "total"), 1)

    return {left_diagnosis, right_diagnosis}


if __name__ == "__main__":
    pid = sys.argv[1]
    # define name for the workflow
    workflow_name = "real-comorb-test-" + pid
    # configure conclave
    mpc_backend = sys.argv[2]
    conclave_config = CodeGenConfig(workflow_name, int(pid)) \
        .with_default_mpc_config(mpc_backend)
    current_dir = os.path.dirname(os.path.realpath(__file__))
    # point conclave to the directory where the generated code should be stored/ read from
    conclave_config.code_path = os.path.join("/mnt/shared", workflow_name)
    # point conclave to directory where data is to be read from...
    conclave_config.input_path = os.path.join(current_dir, "data")
    # and written to
    conclave_config.output_path = os.path.join(current_dir, "data")
    # define this party's unique ID (in this demo there is only one party)
    job_queue = generate_code(protocol, conclave_config, [mpc_backend], ["python"], apply_optimizations=True)
    dispatch_jobs(job_queue, conclave_config)
