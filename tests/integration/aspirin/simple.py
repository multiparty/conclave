import os
import sys

import conclave.lang as cc
from conclave import generate_code, dispatch_jobs
from conclave.config import CodeGenConfig, SharemindCodeGenConfig
from conclave.utils import defCol


def protocol():
    medication_cols = [
        defCol("a", "INTEGER", [1]),
        defCol("b", "INTEGER", [1]),
        defCol("t", "INTEGER", [1])
    ]
    medication = cc.create("medication", medication_cols, {1})

    diagnosis_cols = [
        defCol("c", "INTEGER", [1]),
        defCol("d", "INTEGER", [1]),
        defCol("o", "INTEGER", [1])
    ]
    diagnosis = cc.create("diagnosis", diagnosis_cols, {1})
    
    aspirin = cc.cc_filter(medication, "aspirin", "b", "==", scalar=2)
    heart_patients = cc.cc_filter(diagnosis, "heart_patients", "d", "==", scalar=3)

    joined = cc.join(aspirin, heart_patients, "joined", ["a"], ["c"])
    cases = cc.cc_filter(joined, "cases", "t", "<", other_col_name="o")

    cc.distinct_count(cases, "expected", "a")

    return {medication, diagnosis}


if __name__ == "__main__":
    pid = sys.argv[1]
    # define name for the workflow
    workflow_name = "simple-aspirin-test-" + pid
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
