import os
import sys

import conclave.lang as cc
from conclave import generate_code, dispatch_jobs
from conclave.config import CodeGenConfig, OblivcConfig, NetworkConfig
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
    data_root = sys.argv[2]
    print(data_root)
    # define name for the workflow
    workflow_name = "aspirin-large-join-" + pid + "-" + data_root
    # configure conclave
    conclave_config = CodeGenConfig(workflow_name, int(pid))

    net_conf = [
        {"host": "10.10.10.17", "port": 8000},
        {"host": "10.10.10.11", "port": 8000}
    ]
    net = NetworkConfig(net_conf, int(pid))
    conclave_config.with_network_config(net)
    conclave_config.use_leaky_ops = False

    oc_conf = OblivcConfig("/home/ubuntu/obliv-c/bin/oblivcc", "10.10.10.17:9000")
    conclave_config.with_oc_config(oc_conf)

    conclave_config.code_path = "/mnt/shared/{}/".format(workflow_name)
    conclave_config.input_path = os.path.join("/mnt/shared", data_root)
    conclave_config.output_path = os.path.join("/mnt/shared", data_root)

    job_queue = generate_code(protocol, conclave_config, ["obliv-c"], ["python"], apply_optimizations=True)
    dispatch_jobs(job_queue, conclave_config)