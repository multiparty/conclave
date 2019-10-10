import os
import sys

import conclave.lang as cc
from conclave import generate_code, dispatch_jobs
from conclave.config import CodeGenConfig, SharemindCodeGenConfig, OblivcConfig, NetworkConfig
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


def main():
    pid = sys.argv[1]
    data_root = sys.argv[2]
    mpc_backend = sys.argv[3]

    # define name for the workflow
    workflow_name = "aspirin-large-join-" + pid + "-" + data_root
    # configure conclave
    conclave_config = CodeGenConfig(workflow_name, int(pid))
    if mpc_backend == "sharemind":
        sharemind_conf = SharemindCodeGenConfig("/mnt/shared", use_docker=True, use_hdfs=False)
        conclave_config.with_sharemind_config(sharemind_conf)
    elif mpc_backend == "obliv-c":
        conclave_config.all_pids = [1, 2]
        net_conf = [
            {"host": "ca-spark-node-0", "port": 8001},
            {"host": "cb-spark-node-0", "port": 8002}
        ]
        net = NetworkConfig(net_conf, int(pid))
        conclave_config.with_network_config(net)

        oc_conf = OblivcConfig("/obliv-c/bin/oblivcc", "ca-spark-node-0:9000")
        conclave_config.with_oc_config(oc_conf)
    else:
        raise Exception("Unknown MPC backend {}".format(mpc_backend))

    conclave_config.code_path = os.path.join("/mnt/shared", workflow_name)
    conclave_config.input_path = os.path.join("/mnt/shared", data_root)
    conclave_config.output_path = os.path.join("/mnt/shared", data_root)

    job_queue = generate_code(protocol, conclave_config, [mpc_backend], ["python"], apply_optimizations=True)
    dispatch_jobs(job_queue, conclave_config)


if __name__ == "__main__":
    main()
