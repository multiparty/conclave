import os
import sys

import conclave.lang as cc
from conclave import generate_code, dispatch_jobs
from conclave.config import CodeGenConfig, SharemindCodeGenConfig
from conclave.utils import defCol


def protocol():
    govreg_cols = [
        defCol("a", "INTEGER", 1),
        defCol("b", "INTEGER", 1)
    ]
    govreg = cc.create("govreg", govreg_cols, {1})

    company0_cols = [
        defCol("c", "INTEGER", 1, 2),
        defCol("d", "INTEGER", 2)
    ]
    company0 = cc.create("company0", company0_cols, {2})

    company1_cols = [
        defCol("c", "INTEGER", 1, 3),
        defCol("d", "INTEGER", 3)
    ]
    company1 = cc.create("company1", company1_cols, {3})

    companies = cc.concat([company0, company1], "companies")

    joined = cc.join(govreg, companies, "joined", ["a"], ["c"])
    actual = cc.aggregate(joined, "actual", ["b"], "d", "sum", "total")
    cc.collect(actual, 1)

    return {govreg, company0, company1}


if __name__ == "__main__":
    pid = sys.argv[1]
    data_root = sys.argv[2]
    workflow_name = "ssn-benchmark" + pid + "-" + data_root
    conclave_config = CodeGenConfig(workflow_name, int(pid))
    sharemind_conf = SharemindCodeGenConfig("/mnt/shared", use_docker=True, use_hdfs=False)
    conclave_config.with_sharemind_config(sharemind_conf)
    conclave_config.use_leaky_ops = True
    current_dir = os.path.dirname(os.path.realpath(__file__))
    conclave_config.code_path = os.path.join("/mnt/shared", workflow_name)
    conclave_config.input_path = os.path.join("/mnt/shared", data_root)
    conclave_config.output_path = os.path.join("/mnt/shared", data_root)
    job_queue = generate_code(protocol, conclave_config, ["sharemind"], ["python"], apply_optimizations=True)
    dispatch_jobs(job_queue, conclave_config)
