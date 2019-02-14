import os
import sys

import conclave.lang as cc
from conclave import generate_code, dispatch_jobs
from conclave.config import CodeGenConfig, SharemindCodeGenConfig
from conclave.utils import defCol


def protocol():
    govreg_cols = [
        defCol("a", "INTEGER", [1]),
        defCol("b", "INTEGER", [1])
    ]
    govreg = cc.create("a_govreg", govreg_cols, {1})
    company0_cols = [
        defCol("c", "INTEGER", [1]),
        defCol("d", "INTEGER", [1])
    ]
    company0 = cc.create("company0", company0_cols, {1})
    company1_cols = [
        defCol("c", "INTEGER", [1]),
        defCol("d", "INTEGER", [1])
    ]
    company1 = cc.create("company1", company1_cols, {1})
    companies = cc.concat([company0, company1], "companies")

    joined = cc.join(govreg, companies, "joined", ["a"], ["c"])
    cc.aggregate(joined, "expected", ["b"], "d", "sum", "total")

    return {govreg, company0, company1}


if __name__ == "__main__":
    pid = sys.argv[1]
    # define name for the workflow
    workflow_name = "simple-ssn-" + pid
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
