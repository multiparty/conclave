import os
import sys

import conclave.dag as ccdag
import conclave.lang as cc
from conclave import dispatch_jobs, SharemindCodeGen
from conclave.config import CodeGenConfig, SharemindCodeGenConfig
from conclave.utils import *


def protocol():
    # define inputs
    colsIn1 = [
        defCol("a", "INTEGER", [1]),
        defCol("b", "INTEGER", [1])
    ]
    in1 = cc.create("govreg", colsIn1, {1})
    colsIn2 = [
        defCol("c", "INTEGER", [2]),
        defCol("d", "INTEGER", [2])
    ]
    in2 = cc.create("company0", colsIn2, {2})
    colsIn3 = [
        defCol("c", "INTEGER", [3]),
        defCol("d", "INTEGER", [3])
    ]
    in3 = cc.create("company1", colsIn3, {3})

    cl1 = cc._close(in1, "cl1", {1, 2, 3})
    projA = cc.project(cl1, "projA", ["a", "b"])
    cl2 = cc._close(in2, "cl2", {1, 2, 3})
    cl3 = cc._close(in3, "cl3", {1, 2, 3})
    right_rel = cc.concat([cl2, cl3], "right_rel")
    projB = cc.project(right_rel, "projB", ["c", "d"])

    joined = cc.join(projA, projB, "joined", ["a"], ["c"])
    agg = cc.aggregate(joined, "agg", ["b"], "d", "+", "total")

    cc._open(agg, "opened", 1)
    return {in1, in2, in3}


if __name__ == "__main__":
    pid = sys.argv[1]
    # define name for the workflow
    workflow_name = "sharemind-ssn-" + pid
    # configure conclave
    conclave_config = CodeGenConfig(workflow_name, int(pid))
    conclave_config.all_pids = [1, 2, 3]
    sharemind_conf = SharemindCodeGenConfig("/mnt/shared", use_docker=True, use_hdfs=False)
    conclave_config.with_sharemind_config(sharemind_conf)
    current_dir = os.path.dirname(os.path.realpath(__file__))
    # point conclave to the directory where the generated code should be stored/ read from
    conclave_config.code_path = os.path.join("/mnt/shared", workflow_name)
    # point conclave to directory where data is to be read from...
    conclave_config.input_path = "/mnt/shared/ssn_data"
    # and written to
    conclave_config.output_path = "/mnt/shared/ssn_data"
    job = SharemindCodeGen(conclave_config, ccdag.OpDag(protocol()), conclave_config.pid).generate("sharemind-0", "")
    job_queue = [job]
    dispatch_jobs(job_queue, conclave_config)
