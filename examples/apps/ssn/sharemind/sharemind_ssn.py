from salmon.codegen.sharemind import SharemindCodeGen, SharemindCodeGenConfig
from salmon import CodeGenConfig
import salmon.dispatch
import salmon.net
from salmon.comp import dag_only
import salmon.lang as sal
from salmon.utils import *
import sys

@dag_only
def protocol():

    # define inputs
    colsIn1 = [
        defCol("a", "INTEGER", [1]),
        defCol("b", "INTEGER", [1])
    ]
    in1 = sal.create("govreg", colsIn1, set([1]))
    colsIn2 = [
        defCol("c", "INTEGER", [2]),
        defCol("d", "INTEGER", [2])
    ]
    in2 = sal.create("company0", colsIn2, set([2]))
    colsIn3 = [
        defCol("c", "INTEGER", [3]),
        defCol("d", "INTEGER", [3])
    ]
    in3 = sal.create("company1", colsIn3, set([3]))

    cl1 = sal._close(in1, "cl1", set([1, 2, 3]))
    projA = sal.project(cl1, "projA", ["a", "b"])
    cl2 = sal._close(in2, "cl2", set([1, 2, 3]))
    cl3 = sal._close(in3, "cl3", set([1, 2, 3]))
    right_rel = sal.concat([cl2, cl3], "right_rel")
    projB = sal.project(right_rel, "projB", ["c", "d"])

    joined = sal.join(projA, right_rel, "joined", ["a"], ["c"])
    agg = sal.aggregate(joined, "agg", ["b"], "d", "+", "total")

    opened = sal._open(agg, "opened", 1)
    return set([in1, in2, in3])

if __name__ == "__main__":

    pid = int(sys.argv[1])
    
    workflow_name = "sharemind-ssn-" + str(pid)
    sm_cg_config = SharemindCodeGenConfig(
        workflow_name, "/mnt/shared", use_hdfs=False, use_docker=True)
    codegen_config = CodeGenConfig(
        workflow_name).with_sharemind_config(sm_cg_config)
    codegen_config.code_path = "/mnt/shared/" + workflow_name
    codegen_config.input_path = "/mnt/shared/ssn-data"
    codegen_config.output_path = "/mnt/shared/ssn-data"

    job = SharemindCodeGen(codegen_config, protocol(), pid).generate(
        "sharemind-0", "")
    job_queue = [job]
    
    sharemind_config = {
        "pid": pid,
        "parties": {
            1: {"host": "ca-spark-node-0", "port": 9001},
            2: {"host": "cb-spark-node-0", "port": 9002},
            3: {"host": "cc-spark-node-0", "port": 9003}
        }
    }
    sm_peer = salmon.net.setup_peer(sharemind_config)
    salmon.dispatch.dispatch_all(None, sm_peer, job_queue)

