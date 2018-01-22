from conclave.codegen.sharemind import SharemindCodeGen, SharemindCodeGenConfig
from conclave import CodeGenConfig
import conclave.dispatch
import conclave.net
from conclave.comp import dag_only
import conclave.lang as sal
from conclave.utils import *
import sys
import exampleutils

@dag_only
def protocol():

    # define inputs
    colsIn1 = [
        defCol("a", "INTEGER", [1]),
        defCol("b", "INTEGER", [1])
    ]
    in1 = sal.create("in1", colsIn1, set([1]))
    colsIn2 = [
        defCol("c", "INTEGER", [2]),
        defCol("d", "INTEGER", [2])
    ]
    in2 = sal.create("in2", colsIn2, set([2]))
    colsIn3 = [
        defCol("e", "INTEGER", [3]),
        defCol("f", "INTEGER", [3])
    ]
    in3 = sal.create("in3", colsIn3, set([3]))

    cl1 = sal._close(in1, "cl1", set([1, 2, 3]))
    cl2 = sal._close(in2, "cl2", set([1, 2, 3]))
    cl3 = sal._close(in3, "cl3", set([1, 2, 3]))
    rel = sal.concat([cl1, cl2, cl3], "rel")
    agg = sal.aggregate(rel, "agg", ["a"], "b", "+", "total")

    opened = sal._open(agg, "opened", 1)
    # return root nodes
    return set([in1, in2, in3])


if __name__ == "__main__":

    pid = int(sys.argv[1])
    sharemind_config = exampleutils.get_sharemind_config(pid)

    sm_peer = conclave.net.setup_peer(sharemind_config)

    workflow_name = "job-" + str(pid)
    sm_cg_config = SharemindCodeGenConfig(
        workflow_name, "/mnt/shared", use_hdfs=False)
    codegen_config = CodeGenConfig(
        workflow_name).with_sharemind_config(sm_cg_config)
    codegen_config.code_path = "/mnt/shared/" + workflow_name
    codegen_config.input_path = "/mnt/shared"
    codegen_config.output_path = "/mnt/shared"

    exampleutils.generate_data(pid, codegen_config.output_path)

    job = SharemindCodeGen(codegen_config, protocol(), pid).generate(
        "sharemind-0", "")
    job_queue = [job]
    conclave.dispatch.dispatch_all(None, sm_peer, job_queue)
