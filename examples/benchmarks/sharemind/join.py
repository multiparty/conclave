from salmon.codegen import CodeGenConfig
from salmon.codegen.sharemind import SharemindCodeGen, SharemindCodeGenConfig
import salmon.dispatch
import salmon.net
from salmon.comp import dagonly
import salmon.lang as sal
from salmon.utils import *
import sys

def join(pid, config, sharemind_peer, f_size):

    @dagonly
    def protocol():

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

        cl1 = sal._close(in1, "cl1", set([1, 2, 3]))
        cl2 = sal._close(in2, "cl2", set([1, 2, 3]))

        res = sal.join(cl1, cl2, "res", ["a"], ["c"])

        opened = sal._open(res, "opened", 1)
        return set([in1, in2])

    cg = SharemindCodeGen(config, protocol(), pid)
    job = cg.generate("join_{}".format(f_size), "")
    job_queue = [job]

    salmon.dispatch.dispatch_all(None, sharemind_peer, job_queue)


def no_hdfs():

    pid = int(sys.argv[1])
    num_tuples = sys.argv[2]

    workflow_name = "{}_{}_{}".format(op, num_tuples, pid)
    sm_cg_config = SharemindCodeGenConfig(
        workflow_name, "/mnt/shared", use_hdfs=False, use_docker=True)
    codegen_config = CodeGenConfig(
        workflow_name).with_sharemind_config(sm_cg_config)
    codegen_config.code_path = "/mnt/shared/" + workflow_name
    codegen_config.input_path = "/mnt/shared/join/" + num_tuples
    codegen_config.output_path = "/mnt/shared/join/" + num_tuples

    sharemind_config = {
        "pid": pid,
        "parties": {
            1: {"host": "ca-spark-node-0", "port": 9001},
            2: {"host": "cb-spark-node-0", "port": 9002},
            3: {"host": "cc-spark-node-0", "port": 9003}
        }
    }
    sm_peer = salmon.net.setup_peer(sharemind_config)

    join(pid, codegen_config, sm_peer, num_tuples)
    
if __name__ == "__main__":

    no_hdfs()
