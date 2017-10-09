from salmon.codegen.sharemind import SharemindCodeGen
import salmon.dispatch
import salmon.net
from salmon.comp import dagonly
import salmon.lang as sal
from salmon.utils import *
import sys


@dagonly
def protocol():

    # define inputs
    colsIn1 = [
        defCol("a", "INTEGER", [1]),
        defCol("b", "INTEGER", [1])
    ]
    in1 = sal.create("in1", colsIn1, set([1]))
    colsIn2 = [
        defCol("a", "INTEGER", [2]),
        defCol("b", "INTEGER", [2])
    ]
    in2 = sal.create("in2", colsIn2, set([2]))
    colsIn3 = [
        defCol("a", "INTEGER", [3]),
        defCol("b", "INTEGER", [3])
    ]
    in3 = sal.create("in3", colsIn3, set([3]))

    cl1 = sal._close(in1, "cl1", set([1, 2, 3]))
    cl2 = sal._close(in2, "cl2", set([1, 2, 3]))
    cl3 = sal._close(in3, "cl3", set([1, 2, 3]))
    rel = sal.concat([cl1, cl2, cl3], "rel")
    agg = sal.aggregate(rel, "agg", "a", "b", "+", "total")

    opened = sal._open(agg, "opened", 1)
    # return root nodes
    return set([in1, in2, in3])

if __name__ == "__main__":

    sharemind_home = "/home/sharemind/Sharemind-SDK/sharemind/client"
    spark_master = "local"

    pid = int(sys.argv[1])
    sharemind_config = {
        "pid": pid,
        "parties": {
            1: {"host": "localhost", "port": 9001},
            2: {"host": "localhost", "port": 9002},
            3: {"host": "localhost", "port": 9003}
        }
    }
    sm_peer = salmon.net.setup_peer(sharemind_config)

    codegen_config = CodeGenConfig()

    job = SharemindCodeGen(codegen_config, protocol(), pid).generate("job-" + str(pid), sharemind_home)
    job_queue = [job]
    salmon.dispatch.dispatch_all(spark_master, sm_peer, job_queue)
