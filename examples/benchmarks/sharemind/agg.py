from salmon.codegen import CodeGenConfig
from salmon.codegen.sharemind import SharemindCodeGen
import salmon.dispatch
import salmon.net
from salmon.comp import dagonly
import salmon.lang as sal
from salmon.utils import *
import sys


def setup():

    # define inputs
    colsIn1 = [
        defCol("a", "INTEGER", [1]),
        defCol("b", "INTEGER", [1])
    ]
    colsIn2 = [
        defCol("a", "INTEGER", [2]),
        defCol("b", "INTEGER", [2])
    ]
    colsIn3 = [
        defCol("a", "INTEGER", [3]),
        defCol("b", "INTEGER", [3])
    ]

    in1 = sal.create("in1", colsIn1, set([1]))
    in2 = sal.create("in2", colsIn2, set([2]))
    in3 = sal.create("in3", colsIn3, set([3]))

    cl1 = sal._close(in1, "cl1", set([1, 2, 3]))
    cl2 = sal._close(in2, "cl2", set([1, 2, 3]))
    cl3 = sal._close(in3, "cl3", set([1, 2, 3]))

    # TODO: (ben) do we need to concat first?
    rel = sal.concat([cl1, cl2, cl3], "rel")

    return set([in1, in2, in3]), rel


@dagonly
def agg():

    inputs, rel = setup()
    res = sal.aggregate(rel, "agg", ["b"], "a", "+", "total")

    opened = sal._open(res, "opened", 1)

    return inputs


def party_proc():

    pid = sys.argv[1]

    sharemind_home = "/home/sharemind/Sharemind-SDK/sharemind/client"

    sharemind_config = {
        "pid": pid,
        "parties": {
            1: {"host": "ca-spark-node-0", "port": 9001},
            2: {"host": "cb-spark-node-0", "port": 9002},
            3: {"host": "cc-spark-node-0", "port": 9003}
        }
    }
    peer = salmon.net.setup_peer(sharemind_config)

    codegen_config = CodeGenConfig()

    cg = SharemindCodeGen(codegen_config, agg(), pid)
    cg.generate("agg-" + str(pid), sharemind_home)


if __name__ == "__main__":

    party_proc()
    agg()
