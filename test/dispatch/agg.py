from salmon.codegen.sharemind import SharemindCodeGen
import salmon.dispatch
import salmon.net
from salmon.comp import dagonly
import salmon.lang as sal
from salmon.utils import *
from multiprocessing import Process
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

def party_proc(pid):

    sharemind_home = "/home/sharemind/Sharemind-SDK/sharemind/client"
    config = {
        "pid": pid,
        "parties": {
            1: {"host": "localhost", "port": 9001},
            2: {"host": "localhost", "port": 9002},
            3: {"host": "localhost", "port": 9003}
        }
    }
    peer = salmon.net.setup_peer(config)

    job = SharemindCodeGen(protocol(), pid).generate("job-" + str(pid), sharemind_home)
    job_queue = [job]
    salmon.dispatch.dispatch_all(peer, job_queue)

if __name__ == "__main__":

    # run each party in separate process
    # TODO: switch to threads if asyncio is thread-safe
    procs = []
    for pid in [1, 2, 3]:
        p = Process(target=party_proc, args=(pid,))
        p.start()
        procs.append(p)
    # wait for processes to complete
    for p in procs:
        p.join()

