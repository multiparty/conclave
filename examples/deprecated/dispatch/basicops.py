from salmon import CodeGenConfig
from salmon.codegen.sharemind import SharemindCodeGen
import salmon.dispatch
import salmon.net
from salmon.comp import dagonly
import salmon.lang as sal
from salmon.utils import *
from multiprocessing import Process
import sys


def setup():

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
    return set([in1, in2, in3]), rel

@dagonly
def agg():

    inputs, rel = setup()
    res = sal.aggregate(rel, "agg", "a", "b", "+", "total")

    opened = sal._open(res, "opened", 1)
    return inputs

@dagonly
def proj():

    inputs, rel = setup()
    res = sal.project(rel, "res", ["a"])

    opened = sal._open(res, "opened", 1)
    return inputs

@dagonly
def mult_by_const():

    inputs, rel = setup()
    res = sal.multiply(rel, "res", "a", ["a", 10])

    opened = sal._open(res, "opened", 1)
    return inputs

@dagonly
def mult_mixed():

    inputs, rel = setup()
    res = sal.multiply(rel, "res", "a", ["a", "b"])

    opened = sal._open(res, "opened", 1)
    return inputs

@dagonly
def div():

    inputs, rel = setup()
    res = sal.divide(rel, "res", "b", ["b", 2])

    opened = sal._open(res, "opened", 1)
    return inputs

@dagonly
def join():

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
        defCol("a", "INTEGER", [3]),
        defCol("b", "INTEGER", [3])
    ]
    # TODO: dummy relation for now
    in3 = sal.create("in3", colsIn3, set([3]))

    cl1 = sal._close(in1, "cl1", set([1, 2, 3]))
    cl2 = sal._close(in2, "cl2", set([1, 2, 3]))
    cl3 = sal._close(in3, "cl3", set([1, 2, 3]))

    res = sal.join(cl1, cl2, "res", ["a"], ["c"])

    opened = sal._open(res, "opened", 1)
    return set([in1, in2, in3])


@dagonly
def div_broken():

    inputs, rel = setup()
    res = sal.divide(rel, "res", "b", [10000, "b"])

    opened = sal._open(res, "opened", 1)
    return inputs


def party_proc(pid):

    sharemind_home = "/home/sharemind/Sharemind-SDK/sharemind/client"
    spark_master = "local"

    sharemind_config = {
        "pid": pid,
        "parties": {
            1: {"host": "localhost", "port": 9001},
            2: {"host": "localhost", "port": 9002},
            3: {"host": "localhost", "port": 9003}
        }
    }
    peer = salmon.net.setup_peer(sharemind_config)

    codegen_config = CodeGenConfig()

    job = SharemindCodeGen(codegen_config, join(), pid).generate("job-" + str(pid), sharemind_home)
    job_queue = [job]
    salmon.dispatch.dispatch_all(spark_master, peer, job_queue)

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
