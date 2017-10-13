import salmon.lang as sal
from salmon.comp import dagonly, pruneDag
from salmon.utils import *
import salmon.partition as part
from salmon.codegen.scotch import ScotchCodeGen
from salmon.codegen.sharemind import SharemindCodeGen, SharemindCodeGenConfig
from salmon.codegen import CodeGenConfig
from salmon.codegen.spark import SparkCodeGen
from salmon.codegen.python import PythonCodeGen
from salmon import codegen
from salmon.dispatch import dispatch_all
from salmon.net import setup_peer
import sys


def testHybridJoinWorkflow():

    @dagonly
    def protocol():
        # define inputs
        colsInA = [
            defCol("a", "INTEGER", [1]),
            defCol("b", "INTEGER", [1]),
        ]
        in1 = sal.create("in1", colsInA, set([1]))
        in1.isMPC = True

        colsInB = [
            defCol("c", "INTEGER", [1], [2]),
            defCol("d", "INTEGER", [2])
        ]
        in2 = sal.create("in2", colsInB, set([2]))
        in2.isMPC = True

        clA = sal._close(in1, "clA", set([1, 2, 3]))
        clA.isMPC = True
        clB = sal._close(in2, "clB", set([1, 2, 3]))
        clB.isMPC = True

        shuffledA = sal.shuffle(clA, "shuffledA")
        shuffledA.isMPC = True
        persistedA = sal._persist(shuffledA, "persistedA")
        persistedA.isMPC = True
        shuffledB = sal.shuffle(clB, "shuffledB")
        shuffledB.isMPC = True
        persistedB = sal._persist(shuffledB, "persistedB")
        persistedB.isMPC = True

        keysAclosed = sal.project(shuffledA, "keysAclosed", ["a"])
        keysAclosed.isMPC = True
        keysBclosed = sal.project(shuffledB, "keysBclosed", ["c"])
        keysBclosed.isMPC = True

        keysA = sal._open(keysAclosed, "keysA", 1)
        keysA.isMPC = True
        keysB = sal._open(keysBclosed, "keysB", 1)
        keysB.isMPC = True

        indexedA = sal.index(keysA, "indexedA", "indexA")
        indexedA.isMPC = False
        indexedB = sal.index(keysB, "indexedB", "indexB")
        indexedB.isMPC = False

        joinedindeces = sal.join(
            indexedA, indexedB, "joinedindeces", ["a"], ["c"])
        joinedindeces.isMPC = False
        indecesonly = sal.project(
            joinedindeces, "indecesonly", ["indexA", "indexB"])
        indecesonly.isMPC = False

        indecesclosed = sal._close(
            indecesonly, "indecesclosed", set([1, 2, 3]))
        indecesclosed.isMPC = True

        joined = sal._index_join(persistedA, persistedB, "joined", [
                                 "a"], ["c"], indecesclosed)
        joined.isMPC = True

        # dummy projection to force non-mpc subdag
        res = sal.project(
            joined, "res", ["a"])
        res.isMPC = True

        sal._open(res, "opened", 1)

        # create dag
        return set([in1, in2])

    pid = int(sys.argv[1])
    workflow_name = "hybrid-join-" + str(pid)
    sm_cg_config = SharemindCodeGenConfig(workflow_name, "/mnt/shared")
    codegen_config = CodeGenConfig(
        workflow_name).with_sharemind_config(sm_cg_config)
    codegen_config.code_path = "/mnt/shared/" + workflow_name
    codegen_config.input_path = "/mnt/shared"
    codegen_config.output_path = "/mnt/shared"

    dag = pruneDag(protocol(), pid)
    mapping = part.heupart(dag, ["sharemind"], ["python"])
    job_queue = []
    for idx, (fmwk, subdag) in enumerate(mapping):
        if fmwk == "sharemind":
            job = SharemindCodeGen(codegen_config, subdag, pid).generate(
                "sharemind-" + str(idx), None)
        else:
            job = PythonCodeGen(codegen_config, subdag).generate("python-" + str(idx), None)
        job_queue.append(job)

    # sharemind_config = {
    #     "pid": pid,
    #     "parties": {
    #         1: {"host": "localhost", "port": 9001},
    #         2: {"host": "localhost", "port": 9002},
    #         3: {"host": "localhost", "port": 9003}
    #     }
    # }
    # sm_peer = setup_peer(sharemind_config)
    # dispatch_all(None, sm_peer, job_queue)

if __name__ == "__main__":

    testHybridJoinWorkflow()
