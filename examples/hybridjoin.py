import salmon.lang as sal
from salmon.comp import dagonly
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
        in1.isMPC = False

        projA = sal.project(in1, "projA", ["a", "b"])
        projA.isMPC = False
        projA.outRel.storedWith = set([1])

        colsInB = [
            defCol("c", "INTEGER", [1], [2]),
            defCol("d", "INTEGER", [2])
        ]
        in2 = sal.create("in2", colsInB, set([2]))
        in2.isMPC = False

        projB = sal.project(in2, "projB", ["c", "d"])
        projB.isMPC = False
        projB.outRel.storedWith = set([2])

        clA = sal._close(projA, "clA", set([1, 2, 3]))
        clA.isMPC = True
        clB = sal._close(projB, "clB", set([1, 2, 3]))
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
        keysAclosed.outRel.storedWith = set([1, 2, 3])
        keysAclosed.isMPC = True
        keysBclosed = sal.project(shuffledB, "keysBclosed", ["c"])
        keysBclosed.isMPC = True
        keysBclosed.outRel.storedWith = set([1, 2, 3])

        keysA = sal._open(keysAclosed, "keysA", 1)
        keysA.isMPC = True
        keysB = sal._open(keysBclosed, "keysB", 1)
        keysB.isMPC = True

        indexedA = sal.index(keysA, "indexedA", "indexA")
        indexedA.isMPC = False
        indexedA.outRel.storedWith = set([1])
        indexedB = sal.index(keysB, "indexedB", "indexB")
        indexedB.isMPC = False
        indexedB.outRel.storedWith = set([1])

        joinedindeces = sal.join(
            indexedA, indexedB, "joinedindeces", ["a"], ["c"])
        joinedindeces.isMPC = False
        joinedindeces.outRel.storedWith = set([1])

        indecesonly = sal.project(
            joinedindeces, "indecesonly", ["indexA", "indexB"])
        indecesonly.isMPC = False
        indecesonly.outRel.storedWith = set([1])

        indecesclosed = sal._close(
            indecesonly, "indecesclosed", set([1, 2, 3]))
        indecesclosed.isMPC = True

        joined = sal._index_join(persistedA, persistedB, "joined", [
                                 "a"], ["c"], indecesclosed)
        joined.isMPC = True

        sal._open(joined, "opened", 1)

        # create dag
        return set([in1, in2])

    pid = int(sys.argv[1])
    workflow_name = "hybrid-join-" + str(pid)
    sm_cg_config = SharemindCodeGenConfig(
        workflow_name, "/mnt/shared", use_hdfs=False)
    codegen_config = CodeGenConfig(
        workflow_name).with_sharemind_config(sm_cg_config)
    codegen_config.code_path = "/mnt/shared/" + workflow_name
    codegen_config.input_path = "/mnt/shared"
    codegen_config.output_path = "/mnt/shared"

    dag = protocol()
    mapping = part.heupart(dag, ["sharemind"], ["python"])
    job_queue = []
    for idx, (fmwk, subdag, storedWith) in enumerate(mapping):
        if fmwk == "sharemind":
            job = SharemindCodeGen(codegen_config, subdag, pid).generate(
                "sharemind-" + str(idx), None)
        else:
            job = PythonCodeGen(codegen_config, subdag).generate(
                "python-" + str(idx), None)
        # TODO: this probably doesn't belong here
        if not pid in storedWith:
            job.skip = True
        job_queue.append(job)

    sharemind_config = {
        "pid": pid,
        "parties": {
            1: {"host": "localhost", "port": 9001},
            2: {"host": "localhost", "port": 9002},
            3: {"host": "localhost", "port": 9003}
        }
    }
    sm_peer = setup_peer(sharemind_config)
    dispatch_all(None, sm_peer, job_queue)

if __name__ == "__main__":

    testHybridJoinWorkflow()
