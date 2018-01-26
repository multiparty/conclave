import salmon.lang as sal
from salmon.comp import dagonly
from salmon.utils import *
import salmon.partition as part
from salmon.codegen.scotch import ScotchCodeGen
from salmon.codegen.sharemind import SharemindCodeGen, SharemindCodeGenConfig
from salmon.codegen.spark import SparkCodeGen
from salmon.codegen.python import PythonCodeGen
from salmon import generate_code, CodeGenConfig
from salmon.dispatch import dispatch_all
from salmon.net import setup_peer
import sys
import exampleutils
import salmon.dag as saldag


def testHybridJoinWorkflow():

    def hybrid_join():

        # define inputs
        colsInA = [
            defCol("a", "INTEGER", [1]),
            defCol("b", "INTEGER", [1]),
        ]
        in1 = sal.create("in1", colsInA, set([1]))
        in1.isMPC = False

        proja = sal.project(in1, "proja", ["a", "b"])
        proja.isMPC = False
        proja.outRel.storedWith = set([1])

        colsInB = [
            defCol("c", "INTEGER", [1], [2]),
            defCol("d", "INTEGER", [2])
        ]
        in2 = sal.create("in2", colsInB, set([2]))
        in2.isMPC = False

        projb = sal.project(in2, "projb", ["c", "d"])
        projb.isMPC = False
        projb.outRel.storedWith = set([2])

        clA = sal._close(proja, "clA", set([1, 2, 3]))
        clA.isMPC = True
        clB = sal._close(projb, "clB", set([1, 2, 3]))
        clB.isMPC = True

        shuffledA = sal.shuffle(clA, "shuffledA")
        shuffledA.isMPC = True
        persistedA = sal._persist(shuffledA, "persistedA")
        persistedA.isMPC = True
        shuffledB = sal.shuffle(clB, "shuffledB")
        shuffledB.isMPC = True
        persistedB = sal._persist(shuffledB, "persistedB")
        persistedB.isMPC = True

        keysaclosed = sal.project(shuffledA, "keysaclosed", ["a"])
        keysaclosed.outRel.storedWith = set([1, 2, 3])
        keysaclosed.isMPC = True
        keysbclosed = sal.project(shuffledB, "keysbclosed", ["c"])
        keysbclosed.isMPC = True
        keysbclosed.outRel.storedWith = set([1, 2, 3])

        keysa = sal._open(keysaclosed, "keysa", 1)
        keysa.isMPC = True
        keysb = sal._open(keysbclosed, "keysb", 1)
        keysb.isMPC = True

        indexedA = sal.index(keysa, "indexedA", "indexA")
        indexedA.isMPC = False
        indexedA.outRel.storedWith = set([1])
        indexedB = sal.index(keysb, "indexedB", "indexB")
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

        return joined, set([in1, in2])

    def hybrid_agg(in1):

        shuffled = sal.shuffle(in1, "shuffled")
        shuffled.outRel.storedWith = set([1, 2, 3])
        shuffled.isMPC = True

        persisted = sal._persist(shuffled, "persisted")
        persisted.outRel.storedWith = set([1, 2, 3])
        persisted.isMPC = True
        
        keysclosed = sal.project(shuffled, "keysclosed", ["b"])
        keysclosed.outRel.storedWith = set([1, 2, 3])
        keysclosed.isMPC = True
        
        keys = sal._open(keysclosed, "keys", 1)
        keys.isMPC = True
        
        indexed = sal.index(keys, "indexed", "rowIndex")
        indexed.isMPC = False
        indexed.outRel.storedWith = set([1])
        
        distinctKeys = sal.distinct(keys, "distinctKeys", ["b"])
        distinctKeys.isMPC = False
        distinctKeys.outRel.storedWith = set([1])

        # TODO: hack to get keys stored
        # need to fix later!
        fakeDistinctKeys = sal.project(distinctKeys, "distinctKeys", ["b"])
        fakeDistinctKeys.isMPC = False
        fakeDistinctKeys.outRel.storedWith = set([1])

        indexedDistinct = sal.index(distinctKeys, "indexedDistinct", "keyIndex")
        indexedDistinct.isMPC = False
        indexedDistinct.outRel.storedWith = set([1])

        joinedindeces = sal.join(
            indexed, indexedDistinct, "joinedindeces", ["b"], ["b"])
        joinedindeces.isMPC = False
        joinedindeces.outRel.storedWith = set([1])

        # TODO: could project row indeces away too
        indecesonly = sal.project(
            joinedindeces, "indecesonly", ["rowIndex", "keyIndex"])
        indecesonly.isMPC = False
        indecesonly.outRel.storedWith = set([1])

        closedDistinct = sal._close(distinctKeys, "closedDistinct", set([1, 2, 3]))
        closedDistinct.isMPC = True
        closedLookup = sal._close(indecesonly, "closedLookup", set([1, 2, 3]))
        closedLookup.isMPC = True

        agg = sal.index_aggregate(persisted, "agg", ["b"], "d", "+", "d", closedLookup, closedDistinct)
        agg.isMPC = True
        sal._open(agg, "aggopened", 1)

    def protocol():

        joinedres, inputs = hybrid_join()
        hybrid_agg(joinedres)
        return saldag.OpDag(inputs)

    pid = int(sys.argv[1])
    workflow_name = "ssn-" + str(pid)
    sm_cg_config = SharemindCodeGenConfig(
        workflow_name, "/mnt/shared", use_hdfs=False, use_docker=False)
    codegen_config = CodeGenConfig(
        workflow_name).with_sharemind_config(sm_cg_config)
    codegen_config.code_path = "/mnt/shared/" + workflow_name
    codegen_config.input_path = "/mnt/shared"
    codegen_config.output_path = "/mnt/shared"

    exampleutils.generate_ssn_data(pid, codegen_config.output_path)

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

    sharemind_config = exampleutils.get_sharemind_config(pid, True)
    sm_peer = setup_peer(sharemind_config)
    dispatch_all(None, sm_peer, job_queue)
    if pid == 1:
        expected = ['', '1,30', '2,50', '3,30']
        exampleutils.check_res(expected, "/mnt/shared/aggopened.csv")
        print("Success")

if __name__ == "__main__":

    testHybridJoinWorkflow()
