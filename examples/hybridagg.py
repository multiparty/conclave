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
import exampleutils


def testHybridAggWorkflow():

    @dagonly
    def protocol():

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

        clA = sal._close(proja, "clA", set([1, 2, 3]))
        clA.isMPC = True

        shuffled = sal.shuffle(clA, "shuffled")
        shuffled.outRel.storedWith = set([1, 2, 3])
        shuffled.isMPC = True

        persisted = sal._persist(shuffled, "persisted")
        persisted.outRel.storedWith = set([1, 2, 3])
        persisted.isMPC = True

        keysclosed = sal.project(shuffled, "keysclosed", ["a"])
        keysclosed.outRel.storedWith = set([1, 2, 3])
        keysclosed.isMPC = True

        keys = sal._open(keysclosed, "keys", 1)
        keys.isMPC = True

        indexed = sal.index(keys, "indexed", "rowIndex")
        indexed.isMPC = False
        indexed.outRel.storedWith = set([1])

        distinctKeys = sal.distinct(keys, "distinctKeys", ["a"])
        distinctKeys.isMPC = False
        distinctKeys.outRel.storedWith = set([1])

        # TODO: hack to get keys stored
        # need to fix later!
        fakeDistinctKeys = sal.distinct(keys, "distinctKeys", ["a"])
        fakeDistinctKeys.isMPC = False
        fakeDistinctKeys.outRel.storedWith = set([1])

        indexedDistinct = sal.index(distinctKeys, "indexedDistinct", "keyIndex")
        indexedDistinct.isMPC = False
        indexedDistinct.outRel.storedWith = set([1])

        joinedindeces = sal.join(
            indexed, indexedDistinct, "joinedindeces", ["a"], ["a"])
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

        agg = sal.index_aggregate(persisted, "agg", ["a"], "b", "+", "b", closedLookup, closedDistinct)
        agg.isMPC = True
        sal._open(agg, "opened", 1)

        # create dag
        return set([in1])

    pid = int(sys.argv[1])
    workflow_name = "hybrid-agg-" + str(pid)
    sm_cg_config = SharemindCodeGenConfig(
        workflow_name, "/mnt/shared", use_hdfs=False, use_docker=False)
    codegen_config = CodeGenConfig(
        workflow_name).with_sharemind_config(sm_cg_config)
    codegen_config.code_path = "/mnt/shared/" + workflow_name
    codegen_config.input_path = "/mnt/shared"
    codegen_config.output_path = "/mnt/shared"

    exampleutils.generate_agg_data(pid, codegen_config.output_path)

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
        expected = ['', '1,2000', '2,242', '3,300', '5,500', '7,2400', '9,1100']
        exampleutils.check_res(expected, "/mnt/shared/opened.csv")
        print("Success")

if __name__ == "__main__":

    testHybridAggWorkflow()
