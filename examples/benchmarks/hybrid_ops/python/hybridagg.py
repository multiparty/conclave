import conclave.lang as sal
from conclave.comp import dag_only
from conclave.utils import *
import conclave.partition as part
from conclave.codegen.scotch import ScotchCodeGen
from conclave.codegen.sharemind import SharemindCodeGen, SharemindCodeGenConfig
from conclave.codegen.spark import SparkCodeGen
from conclave.codegen.python import PythonCodeGen
from conclave.codegen.viz import VizCodeGen
from conclave import generate_code, CodeGenConfig
from conclave.dispatch import dispatch_all
from conclave.net import setup_peer
import sys

def testHybridAggWorkflow():

    @dag_only
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
        proja.out_rel.storedWith = set([1])

        # define inputs
        colsInB = [
            defCol("a", "INTEGER", [2]),
            defCol("b", "INTEGER", [2]),
        ]
        in2 = sal.create("in2", colsInB, set([2]))
        in2.isMPC = False

        projb = sal.project(in2, "projb", ["a", "b"])
        projb.isMPC = False
        projb.out_rel.storedWith = set([2])

        # define inputs
        colsInC = [
            defCol("a", "INTEGER", [3]),
            defCol("b", "INTEGER", [3]),
        ]
        in3 = sal.create("in3", colsInC, set([3]))
        in3.isMPC = False

        projc = sal.project(in3, "projc", ["a", "b"])
        projc.isMPC = False
        projc.out_rel.storedWith = set([3])

        clA = sal._close(proja, "clA", set([1, 2, 3]))
        clA.isMPC = True

        clB = sal._close(projb, "clB", set([1, 2, 3]))
        clB.isMPC = True

        clC = sal._close(projc, "clC", set([1, 2, 3]))
        clC.isMPC = True

        comb = sal.concat([clA, clB, clC], "comb")
        comb.out_rel.storedWith = set([1, 2, 3])
        comb.isMPC = True

        shuffled = sal.shuffle(comb, "shuffled")
        shuffled.out_rel.storedWith = set([1, 2, 3])
        shuffled.isMPC = True

        persisted = sal._persist(shuffled, "persisted")
        persisted.out_rel.storedWith = set([1, 2, 3])
        persisted.isMPC = True

        keysclosed = sal.project(shuffled, "keysclosed", ["a"])
        keysclosed.out_rel.storedWith = set([1, 2, 3])
        keysclosed.isMPC = True

        keys = sal._open(keysclosed, "keys", 1)
        keys.isMPC = True

        indexed = sal.index(keys, "indexed", "rowIndex")
        indexed.isMPC = False
        indexed.out_rel.storedWith = set([1])

        sortedByKey = sal.sort_by(indexed, "sortedByKey", "a")
        sortedByKey.isMPC = False
        sortedByKey.out_rel.storedWith = set([1])

        eqFlags = sal._comp_neighs(sortedByKey, "eqFlags", "a")
        eqFlags.isMPC = False
        eqFlags.out_rel.storedWith = set([1])

        # TODO: hack to get keys stored
        # need to fix later!
        sortedByKey = sal.project(sortedByKey, "sortedByKey", ["rowIndex", "a"])
        sortedByKey.isMPC = False
        sortedByKey.out_rel.storedWith = set([1])

        closedEqFlags = sal._close(eqFlags, "closedEqFlags", set([1, 2, 3]))
        closedEqFlags.isMPC = True
        closedSortedByKey = sal._close(sortedByKey, "closedSortedByKey", set([1, 2, 3]))
        closedSortedByKey.isMPC = True
        
        agg = sal.index_aggregate(persisted, "agg", ["a"], "b", "+", "b", closedEqFlags, closedSortedByKey)
        agg.out_rel.storedWith = set([1, 2, 3])
        agg.isMPC = True

        sal._open(agg, "opened", 1)

        # create condag
        return set([in1, in2, in3])

    pid = int(sys.argv[1])
    size = sys.argv[2]

    workflow_name = "hybrid-agg-" + str(pid)
    sm_cg_config = SharemindCodeGenConfig(
        workflow_name, "/mnt/shared", use_hdfs=False, use_docker=True)
    codegen_config = CodeGenConfig(
        workflow_name).with_sharemind_config(sm_cg_config)
    codegen_config.code_path = "/mnt/shared/" + workflow_name
    codegen_config.input_path = "/mnt/shared/" + size
    codegen_config.output_path = "/mnt/shared/" + size

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
            1: {"host": "ca-spark-node-0", "port": 9001},
            2: {"host": "cb-spark-node-0", "port": 9002},
            3: {"host": "cc-spark-node-0", "port": 9003}
        }
    }
    sm_peer = setup_peer(sharemind_config)
    dispatch_all(None, sm_peer, job_queue)
    
if __name__ == "__main__":

    testHybridAggWorkflow()
