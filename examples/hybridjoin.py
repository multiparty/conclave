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


def testHybridJoinWorkflow():

    @dagonly
    def protocol():
        # define inputs
        colsInA = [
            defCol("a", "INTEGER", [1]),
            defCol("b", "INTEGER", [1]),
        ]
        inA = sal.create("inA", colsInA, set([1]))
        inA.isMPC = True

        colsInB = [
            defCol("c", "INTEGER", [1], [2]),
            defCol("d", "INTEGER", [2])
        ]
        inB = sal.create("inB", colsInB, set([2]))
        inB.isMPC = True

        clA = sal._close(inA, "clA", set([1, 2, 3]))
        clA.isMPC = True
        clB = sal._close(inB, "clB", set([1, 2, 3]))
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
        return set([inA, inB])

    job_name = "job-1"
    sm_cg_config = SharemindCodeGenConfig(job_name, "/mnt/shared")
    codegen_config = CodeGenConfig(
        job_name).with_sharemind_config(sm_cg_config)
    codegen_config.code_path = "/mnt/shared/" + job_name
    codegen_config.input_path = "/mnt/shared"
    codegen_config.output_path = "/mnt/shared"

    dag = pruneDag(protocol(), 1)
    mapping = part.heupart(dag, ["sharemind"], ["python"])
    for fmwk, subdag in mapping:
        print("dag dag dag")
        print(fmwk, subdag)
        print()
        if fmwk == "sharemind":
            job, code = SharemindCodeGen(codegen_config, subdag, 1)._generate(None, None)
            # print(code["miner"])
        else:
            job, code = PythonCodeGen(codegen_config, subdag)._generate(None, None)
            print(code)


if __name__ == "__main__":

    testHybridJoinWorkflow()
