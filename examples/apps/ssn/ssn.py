import sys

import salmon.lang as sal
from salmon import CodeGenConfig
from salmon import generate_and_dispatch
from salmon.codegen.sharemind import SharemindCodeGenConfig
from salmon.utils import *


def run_ssn_workflow():
    def hybrid_join():
        # define inputs
        colsInA = [
            defCol("a", "INTEGER", [1]),
            defCol("b", "INTEGER", [1]),
        ]
        in1 = sal.create("govreg", colsInA, {1})
        in1.isMPC = False

        proja = sal.project(in1, "proja", ["a", "b"])
        proja.isMPC = False
        proja.out_rel.storedWith = {1}

        colsInB = [
            defCol("c", "INTEGER", [1], [2]),
            defCol("d", "INTEGER", [2])
        ]
        in2 = sal.create("company0", colsInB, {2})
        in2.isMPC = False

        projb = sal.project(in2, "projb", ["c", "d"])
        projb.isMPC = False
        projb.out_rel.storedWith = {2}

        colsInC = [
            defCol("c", "INTEGER", [1], [3]),
            defCol("d", "INTEGER", [3])
        ]
        in3 = sal.create("company1", colsInC, {3})
        in3.isMPC = False

        projc = sal.project(in3, "projc", ["c", "d"])
        projc.isMPC = False
        projc.out_rel.storedWith = {3}

        clA = sal._close(proja, "clA", {1, 2, 3})
        clA.isMPC = True
        clB = sal._close(projb, "clB", {1, 2, 3})
        clB.isMPC = True
        clC = sal._close(projc, "clC", {1, 2, 3})
        clC.isMPC = True

        rightClosed = sal.concat([clB, clC], "clD")
        rightClosed.isMPC = True
        rightClosed.out_rel.storedWith = {1, 2, 3}

        shuffledA = sal.shuffle(clA, "shuffledA")
        shuffledA.isMPC = True
        persistedA = sal._persist(shuffledA, "persistedA")
        persistedA.isMPC = True
        shuffledB = sal.shuffle(rightClosed, "shuffledB")
        shuffledB.isMPC = True
        persistedB = sal._persist(shuffledB, "persistedB")
        persistedB.isMPC = True

        keysaclosed = sal.project(shuffledA, "keysaclosed", ["a"])
        keysaclosed.out_rel.storedWith = {1, 2, 3}
        keysaclosed.isMPC = True
        keysbclosed = sal.project(shuffledB, "keysbclosed", ["c"])
        keysbclosed.isMPC = True
        keysbclosed.out_rel.storedWith = {1, 2, 3}

        keysa = sal._open(keysaclosed, "keysa", 1)
        keysa.isMPC = True
        keysb = sal._open(keysbclosed, "keysb", 1)
        keysb.isMPC = True

        indexedA = sal.index(keysa, "indexedA", "indexA")
        indexedA.isMPC = False
        indexedA.out_rel.storedWith = {1}
        indexedB = sal.index(keysb, "indexedB", "indexB")
        indexedB.isMPC = False
        indexedB.out_rel.storedWith = {1}

        joinedindeces = sal.join(
            indexedA, indexedB, "joinedindeces", ["a"], ["c"])
        joinedindeces.isMPC = False
        joinedindeces.out_rel.storedWith = {1}

        indecesonly = sal.project(
            joinedindeces, "indecesonly", ["indexA", "indexB"])
        indecesonly.isMPC = False
        indecesonly.out_rel.storedWith = {1}

        indecesclosed = sal._close(
            indecesonly, "indecesclosed", {1, 2, 3})
        indecesclosed.isMPC = True

        joined = sal._index_join(persistedA, persistedB, "joined", [
            "a"], ["c"], indecesclosed)
        joined.isMPC = True

        return joined, {in1, in2, in3}

    def hybrid_agg(in1):
        shuffled = sal.shuffle(in1, "shuffled")
        shuffled.out_rel.storedWith = {1, 2, 3}
        shuffled.isMPC = True

        persisted = sal._persist(shuffled, "persisted")
        persisted.out_rel.storedWith = {1, 2, 3}
        persisted.isMPC = True

        keysclosed = sal.project(shuffled, "keysclosed", ["b"])
        keysclosed.out_rel.storedWith = {1, 2, 3}
        keysclosed.isMPC = True

        keys = sal._open(keysclosed, "keys", 1)
        keys.isMPC = True

        indexed = sal.index(keys, "indexed", "rowIndex")
        indexed.isMPC = False
        indexed.out_rel.storedWith = {1}

        sortedByKey = sal.sort_by(indexed, "sortedByKey", "b")
        sortedByKey.isMPC = False
        sortedByKey.out_rel.storedWith = {1}

        eqFlags = sal._comp_neighs(sortedByKey, "eqFlags", "b")
        eqFlags.isMPC = False
        eqFlags.out_rel.storedWith = {1}

        # TODO: should be a persist op
        sortedByKeyStored = sal.project(
            sortedByKey, "sortedByKeyStored", ["rowIndex", "b"])
        sortedByKeyStored.isMPC = False
        sortedByKeyStored.out_rel.storedWith = {1}

        closedEqFlags = sal._close(eqFlags, "closedEqFlags", {1, 2, 3})
        closedEqFlags.isMPC = True
        closedSortedByKey = sal._close(sortedByKeyStored, "closedSortedByKey", {1, 2, 3})
        closedSortedByKey.isMPC = True

        agg = sal.index_aggregate(persisted, "agg", ["b"], "d", "+", "d", closedEqFlags, closedSortedByKey)
        agg.isMPC = True
        sal._open(agg, "ssnopened", 1)

    def protocol():
        joined_res, inputs = hybrid_join()
        hybrid_agg(joined_res)
        return inputs

    pid = int(sys.argv[1])
    workflow_name = "ssn-" + str(pid)

    sm_config = SharemindCodeGenConfig("/mnt/shared", use_hdfs=False, use_docker=True)
    conclave_config = CodeGenConfig(
        workflow_name).with_sharemind_config(sm_config)
    conclave_config.pid = pid
    conclave_config.all_pids = [1, 2, 3]
    conclave_config.code_path = "/mnt/shared/" + workflow_name
    conclave_config.input_path = "/mnt/shared/ssn-data-small"
    conclave_config.output_path = "/mnt/shared/ssn-data-small"
    network_config = {
        "pid": pid,
        "parties": {
            1: {"host": "ca-spark-node-0", "port": 9001},
            2: {"host": "cb-spark-node-0", "port": 9002},
            3: {"host": "cc-spark-node-0", "port": 9003}
        }
    }
    conclave_config.with_network_config(network_config)

    # run code generation (without rewrite passes) and dispatch
    generate_and_dispatch(protocol, conclave_config, ["sharemind"], ["python"], apply_optimizations=False)


if __name__ == "__main__":
    run_ssn_workflow()
