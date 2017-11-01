import salmon.lang as sal
import salmon.dag as saldag
from salmon.comp import dagonly, mpc
from salmon.utils import *
from salmon.codegen.scotch import ScotchCodeGen
import salmon.partition as part


def test_partition_taxi():

    @mpc(1)
    def protocol():
        colsIn1 = [
            defCol("companyID", "INTEGER", [1]),
            defCol("price", "INTEGER", [1])
        ]
        in1 = sal.create("yellow1", colsIn1, set([1]))
        colsIn2 = [
            defCol("companyID", "INTEGER", [2]),
            defCol("price", "INTEGER", [2])
        ]
        in2 = sal.create("yellow2", colsIn2, set([2]))
        colsIn3 = [
            defCol("companyID", "INTEGER", [3]),
            defCol("price", "INTEGER", [3])
        ]
        in3 = sal.create("yellow3", colsIn3, set([3]))

        cab_data = sal.concat([in1, in2, in3], "cab_data")

        selected_input = sal.project(
            cab_data, "selected_input", ["companyID", "price"])
        local_rev = sal.aggregate(selected_input, "local_rev", [
                                  "companyID"], "price", "+", "local_rev")
        scaled_down = sal.divide(
            local_rev, "scaled_down", "local_rev", ["local_rev", 1000])
        first_val_blank = sal.multiply(
            scaled_down, "first_val_blank", "companyID", ["companyID", 0])
        local_rev_scaled = sal.multiply(
            first_val_blank, "local_rev_scaled", "local_rev", ["local_rev", 100])
        total_rev = sal.aggregate(first_val_blank, "total_rev", [
                                  "companyID"], "local_rev", "+", "global_rev")
        local_total_rev = sal.join(local_rev_scaled, total_rev, "local_total_rev", [
                                   "companyID"], ["companyID"])
        market_share = sal.divide(local_total_rev, "market_share", "local_rev", [
                                  "local_rev", "global_rev"])
        market_share_squared = sal.multiply(market_share, "market_share_squared", "local_rev",
                                            ["local_rev", "local_rev", 1])
        hhi = sal.aggregate(market_share_squared, "hhi", [
                            "companyID"], "local_rev", "+", "hhi")
        # dummy projection to force non-mpc subdag
        hhi_only = sal.project(
            hhi, "hhi_only", ["companyID", "hhi"])

        sal.collect(hhi_only, 1)

        # return root nodes
        return set([in1, in2, in3])

    dag = protocol()
    mapping = part.heupart(dag, ["sharemind"], ["spark"])
    expected = '''sparkcreate->yellow1,
project->selected_input_0,
aggregation->local_rev_0,
divide->scaled_down_0_0{1}###sparkcreate->yellow2,
project->selected_input_1,
aggregation->local_rev_1,
divide->scaled_down_0_1{2}###sparkcreate->yellow3,
project->selected_input_2,
aggregation->local_rev_2,
divide->scaled_down_0_2{3}###sharemindcreatempc->scaled_down_0_0,
closempc->scaled_down_0_0_close,
creatempc->scaled_down_0_1,
closempc->scaled_down_0_1_close,
creatempc->scaled_down_0_2,
closempc->scaled_down_0_2_close,
concatmpc->cab_data,
aggregationmpc->local_rev_obl,
multiplympc->first_val_blank,
multiplympc->local_rev_scaled,
aggregationmpc->total_rev,
joinmpc->local_total_rev,
dividempc->market_share,
multiplympc->market_share_squared,
aggregationmpc->hhi,
openmpc->hhi_open{1, 2, 3}###sparkcreate->hhi_open,
project->hhi_only{1}'''
    actual = "###".join([fmwk + str(subdag) + str(parties)
                         for (fmwk, subdag, parties) in mapping])
    assert expected == actual, actual


def test_partition_ssn():

    def hybrid_join():

        # define inputs
        colsInA = [
            defCol("a", "INTEGER", [1]),
            defCol("b", "INTEGER", [1]),
        ]
        in1 = sal.create("govreg", colsInA, set([1]))
        in1.isMPC = False

        proja = sal.project(in1, "proja", ["a", "b"])
        proja.isMPC = False
        proja.outRel.storedWith = set([1])

        colsInB = [
            defCol("c", "INTEGER", [1], [2]),
            defCol("d", "INTEGER", [2])
        ]
        in2 = sal.create("company0", colsInB, set([2]))
        in2.isMPC = False

        projb = sal.project(in2, "projb", ["c", "d"])
        projb.isMPC = False
        projb.outRel.storedWith = set([2])

        colsInC = [
            defCol("c", "INTEGER", [1], [3]),
            defCol("d", "INTEGER", [3])
        ]
        in3 = sal.create("company1", colsInC, set([3]))
        in3.isMPC = False

        projc = sal.project(in3, "projc", ["c", "d"])
        projc.isMPC = False
        projc.outRel.storedWith = set([3])

        clA = sal._close(proja, "clA", set([1, 2, 3]))
        clA.isMPC = True
        clB = sal._close(projb, "clB", set([1, 2, 3]))
        clB.isMPC = True
        clC = sal._close(projc, "clC", set([1, 2, 3]))
        clC.isMPC = True

        rightClosed = sal.concat([clB, clC], "clD")
        rightClosed.isMPC = True
        rightClosed.outRel.storedWith = set([1, 2, 3])

        shuffledA = sal.shuffle(clA, "shuffledA")
        shuffledA.isMPC = True
        persistedA = sal._persist(shuffledA, "persistedA")
        persistedA.isMPC = True
        shuffledB = sal.shuffle(rightClosed, "shuffledB")
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

        return joined, set([in1, in2, in3])

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

        sortedByKey = sal.sort_by(indexed, "sortedByKey", "b")
        sortedByKey.isMPC = False
        sortedByKey.outRel.storedWith = set([1])

        eqFlags = sal._comp_neighs(sortedByKey, "eqFlags", "b")
        eqFlags.isMPC = False
        eqFlags.outRel.storedWith = set([1])

        # TODO: should be a persist op
        sortedByKeyStored = sal.project(
            sortedByKey, "sortedByKeyStored", ["rowIndex", "b"])
        sortedByKeyStored.isMPC = False
        sortedByKeyStored.outRel.storedWith = set([1])

        closedEqFlags = sal._close(eqFlags, "closedEqFlags", set([1, 2, 3]))
        closedEqFlags.isMPC = True
        closedSortedByKey = sal._close(
            sortedByKeyStored, "closedSortedByKey", set([1, 2, 3]))
        closedSortedByKey.isMPC = True

        agg = sal.index_aggregate(
            persisted, "agg", ["b"], "d", "+", "d", closedEqFlags, closedSortedByKey)
        agg.isMPC = True
        sal._open(agg, "ssnopened", 1)

    def protocol():

        joinedres, inputs = hybrid_join()
        hybrid_agg(joinedres)
        return saldag.OpDag(inputs)

    dag = protocol()
    mapping = part.heupart(dag, ["sharemind"], ["python"])
    expected = '''pythoncreate->company0,
project->projb{2}###pythoncreate->company1,
project->projc{3}###pythoncreate->govreg,
project->proja{1}###sharemindcreatempc->proja,
closempc->clA,
creatempc->projb,
closempc->clB,
creatempc->projc,
closempc->clC,
concatmpc->clD,
shufflempc->shuffledA,
persistmpc->persistedA,
projectmpc->keysaclosed,
openmpc->keysa,
shufflempc->shuffledB,
persistmpc->persistedB,
projectmpc->keysbclosed,
openmpc->keysb{1, 2, 3}###pythoncreate->keysa,
index->indexedA,
create->keysb,
index->indexedB,
join->joinedindeces,
project->indecesonly{1}###sharemindcreatempc->indecesonly,
closempc->indecesclosed,
creatempc->persistedA,
creatempc->persistedB,
indexJoinmpc->joined,
shufflempc->shuffled,
persistmpc->persisted,
projectmpc->keysclosed,
openmpc->keys{1, 2, 3}###pythoncreate->keys,
index->indexed,
sortBy->sortedByKey,
compNeighs->eqFlags,
project->sortedByKeyStored{1}###sharemindcreatempc->eqFlags,
closempc->closedEqFlags,
creatempc->persisted,
creatempc->sortedByKeyStored,
closempc->closedSortedByKey,
aggregationmpc->agg,
openmpc->ssnopened{1, 2, 3}'''
    actual = "###".join([fmwk + str(subdag) + str(parties)
                         for (fmwk, subdag, parties) in mapping])
    assert expected == actual, actual


def test_inputs_out_of_order():

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

        colsInB = [
            defCol("c", "INTEGER", [1], [2]),
            defCol("d", "INTEGER", [2])
        ]
        in2 = sal.create("in2", colsInB, set([2]))
        in2.isMPC = False

        projb = sal.project(in2, "projb", ["c", "d"])
        projb.isMPC = False
        projb.outRel.storedWith = set([2])

        colsInC = [
            defCol("c", "INTEGER", [1], [3]),
            defCol("d", "INTEGER", [3])
        ]
        in3 = sal.create("beforeOthers", colsInC, set([1, 2, 3]))
        in3.isMPC = True

        clA = sal._close(proja, "clA", set([1, 2, 3]))
        clA.isMPC = True
        clB = sal._close(projb, "clB", set([1, 2, 3]))
        clB.isMPC = True
        clC = sal._close(in3, "clC", set([1, 2, 3]))
        clC.isMPC = True

        rightClosed = sal.concat([clA, clB, clC], "a")
        rightClosed.isMPC = True
        rightClosed.outRel.storedWith = set([1, 2, 3])

        shuffledA = sal.shuffle(clA, "shuffledA")
        shuffledA.isMPC = True
        sal._open(shuffledA, "ssnopened", 1)

        return saldag.OpDag(set([in1, in2, in3]))

    dag = protocol()
    mapping = part.heupart(dag, ["sharemind"], ["python"])
    expected = '''pythoncreate->in1,
project->proja{1}###pythoncreate->in2,
project->projb{2}###sharemindcreatempc->beforeOthers,
closempc->clC,
creatempc->proja,
closempc->clA,
creatempc->projb,
closempc->clB,
concatmpc->a,
shufflempc->shuffledA,
openmpc->ssnopened{1, 2, 3}'''
    actual = "###".join([fmwk + str(subdag) + str(parties)
                         for (fmwk, subdag, parties) in mapping])
    assert expected == actual, actual

if __name__ == "__main__":

    test_partition_taxi()
    test_partition_ssn()
    test_inputs_out_of_order()
