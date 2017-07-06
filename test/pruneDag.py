import salmon.lang as sal
from salmon.comp import mpc, scotch
from salmon.utils import *


def testSingleConcat():

    @scotch
    @mpc(1)
    def protocol():

        # define inputs
        colsIn1 = [
            defCol("INTEGER", [1]),
            defCol("INTEGER", [1])
        ]
        in1 = sal.create("in1", colsIn1, set([1]))
        colsIn2 = [
            defCol("INTEGER", [2]),
            defCol("INTEGER", [2])
        ]
        in2 = sal.create("in2", colsIn2, set([2]))
        colsIn3 = [
            defCol("INTEGER", [3]),
            defCol("INTEGER", [3])
        ]
        in3 = sal.create("in3", colsIn3, set([3]))

        # combine parties' inputs into one relation
        rel = sal.concat([in1, in2, in3], "rel")

        sal.collect(rel, 1)

        # return root nodes
        return set([in1, in2, in3])

    expected = """CREATE RELATION in1([in1_0 {1}, in1_1 {1}]) {1} WITH COLUMNS (INTEGER, INTEGER)
STORE in2([in2_0 {2}, in2_1 {2}]) {2} INTO in2_store([in2_store_0 {2}, in2_store_1 {2}]) {1}
STORE in3([in3_0 {3}, in3_1 {3}]) {3} INTO in3_store([in3_store_0 {3}, in3_store_1 {3}]) {1}
CONCAT [in1([in1_0 {1}, in1_1 {1}]) {1}, in2_store([in2_store_0 {2}, in2_store_1 {2}]) {1}, in3_store([in3_store_0 {3}, in3_store_1 {3}]) {1}] AS rel([rel_0 {1,2,3}, rel_1 {1,2,3}]) {1}
"""
    actual = protocol()
    assert expected == actual, actual


def testAggProj():

    @scotch
    @mpc(2)
    def protocol():

        # define inputs
        colsIn1 = [
            defCol("INTEGER", [1]),
            defCol("INTEGER", [1])
        ]
        in1 = sal.create("in1", colsIn1, set([1]))
        colsIn2 = [
            defCol("INTEGER", [2]),
            defCol("INTEGER", [2])
        ]
        in2 = sal.create("in2", colsIn2, set([2]))

        # combine parties' inputs into one relation
        rel = sal.concat([in1, in2], "rel")

        # specify the workflow
        projA = sal.project(rel, "projA", ["rel_0", "rel_1"])
        projB = sal.project(projA, "projB", ["projA_0", "projA_1"])
        agg = sal.aggregate(projB, "agg", "projB_0", "projB_1", "+")
        projC = sal.project(agg, "projC", ["agg_0", "agg_1"])

        sal.collect(projC, 1)

        # return root nodes
        return set([in1, in2])

    expected = """STORE agg_0([agg_0_0 {1}, agg_0_1 {1}]) {1} INTO agg_0_store([agg_0_store_0 {1}, agg_0_store_1 {1}]) {1, 2}
CREATE RELATION in2([in2_0 {2}, in2_1 {2}]) {2} WITH COLUMNS (INTEGER, INTEGER)
PROJECT [in2_0, in2_1] FROM (in2([in2_0 {2}, in2_1 {2}]) {2}) AS projA_1([projA_1_0 {2}, projA_1_1 {2}]) {2}
PROJECT [projA_1_0, projA_1_1] FROM (projA_1([projA_1_0 {2}, projA_1_1 {2}]) {2}) AS projB_1([projB_1_0 {2}, projB_1_1 {2}]) {2}
AGG [projB_1_1, +] FROM (projB_1([projB_1_0 {2}, projB_1_1 {2}]) {2}) GROUP BY [projB_1_0] AS agg_1([agg_1_0 {2}, agg_1_1 {2}]) {2}
STORE agg_1([agg_1_0 {2}, agg_1_1 {2}]) {2} INTO agg_1_store([agg_1_store_0 {2}, agg_1_store_1 {2}]) {1, 2}
CONCATMPC [agg_0_store([agg_0_store_0 {1}, agg_0_store_1 {1}]) {1, 2}, agg_1_store([agg_1_store_0 {2}, agg_1_store_1 {2}]) {1, 2}] AS rel([rel_0 {1,2}, rel_1 {1,2}]) {1, 2}
AGGMPC [rel_1, +] FROM (rel([rel_0 {1,2}, rel_1 {1,2}]) {1, 2}) GROUP BY [rel_0] AS agg_obl([agg_obl_0 {1,2}, agg_obl_1 {1,2}]) {1, 2}
STORE agg_obl([agg_obl_0 {1,2}, agg_obl_1 {1,2}]) {1, 2} INTO agg_obl_store([agg_obl_store_0 {1,2}, agg_obl_store_1 {1,2}]) {1}
"""
    actual = protocol()
    assert expected == actual, actual


def testAggProjProj():

    @scotch
    @mpc(2)
    def protocol():

        # define inputs
        colsIn1 = [
            defCol("INTEGER", [1]),
            defCol("INTEGER", [1])
        ]
        in1 = sal.create("in1", colsIn1, set([1]))
        colsIn2 = [
            defCol("INTEGER", [2]),
            defCol("INTEGER", [2])
        ]
        in2 = sal.create("in2", colsIn2, set([2]))

        # combine parties' inputs into one relation
        rel = sal.concat([in1, in2], "rel")

        # specify the workflow
        projA = sal.project(rel, "projA", ["rel_0", "rel_1"])
        projB = sal.project(projA, "projB", ["projA_0", "projA_1"])
        agg = sal.aggregate(projB, "agg", "projB_0", "projB_1", "+")
        projC = sal.project(agg, "projC", ["agg_0", "agg_1"])
        projD = sal.project(projC, "projD", ["projC_0", "projC_1"])

        sal.collect(projD, 1)

        # return root nodes
        return set([in1, in2])

    expected = """STORE agg_0([agg_0_0 {1}, agg_0_1 {1}]) {1} INTO agg_0_store([agg_0_store_0 {1}, agg_0_store_1 {1}]) {1, 2}
CREATE RELATION in2([in2_0 {2}, in2_1 {2}]) {2} WITH COLUMNS (INTEGER, INTEGER)
PROJECT [in2_0, in2_1] FROM (in2([in2_0 {2}, in2_1 {2}]) {2}) AS projA_1([projA_1_0 {2}, projA_1_1 {2}]) {2}
PROJECT [projA_1_0, projA_1_1] FROM (projA_1([projA_1_0 {2}, projA_1_1 {2}]) {2}) AS projB_1([projB_1_0 {2}, projB_1_1 {2}]) {2}
AGG [projB_1_1, +] FROM (projB_1([projB_1_0 {2}, projB_1_1 {2}]) {2}) GROUP BY [projB_1_0] AS agg_1([agg_1_0 {2}, agg_1_1 {2}]) {2}
STORE agg_1([agg_1_0 {2}, agg_1_1 {2}]) {2} INTO agg_1_store([agg_1_store_0 {2}, agg_1_store_1 {2}]) {1, 2}
CONCATMPC [agg_0_store([agg_0_store_0 {1}, agg_0_store_1 {1}]) {1, 2}, agg_1_store([agg_1_store_0 {2}, agg_1_store_1 {2}]) {1, 2}] AS rel([rel_0 {1,2}, rel_1 {1,2}]) {1, 2}
AGGMPC [rel_1, +] FROM (rel([rel_0 {1,2}, rel_1 {1,2}]) {1, 2}) GROUP BY [rel_0] AS agg_obl([agg_obl_0 {1,2}, agg_obl_1 {1,2}]) {1, 2}
STORE agg_obl([agg_obl_0 {1,2}, agg_obl_1 {1,2}]) {1, 2} INTO agg_obl_store([agg_obl_store_0 {1,2}, agg_obl_store_1 {1,2}]) {1}
"""
    actual = protocol()
    assert expected == actual, actual


def testAggProjProjOther():

    @scotch
    @mpc(1)
    def protocol():

        # define inputs
        colsIn1 = [
            defCol("INTEGER", [1]),
            defCol("INTEGER", [1])
        ]
        in1 = sal.create("in1", colsIn1, set([1]))
        colsIn2 = [
            defCol("INTEGER", [2]),
            defCol("INTEGER", [2])
        ]
        in2 = sal.create("in2", colsIn2, set([2]))

        # combine parties' inputs into one relation
        rel = sal.concat([in1, in2], "rel")

        # specify the workflow
        projA = sal.project(rel, "projA", ["rel_0", "rel_1"])
        projB = sal.project(projA, "projB", ["projA_0", "projA_1"])
        agg = sal.aggregate(projB, "agg", "projB_0", "projB_1", "+")
        projC = sal.project(agg, "projC", ["agg_0", "agg_1"])
        projD = sal.project(projC, "projD", ["projC_0", "projC_1"])

        sal.collect(projD, 1)

        # return root nodes
        return set([in1, in2])

    expected = """STORE agg_1([agg_1_0 {2}, agg_1_1 {2}]) {2} INTO agg_1_store([agg_1_store_0 {2}, agg_1_store_1 {2}]) {1, 2}
CREATE RELATION in1([in1_0 {1}, in1_1 {1}]) {1} WITH COLUMNS (INTEGER, INTEGER)
PROJECT [in1_0, in1_1] FROM (in1([in1_0 {1}, in1_1 {1}]) {1}) AS projA_0([projA_0_0 {1}, projA_0_1 {1}]) {1}
PROJECT [projA_0_0, projA_0_1] FROM (projA_0([projA_0_0 {1}, projA_0_1 {1}]) {1}) AS projB_0([projB_0_0 {1}, projB_0_1 {1}]) {1}
AGG [projB_0_1, +] FROM (projB_0([projB_0_0 {1}, projB_0_1 {1}]) {1}) GROUP BY [projB_0_0] AS agg_0([agg_0_0 {1}, agg_0_1 {1}]) {1}
STORE agg_0([agg_0_0 {1}, agg_0_1 {1}]) {1} INTO agg_0_store([agg_0_store_0 {1}, agg_0_store_1 {1}]) {1, 2}
CONCATMPC [agg_0_store([agg_0_store_0 {1}, agg_0_store_1 {1}]) {1, 2}, agg_1_store([agg_1_store_0 {2}, agg_1_store_1 {2}]) {1, 2}] AS rel([rel_0 {1,2}, rel_1 {1,2}]) {1, 2}
AGGMPC [rel_1, +] FROM (rel([rel_0 {1,2}, rel_1 {1,2}]) {1, 2}) GROUP BY [rel_0] AS agg_obl([agg_obl_0 {1,2}, agg_obl_1 {1,2}]) {1, 2}
STORE agg_obl([agg_obl_0 {1,2}, agg_obl_1 {1,2}]) {1, 2} INTO agg_obl_store([agg_obl_store_0 {1,2}, agg_obl_store_1 {1,2}]) {1}
PROJECT [agg_obl_store_0, agg_obl_store_1] FROM (agg_obl_store([agg_obl_store_0 {1,2}, agg_obl_store_1 {1,2}]) {1}) AS projC([projC_0 {1,2}, projC_1 {1,2}]) {1}
PROJECT [projC_0, projC_1] FROM (projC([projC_0 {1,2}, projC_1 {1,2}]) {1}) AS projD([projD_0 {1,2}, projD_1 {1,2}]) {1}
"""
    actual = protocol()
    assert expected == actual, actual

def testJoin():

    @scotch
    @mpc(1)
    def protocol():
        colsInA = [
            defCol("INTEGER", [1]),
            defCol("INTEGER", [1]),
        ]
        inA = sal.create("inA", colsInA, set([1]))
        colsInB = [
            defCol("INTEGER", [2]),
            defCol("INTEGER", [2])
        ]
        inB = sal.create("inB", colsInB, set([2]))
        projB = sal.project(inB, "projB", ["inB_0", "inB_1"])
        joined = sal.join(inA, projB, "joined", "inA_0", "projB_0")
        mult = sal.multiply(joined, "mult", "joined_0", ["joined_0", 0])
        sal.collect(mult, 1)
        return set([inA, inB])

    expected = """CREATE RELATION inA([inA_0 {1}, inA_1 {1}]) {1} WITH COLUMNS (INTEGER, INTEGER)
STORE inA([inA_0 {1}, inA_1 {1}]) {1} INTO inA_store([inA_store_0 {1}, inA_store_1 {1}]) {1, 2}
STORE projB([projB_0 {2}, projB_1 {2}]) {2} INTO projB_store([projB_store_0 {2}, projB_store_1 {2}]) {1, 2}
(inA_store([inA_store_0 {1}, inA_store_1 {1}]) {1, 2}) JOINMPC (projB_store([projB_store_0 {2}, projB_store_1 {2}]) {1, 2}) ON inA_store_0 AND projB_store_0 AS joined([joined_0 {1,2}, joined_1 {1,2}, joined_2 {1,2}]) {1, 2}
MULTIPLYMPC [joined_0 -> joined_0 * 0] FROM (joined([joined_0 {1,2}, joined_1 {1,2}, joined_2 {1,2}]) {1, 2}) AS mult([mult_0 {1,2}, mult_1 {1,2}, mult_2 {1,2}]) {1, 2}
STORE mult([mult_0 {1,2}, mult_1 {1,2}, mult_2 {1,2}]) {1, 2} INTO mult_store([mult_store_0 {1,2}, mult_store_1 {1,2}, mult_store_2 {1,2}]) {1}
"""
    actual = protocol()
    assert expected == actual, actual

if __name__ == "__main__":

    testSingleConcat()
    testAggProj()
    testAggProjProj()
    testAggProjProjOther()
    testJoin()
