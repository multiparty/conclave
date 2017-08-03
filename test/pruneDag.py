import salmon.lang as sal
from salmon.comp import mpc, scotch
from salmon.utils import *


def testSingleConcat():

    @scotch
    @mpc(1)
    def protocol():

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
            defCol("e", "INTEGER", [3]),
            defCol("f", "INTEGER", [3])
        ]
        in3 = sal.create("in3", colsIn3, set([3]))

        # combine parties' inputs into one relation
        rel = sal.concat([in1, in2, in3], "rel", ["x", "y"])

        sal.collect(rel, 1)

        # return root nodes
        return set([in1, in2, in3])

    expected = """CREATE RELATION in1([a {1}, b {1}]) {1} WITH COLUMNS (INTEGER, INTEGER)
CLOSE in2([c {2}, d {2}]) {2} INTO in2_close([c {2}, d {2}]) {1}
CLOSE in3([e {3}, f {3}]) {3} INTO in3_close([e {3}, f {3}]) {1}
CONCAT [in1([a {1}, b {1}]) {1}, in2_close([c {2}, d {2}]) {1}, in3_close([e {3}, f {3}]) {1}] AS rel([x {1,2,3}, y {1,2,3}]) {1}
"""
    actual = protocol()
    assert expected == actual, actual


def testAggProj():

    @scotch
    @mpc(2)
    def protocol():

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

        # combine parties' inputs into one relation
        rel = sal.concat([in1, in2], "rel")

        # specify the workflow
        projA = sal.project(rel, "projA", ["a", "b"])
        projB = sal.project(projA, "projB", ["a", "b"])
        agg = sal.aggregate(projB, "agg", "a", "b", "+", "total_b")
        projC = sal.project(agg, "projC", ["a", "total_b"])

        sal.collect(projC, 1)

        # return root nodes
        return set([in1, in2])

    expected = """CLOSE agg_0([a {1}, total_b {1}]) {1} INTO agg_0_close([a {1}, total_b {1}]) {1, 2}
CREATE RELATION in2([a {2}, b {2}]) {2} WITH COLUMNS (INTEGER, INTEGER)
PROJECT [a, b] FROM (in2([a {2}, b {2}]) {2}) AS projA_1([a {2}, b {2}]) {2}
PROJECT [a, b] FROM (projA_1([a {2}, b {2}]) {2}) AS projB_1([a {2}, b {2}]) {2}
AGG [b, +] FROM (projB_1([a {2}, b {2}]) {2}) GROUP BY [a] AS agg_1([a {2}, total_b {2}]) {2}
CLOSE agg_1([a {2}, total_b {2}]) {2} INTO agg_1_close([a {2}, total_b {2}]) {1, 2}
CONCATMPC [agg_0_close([a {1}, total_b {1}]) {1, 2}, agg_1_close([a {2}, total_b {2}]) {1, 2}] AS rel([a {1,2}, b {1,2}]) {1, 2}
AGGMPC [b, +] FROM (rel([a {1,2}, b {1,2}]) {1, 2}) GROUP BY [a] AS agg_obl([a {1,2}, total_b {1,2}]) {1, 2}
OPEN agg_obl([a {1,2}, total_b {1,2}]) {1, 2} INTO agg_obl_open([a {1,2}, total_b {1,2}]) {1}
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

    expected = """CLOSE agg_0([agg_0_0 {1}, agg_0_1 {1}]) {1} INTO agg_0_close([agg_0_close_0 {1}, agg_0_close_1 {1}]) {1, 2}
CREATE RELATION in2([in2_0 {2}, in2_1 {2}]) {2} WITH COLUMNS (INTEGER, INTEGER)
PROJECT [in2_0, in2_1] FROM (in2([in2_0 {2}, in2_1 {2}]) {2}) AS projA_1([projA_1_0 {2}, projA_1_1 {2}]) {2}
PROJECT [projA_1_0, projA_1_1] FROM (projA_1([projA_1_0 {2}, projA_1_1 {2}]) {2}) AS projB_1([projB_1_0 {2}, projB_1_1 {2}]) {2}
AGG [projB_1_1, +] FROM (projB_1([projB_1_0 {2}, projB_1_1 {2}]) {2}) GROUP BY [projB_1_0] AS agg_1([agg_1_0 {2}, agg_1_1 {2}]) {2}
CLOSE agg_1([agg_1_0 {2}, agg_1_1 {2}]) {2} INTO agg_1_close([agg_1_close_0 {2}, agg_1_close_1 {2}]) {1, 2}
CONCATMPC [agg_0_close([agg_0_close_0 {1}, agg_0_close_1 {1}]) {1, 2}, agg_1_close([agg_1_close_0 {2}, agg_1_close_1 {2}]) {1, 2}] AS rel([rel_0 {1,2}, rel_1 {1,2}]) {1, 2}
AGGMPC [rel_1, +] FROM (rel([rel_0 {1,2}, rel_1 {1,2}]) {1, 2}) GROUP BY [rel_0] AS agg_obl([agg_obl_0 {1,2}, agg_obl_1 {1,2}]) {1, 2}
OPEN agg_obl([agg_obl_0 {1,2}, agg_obl_1 {1,2}]) {1, 2} INTO agg_obl_open([agg_obl_open_0 {1,2}, agg_obl_open_1 {1,2}]) {1}
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

    expected = """CLOSE agg_1([agg_1_0 {2}, agg_1_1 {2}]) {2} INTO agg_1_close([agg_1_close_0 {2}, agg_1_close_1 {2}]) {1, 2}
CREATE RELATION in1([in1_0 {1}, in1_1 {1}]) {1} WITH COLUMNS (INTEGER, INTEGER)
PROJECT [in1_0, in1_1] FROM (in1([in1_0 {1}, in1_1 {1}]) {1}) AS projA_0([projA_0_0 {1}, projA_0_1 {1}]) {1}
PROJECT [projA_0_0, projA_0_1] FROM (projA_0([projA_0_0 {1}, projA_0_1 {1}]) {1}) AS projB_0([projB_0_0 {1}, projB_0_1 {1}]) {1}
AGG [projB_0_1, +] FROM (projB_0([projB_0_0 {1}, projB_0_1 {1}]) {1}) GROUP BY [projB_0_0] AS agg_0([agg_0_0 {1}, agg_0_1 {1}]) {1}
CLOSE agg_0([agg_0_0 {1}, agg_0_1 {1}]) {1} INTO agg_0_close([agg_0_close_0 {1}, agg_0_close_1 {1}]) {1, 2}
CONCATMPC [agg_0_close([agg_0_close_0 {1}, agg_0_close_1 {1}]) {1, 2}, agg_1_close([agg_1_close_0 {2}, agg_1_close_1 {2}]) {1, 2}] AS rel([rel_0 {1,2}, rel_1 {1,2}]) {1, 2}
AGGMPC [rel_1, +] FROM (rel([rel_0 {1,2}, rel_1 {1,2}]) {1, 2}) GROUP BY [rel_0] AS agg_obl([agg_obl_0 {1,2}, agg_obl_1 {1,2}]) {1, 2}
OPEN agg_obl([agg_obl_0 {1,2}, agg_obl_1 {1,2}]) {1, 2} INTO agg_obl_open([agg_obl_open_0 {1,2}, agg_obl_open_1 {1,2}]) {1}
PROJECT [agg_obl_open_0, agg_obl_open_1] FROM (agg_obl_open([agg_obl_open_0 {1,2}, agg_obl_open_1 {1,2}]) {1}) AS projC([projC_0 {1,2}, projC_1 {1,2}]) {1}
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
CLOSE inA([inA_0 {1}, inA_1 {1}]) {1} INTO inA_close([inA_close_0 {1}, inA_close_1 {1}]) {1, 2}
CLOSE projB([projB_0 {2}, projB_1 {2}]) {2} INTO projB_close([projB_close_0 {2}, projB_close_1 {2}]) {1, 2}
(inA_close([inA_close_0 {1}, inA_close_1 {1}]) {1, 2}) JOINMPC (projB_close([projB_close_0 {2}, projB_close_1 {2}]) {1, 2}) ON inA_close_0 AND projB_close_0 AS joined([joined_0 {1,2}, joined_1 {1,2}, joined_2 {1,2}]) {1, 2}
MULTIPLYMPC [joined_0 -> joined_0 * 0] FROM (joined([joined_0 {1,2}, joined_1 {1,2}, joined_2 {1,2}]) {1, 2}) AS mult([mult_0 {1,2}, mult_1 {1,2}, mult_2 {1,2}]) {1, 2}
OPEN mult([mult_0 {1,2}, mult_1 {1,2}, mult_2 {1,2}]) {1, 2} INTO mult_open([mult_open_0 {1,2}, mult_open_1 {1,2}, mult_open_2 {1,2}]) {1}
"""
    actual = protocol()
    assert expected == actual, actual

if __name__ == "__main__":

    testSingleConcat()
    testAggProj()
    testAggProjProj()
    testAggProjProjOther()
    testJoin()
