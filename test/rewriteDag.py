import salmon.lang as sal
from salmon.comp import mpc, scotch
from salmon.utils import *


def testSingleConcat():

    @scotch
    @mpc
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
        colsIn3 = [
            defCol("a", "INTEGER", [3]),
            defCol("b", "INTEGER", [3])
        ]
        in3 = sal.create("in3", colsIn3, set([3]))

        # combine parties' inputs into one relation
        rel = sal.concat([in1, in2, in3], "rel")

        sal.collect(rel, 1)

        # return root nodes
        return set([in1, in2, in3])

    expected = """CREATE RELATION in1([a {1}, b {1}]) {1} WITH COLUMNS (INTEGER, INTEGER)
CREATE RELATION in2([a {2}, b {2}]) {2} WITH COLUMNS (INTEGER, INTEGER)
CLOSE in2([a {2}, b {2}]) {2} INTO in2_close([a {2}, b {2}]) {1}
CREATE RELATION in3([a {3}, b {3}]) {3} WITH COLUMNS (INTEGER, INTEGER)
CLOSE in3([a {3}, b {3}]) {3} INTO in3_close([a {3}, b {3}]) {1}
CONCAT [in1([a {1}, b {1}]) {1}, in2_close([a {2}, b {2}]) {1}, in3_close([a {3}, b {3}]) {1}] AS rel([a {1,2,3}, b {1,2,3}]) {1}
"""
    actual = protocol()
    assert expected == actual, actual


def testSingleAgg():

    @scotch
    @mpc
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
        agg = sal.aggregate(rel, "agg", "a", "b", "+", "total_b")

        sal.collect(agg, 1)

        # return root nodes
        return set([in1, in2])

    expected = """CREATE RELATION in1([in1_0 {1}, in1_1 {1}]) {1} WITH COLUMNS (INTEGER, INTEGER)
AGG [in1_1, +] FROM (in1([in1_0 {1}, in1_1 {1}]) {1}) GROUP BY [in1_0] AS agg_0([agg_0_0 {1}, agg_0_1 {1}]) {1}
CLOSE agg_0([agg_0_0 {1}, agg_0_1 {1}]) {1} INTO agg_0_close([agg_0_close_0 {1}, agg_0_close_1 {1}]) {1, 2}
CREATE RELATION in2([in2_0 {2}, in2_1 {2}]) {2} WITH COLUMNS (INTEGER, INTEGER)
AGG [in2_1, +] FROM (in2([in2_0 {2}, in2_1 {2}]) {2}) GROUP BY [in2_0] AS agg_1([agg_1_0 {2}, agg_1_1 {2}]) {2}
CLOSE agg_1([agg_1_0 {2}, agg_1_1 {2}]) {2} INTO agg_1_close([agg_1_close_0 {2}, agg_1_close_1 {2}]) {1, 2}
CONCATMPC [agg_0_close([agg_0_close_0 {1}, agg_0_close_1 {1}]) {1, 2}, agg_1_close([agg_1_close_0 {2}, agg_1_close_1 {2}]) {1, 2}] AS rel([rel_0 {1,2}, rel_1 {1,2}]) {1, 2}
AGGMPC [rel_1, +] FROM (rel([rel_0 {1,2}, rel_1 {1,2}]) {1, 2}) GROUP BY [rel_0] AS agg_obl([agg_obl_0 {1,2}, agg_obl_1 {1,2}]) {1, 2}
OPEN agg_obl([agg_obl_0 {1,2}, agg_obl_1 {1,2}]) {1, 2} INTO agg_obl_open([agg_obl_open_0 {1,2}, agg_obl_open_1 {1,2}]) {1}
"""
    actual = protocol()
    assert expected == actual, actual


def testSingleProj():

    @scotch
    @mpc
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
        proj = sal.project(rel, "proj", ["rel_0", "rel_1"])

        sal.collect(proj, 1)

        # return root nodes
        return set([in1, in2])

    expected = """CREATE RELATION in1([in1_0 {1}, in1_1 {1}]) {1} WITH COLUMNS (INTEGER, INTEGER)
CREATE RELATION in2([in2_0 {2}, in2_1 {2}]) {2} WITH COLUMNS (INTEGER, INTEGER)
CLOSE in2([in2_0 {2}, in2_1 {2}]) {2} INTO in2_close([in2_close_0 {2}, in2_close_1 {2}]) {1}
CONCAT [in1([in1_0 {1}, in1_1 {1}]) {1}, in2_close([in2_close_0 {2}, in2_close_1 {2}]) {1}] AS rel([rel_0 {1,2}, rel_1 {1,2}]) {1}
PROJECT [rel_0, rel_1] FROM (rel([rel_0 {1,2}, rel_1 {1,2}]) {1}) AS proj([proj_0 {1,2}, proj_1 {1,2}]) {1}
"""
    actual = protocol()
    assert expected == actual, actual


def testSingleMult():

    @scotch
    @mpc
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
        mult = sal.multiply(rel, "mult", "rel_0", ["rel_0", 1])

        sal.collect(mult, 1)

        # return root nodes
        return set([in1, in2])

    expected = """CREATE RELATION in1([in1_0 {1}, in1_1 {1}]) {1} WITH COLUMNS (INTEGER, INTEGER)
CREATE RELATION in2([in2_0 {2}, in2_1 {2}]) {2} WITH COLUMNS (INTEGER, INTEGER)
CLOSE in2([in2_0 {2}, in2_1 {2}]) {2} INTO in2_close([in2_close_0 {2}, in2_close_1 {2}]) {1}
CONCAT [in1([in1_0 {1}, in1_1 {1}]) {1}, in2_close([in2_close_0 {2}, in2_close_1 {2}]) {1}] AS rel([rel_0 {1,2}, rel_1 {1,2}]) {1}
MULTIPLY [rel_0 -> rel_0 * 1] FROM (rel([rel_0 {1,2}, rel_1 {1,2}]) {1}) AS mult([mult_0 {1,2}, mult_1 {1,2}]) {1}
"""
    actual = protocol()
    assert expected == actual, actual

def testSingleDiv():

    @scotch
    @mpc
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
        mult = sal.divide(rel, "mult", "rel_0", ["rel_0", "rel_1"])

        sal.collect(mult, 1)

        # return root nodes
        return set([in1, in2])

    expected = """CREATE RELATION in1([in1_0 {1}, in1_1 {1}]) {1} WITH COLUMNS (INTEGER, INTEGER)
CREATE RELATION in2([in2_0 {2}, in2_1 {2}]) {2} WITH COLUMNS (INTEGER, INTEGER)
CLOSE in2([in2_0 {2}, in2_1 {2}]) {2} INTO in2_close([in2_close_0 {2}, in2_close_1 {2}]) {1}
CONCAT [in1([in1_0 {1}, in1_1 {1}]) {1}, in2_close([in2_close_0 {2}, in2_close_1 {2}]) {1}] AS rel([rel_0 {1,2}, rel_1 {1,2}]) {1}
DIVIDE [rel_0 -> rel_0 / rel_1] FROM (rel([rel_0 {1,2}, rel_1 {1,2}]) {1}) AS mult([mult_0 {1,2}, mult_1 {1,2}]) {1}
"""
    actual = protocol()
    assert expected == actual, actual

def testMultByZero():

    @scotch
    @mpc
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
        mult = sal.multiply(rel, "mult", "rel_0", ["rel_0", 0])

        sal.collect(mult, 1)

        # return root nodes
        return set([in1, in2])

    expected = """CREATE RELATION in1([in1_0 {1}, in1_1 {1}]) {1} WITH COLUMNS (INTEGER, INTEGER)
CLOSE in1([in1_0 {1}, in1_1 {1}]) {1} INTO in1_close([in1_close_0 {1}, in1_close_1 {1}]) {1, 2}
CREATE RELATION in2([in2_0 {2}, in2_1 {2}]) {2} WITH COLUMNS (INTEGER, INTEGER)
CLOSE in2([in2_0 {2}, in2_1 {2}]) {2} INTO in2_close([in2_close_0 {2}, in2_close_1 {2}]) {1, 2}
CONCATMPC [in1_close([in1_close_0 {1}, in1_close_1 {1}]) {1, 2}, in2_close([in2_close_0 {2}, in2_close_1 {2}]) {1, 2}] AS rel([rel_0 {1,2}, rel_1 {1,2}]) {1, 2}
MULTIPLYMPC [rel_0 -> rel_0 * 0] FROM (rel([rel_0 {1,2}, rel_1 {1,2}]) {1, 2}) AS mult([mult_0 {1,2}, mult_1 {1,2}]) {1, 2}
OPEN mult([mult_0 {1,2}, mult_1 {1,2}]) {1, 2} INTO mult_open([mult_open_0 {1,2}, mult_open_1 {1,2}]) {1}
"""
    actual = protocol()
    assert expected == actual, actual


def testConcatPushdown():

    @scotch
    @mpc
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
        proj = sal.project(rel, "proj", ["rel_0", "rel_1"])
        agg = sal.aggregate(proj, "agg", "proj_0", "proj_1", "+")

        sal.collect(agg, 1)

        # return root nodes
        return set([in1, in2, in3])

    expected = """CREATE RELATION in1([in1_0 {1}, in1_1 {1}]) {1} WITH COLUMNS (INTEGER, INTEGER)
CREATE RELATION in2([in2_0 {2}, in2_1 {2}]) {2} WITH COLUMNS (INTEGER, INTEGER)
CREATE RELATION in3([in3_0 {3}, in3_1 {3}]) {3} WITH COLUMNS (INTEGER, INTEGER)
PROJECT [in1_0, in1_1] FROM (in1([in1_0 {1}, in1_1 {1}]) {1}) AS proj_0([proj_0_0 {1}, proj_0_1 {1}]) {1}
AGG [proj_0_1, +] FROM (proj_0([proj_0_0 {1}, proj_0_1 {1}]) {1}) GROUP BY [proj_0_0] AS agg_0([agg_0_0 {1}, agg_0_1 {1}]) {1}
CLOSE agg_0([agg_0_0 {1}, agg_0_1 {1}]) {1} INTO agg_0_close([agg_0_close_0 {1}, agg_0_close_1 {1}]) {1, 2, 3}
PROJECT [in2_0, in2_1] FROM (in2([in2_0 {2}, in2_1 {2}]) {2}) AS proj_1([proj_1_0 {2}, proj_1_1 {2}]) {2}
AGG [proj_1_1, +] FROM (proj_1([proj_1_0 {2}, proj_1_1 {2}]) {2}) GROUP BY [proj_1_0] AS agg_1([agg_1_0 {2}, agg_1_1 {2}]) {2}
CLOSE agg_1([agg_1_0 {2}, agg_1_1 {2}]) {2} INTO agg_1_close([agg_1_close_0 {2}, agg_1_close_1 {2}]) {1, 2, 3}
PROJECT [in3_0, in3_1] FROM (in3([in3_0 {3}, in3_1 {3}]) {3}) AS proj_2([proj_2_0 {3}, proj_2_1 {3}]) {3}
AGG [proj_2_1, +] FROM (proj_2([proj_2_0 {3}, proj_2_1 {3}]) {3}) GROUP BY [proj_2_0] AS agg_2([agg_2_0 {3}, agg_2_1 {3}]) {3}
CLOSE agg_2([agg_2_0 {3}, agg_2_1 {3}]) {3} INTO agg_2_close([agg_2_close_0 {3}, agg_2_close_1 {3}]) {1, 2, 3}
CONCATMPC [agg_0_close([agg_0_close_0 {1}, agg_0_close_1 {1}]) {1, 2, 3}, agg_1_close([agg_1_close_0 {2}, agg_1_close_1 {2}]) {1, 2, 3}, agg_2_close([agg_2_close_0 {3}, agg_2_close_1 {3}]) {1, 2, 3}] AS rel([rel_0 {1,2,3}, rel_1 {1,2,3}]) {1, 2, 3}
AGGMPC [rel_1, +] FROM (rel([rel_0 {1,2,3}, rel_1 {1,2,3}]) {1, 2, 3}) GROUP BY [rel_0] AS agg_obl([agg_obl_0 {1,2,3}, agg_obl_1 {1,2,3}]) {1, 2, 3}
OPEN agg_obl([agg_obl_0 {1,2,3}, agg_obl_1 {1,2,3}]) {1, 2, 3} INTO agg_obl_open([agg_obl_open_0 {1,2,3}, agg_obl_open_1 {1,2,3}]) {1}
"""
    actual = protocol()
    assert expected == actual, actual


def testAgg():

    @scotch
    @mpc
    def protocol():

        # define inputs
        colsIn1 = [
            ("INTEGER", set([1])),
            ("INTEGER", set([1]))
        ]
        in1 = sal.create("in1", colsIn1, set([1]))
        colsIn2 = [
            ("INTEGER", set([2])),
            ("INTEGER", set([2]))
        ]
        in2 = sal.create("in2", colsIn2, set([2]))

        # combine parties' inputs into one relation
        rel = sal.concat([in1, in2], "rel")

        # specify the workflow
        projA = sal.project(rel, "projA", ["rel_0", "rel_1"])
        projB = sal.project(projA, "projB", ["projA_0", "projA_1"])
        agg = sal.aggregate(projB, "agg", "projB_0", "projB_1", "+")

        sal.collect(agg, 1)

        # return root nodes
        return set([in1, in2])

    expected = """CREATE RELATION in1 {1} WITH COLUMNS (INTEGER, INTEGER)
CREATE RELATION in2 {2} WITH COLUMNS (INTEGER, INTEGER)
PROJECT [in1_0, in1_1] FROM (in1 {1}) AS projA_0 {1}
PROJECT [in2_0, in2_1] FROM (in2 {2}) AS projA_1 {2}
PROJECT [projA_0_0, projA_0_1] FROM (projA_0 {1}) AS projB_0 {1}
AGG [projB_0_1, +] FROM (projB_0 {1}) GROUP BY [projB_0_0] AS agg_0 {1}
PROJECT [projA_1_0, projA_1_1] FROM (projA_1 {2}) AS projB_1 {2}
AGG [projB_1_1, +] FROM (projB_1 {2}) GROUP BY [projB_1_0] AS agg_1 {2}
CONCATMPC [agg_0 {1}, agg_1 {2}] AS rel {1, 2}
AGGMPC [rel_1, +] FROM (rel {1, 2}) GROUP BY [rel_0] AS agg_obl {1}
"""
    actual = protocol()
    assert expected == actual, actual


def testAggProj():

    @scotch
    @mpc
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

    expected = """CREATE RELATION in1([in1_0 {1}, in1_1 {1}]) {1} WITH COLUMNS (INTEGER, INTEGER)
CREATE RELATION in2([in2_0 {2}, in2_1 {2}]) {2} WITH COLUMNS (INTEGER, INTEGER)
PROJECT [in1_0, in1_1] FROM (in1([in1_0 {1}, in1_1 {1}]) {1}) AS projA_0([projA_0_0 {1}, projA_0_1 {1}]) {1}
PROJECT [in2_0, in2_1] FROM (in2([in2_0 {2}, in2_1 {2}]) {2}) AS projA_1([projA_1_0 {2}, projA_1_1 {2}]) {2}
PROJECT [projA_0_0, projA_0_1] FROM (projA_0([projA_0_0 {1}, projA_0_1 {1}]) {1}) AS projB_0([projB_0_0 {1}, projB_0_1 {1}]) {1}
AGG [projB_0_1, +] FROM (projB_0([projB_0_0 {1}, projB_0_1 {1}]) {1}) GROUP BY [projB_0_0] AS agg_0([agg_0_0 {1}, agg_0_1 {1}]) {1}
CLOSE agg_0([agg_0_0 {1}, agg_0_1 {1}]) {1} INTO agg_0_close([agg_0_close_0 {1}, agg_0_close_1 {1}]) {1, 2}
PROJECT [projA_1_0, projA_1_1] FROM (projA_1([projA_1_0 {2}, projA_1_1 {2}]) {2}) AS projB_1([projB_1_0 {2}, projB_1_1 {2}]) {2}
AGG [projB_1_1, +] FROM (projB_1([projB_1_0 {2}, projB_1_1 {2}]) {2}) GROUP BY [projB_1_0] AS agg_1([agg_1_0 {2}, agg_1_1 {2}]) {2}
CLOSE agg_1([agg_1_0 {2}, agg_1_1 {2}]) {2} INTO agg_1_close([agg_1_close_0 {2}, agg_1_close_1 {2}]) {1, 2}
CONCATMPC [agg_0_close([agg_0_close_0 {1}, agg_0_close_1 {1}]) {1, 2}, agg_1_close([agg_1_close_0 {2}, agg_1_close_1 {2}]) {1, 2}] AS rel([rel_0 {1,2}, rel_1 {1,2}]) {1, 2}
AGGMPC [rel_1, +] FROM (rel([rel_0 {1,2}, rel_1 {1,2}]) {1, 2}) GROUP BY [rel_0] AS agg_obl([agg_obl_0 {1,2}, agg_obl_1 {1,2}]) {1, 2}
OPEN agg_obl([agg_obl_0 {1,2}, agg_obl_1 {1,2}]) {1, 2} INTO agg_obl_open([agg_obl_open_0 {1,2}, agg_obl_open_1 {1,2}]) {1}
PROJECT [agg_obl_open_0, agg_obl_open_1] FROM (agg_obl_open([agg_obl_open_0 {1,2}, agg_obl_open_1 {1,2}]) {1}) AS projC([projC_0 {1,2}, projC_1 {1,2}]) {1}
"""
    actual = protocol()
    assert expected == actual, actual


def testInternalAgg():

    @scotch
    @mpc
    def protocol():
        # define inputs
        colsInA = [
            ("INTEGER", set([1])),
            ("INTEGER", set([1]))
        ]
        inA = sal.create("inA", colsInA, set([1]))
        colsInB = [
            ("INTEGER", set([2])),
            ("INTEGER", set([2]))
        ]
        inB = sal.create("inB", colsInB, set([2]))

        # specify the workflow
        projA = sal.project(inA, "projA", ["inA_0", "inA_1"])
        projB = sal.project(inB, "projB", ["inB_0", "inB_1"])
        joined = sal.join(projA, projB, "joined", "projA_0", "projB_0")
        agg = sal.aggregate(joined, "agg", "joined_0", "joined_1", "+")
        proj = sal.project(agg, "proj", ["agg_0", "agg_1"])

        sal.collect(proj, 1)

        return set([inA, inB])

    expected = """CREATE RELATION inA {1} WITH COLUMNS (INTEGER, INTEGER)
CREATE RELATION inB {2} WITH COLUMNS (INTEGER, INTEGER)
PROJECT [inA_0, inA_1] FROM (inA {1}) AS projA {1}
PROJECT [inB_0, inB_1] FROM (inB {2}) AS projB {2}
(projA {1}) JOINMPC (projB {2}) ON projA_0 AND projB_0 AS joined {1, 2}
AGGMPC [joined_1, +] FROM (joined {1, 2}) GROUP BY [joined_0] AS agg {1}
PROJECT [agg_0, agg_1] FROM (agg {1}) AS proj {1}
"""
    actual = protocol()
    assert expected == actual, actual


def testJoin():

    @scotch
    @mpc
    def protocol():
        # define inputs
        colsInA = [
            ("INTEGER", set([1])),
            ("INTEGER", set([1]))
        ]
        inA = sal.create("inA", colsInA, set([1]))

        colsInB = [
            ("INTEGER", set([2])),
            ("INTEGER", set([2]))
        ]
        inB = sal.create("inB", colsInB, set([2]))

        # specify the workflow
        aggA = sal.aggregate(inA, "aggA", "inA_0", "inA_1", "+")
        projA = sal.project(aggA, "projA", ["aggA_0", "aggA_1"])

        aggB = sal.aggregate(inB, "aggB", "inB_0", "inB_1", "+")
        projB = sal.project(aggB, "projB", ["aggB_0", "aggB_1"])

        joined = sal.join(projA, projB, "joined", "projA_0", "projB_0")

        proj = sal.project(joined, "proj", ["joined_0", "joined_1"])
        agg = sal.aggregate(
            proj, "agg", "proj_0", "proj_1", "+")

        sal.collect(agg, 1)

        # create dag
        return set([inA, inB])

    expected = """CREATE RELATION inA {1} WITH COLUMNS (INTEGER, INTEGER)
AGG [inA_1, +] FROM (inA {1}) GROUP BY [inA_0] AS aggA {1}
CREATE RELATION inB {2} WITH COLUMNS (INTEGER, INTEGER)
AGG [inB_1, +] FROM (inB {2}) GROUP BY [inB_0] AS aggB {2}
PROJECT [aggA_0, aggA_1] FROM (aggA {1}) AS projA {1}
PROJECT [aggB_0, aggB_1] FROM (aggB {2}) AS projB {2}
(projA {1}) JOINMPC (projB {2}) ON projA_0 AND projB_0 AS joined {1, 2}
PROJECTMPC [joined_0, joined_1] FROM (joined {1, 2}) AS proj {1, 2}
AGGMPC [proj_1, +] FROM (proj {1, 2}) GROUP BY [proj_0] AS agg {1}
"""
    actual = protocol()
    assert expected == actual, actual


def testJoinConcat():

    @scotch
    @mpc
    def protocol():
        # define inputs
        colsInA = [
            ("INTEGER", set([1])),
            ("INTEGER", set([1]))
        ]
        inA = sal.create("inA", colsInA, set([1]))

        colsInB = [
            ("INTEGER", set([2])),
            ("INTEGER", set([2]))
        ]
        inB = sal.create("inB", colsInB, set([2]))

        colsInC = [
            ("INTEGER", set([3])),
            ("INTEGER", set([3])),
            ("INTEGER", set([3]))
        ]
        inC = sal.create("inC", colsInC, set([3]))

        # specify the workflow
        aggA = sal.aggregate(inA, "aggA", "inA_0", "inA_1", "+")
        projA = sal.project(aggA, "projA", ["aggA_0", "aggA_1"])

        aggB = sal.aggregate(inB, "aggB", "inB_0", "inB_1", "+")
        projB = sal.project(aggB, "projB", ["aggB_0", "aggB_1"])

        joined = sal.join(projA, projB, "joined", "projA_0", "projB_0")
        comb = sal.concat([inC, joined], "comb")
        sal.collect(comb, 3)

        # create dag
        return set([inA, inB, inC])

    expected = """CREATE RELATION inA {1} WITH COLUMNS (INTEGER, INTEGER)
AGG [inA_1, +] FROM (inA {1}) GROUP BY [inA_0] AS aggA {1}
CREATE RELATION inB {2} WITH COLUMNS (INTEGER, INTEGER)
AGG [inB_1, +] FROM (inB {2}) GROUP BY [inB_0] AS aggB {2}
CREATE RELATION inC {3} WITH COLUMNS (INTEGER, INTEGER, INTEGER)
PROJECT [aggA_0, aggA_1] FROM (aggA {1}) AS projA {1}
PROJECT [aggB_0, aggB_1] FROM (aggB {2}) AS projB {2}
(projA {1}) JOINMPC (projB {2}) ON projA_0 AND projB_0 AS joined {3}
CONCAT [inC {3}, joined {3}] AS comb {3}
"""
    actual = protocol()
    assert expected == actual, actual


def testJoinConcat2():

    @scotch
    @mpc
    def protocol():
        # define inputs
        colsInA = [
            ("INTEGER", set([2])),
            ("INTEGER", set([2]))
        ]
        inA = sal.create("inA", colsInA, set([2]))

        colsInB = [
            ("INTEGER", set([3])),
            ("INTEGER", set([3]))
        ]
        inB = sal.create("inB", colsInB, set([3]))

        colsInC = [
            ("INTEGER", set([1])),
            ("INTEGER", set([1])),
            ("INTEGER", set([1]))
        ]
        inC = sal.create("inC", colsInC, set([1]))

        # specify the workflow
        aggA = sal.aggregate(inA, "aggA", "inA_0", "inA_1", "+")
        projA = sal.project(aggA, "projA", ["aggA_0", "aggA_1"])

        aggB = sal.aggregate(inB, "aggB", "inB_0", "inB_1", "+")
        projB = sal.project(aggB, "projB", ["aggB_0", "aggB_1"])

        joined = sal.join(projA, projB, "joined", "projA_0", "projB_0")
        comb = sal.concat([inC, joined], "comb")
        agg = sal.aggregate(comb, "agg", "comb_1", "comb_2", "+")
        sal.collect(agg, 1)

        # create dag
        return set([inA, inB, inC])

    expected = """CREATE RELATION inA {2} WITH COLUMNS (INTEGER, INTEGER)
AGG [inA_1, +] FROM (inA {2}) GROUP BY [inA_0] AS aggA {2}
CREATE RELATION inB {3} WITH COLUMNS (INTEGER, INTEGER)
AGG [inB_1, +] FROM (inB {3}) GROUP BY [inB_0] AS aggB {3}
CREATE RELATION inC {1} WITH COLUMNS (INTEGER, INTEGER, INTEGER)
PROJECT [aggA_0, aggA_1] FROM (aggA {2}) AS projA {2}
PROJECT [aggB_0, aggB_1] FROM (aggB {3}) AS projB {3}
(projA {2}) JOINMPC (projB {3}) ON projA_0 AND projB_0 AS joined {2, 3}
CONCATMPC [inC {1}, joined {2, 3}] AS comb {1, 2, 3}
AGGMPC [comb_2, +] FROM (comb {1, 2, 3}) GROUP BY [comb_1] AS agg {1}
"""
    actual = protocol()
    assert expected == actual, actual


def testMultAgg():

    @scotch
    @mpc
    def protocol():

        # define inputs
        colsIn1 = [
            ("INTEGER", set([1])),
            ("INTEGER", set([1]))
        ]
        in1 = sal.create("in1", colsIn1, set([1]))
        colsIn2 = [
            ("INTEGER", set([2])),
            ("INTEGER", set([2]))
        ]
        in2 = sal.create("in2", colsIn2, set([2]))

        # combine parties' inputs into one relation
        rel = sal.concat([in1, in2], "rel")

        # specify the workflow
        projA = sal.project(rel, "projA", ["rel_0", "rel_1"])
        aggA = sal.aggregate(projA, "aggA", "projA_0", "projA_1", "+")

        projB = sal.project(rel, "projB", ["rel_0", "rel_1"])
        aggB = sal.aggregate(projB, "aggB", "projB_0", "projB_1", "+")

        sal.collect(aggA, 1)
        sal.collect(aggB, 1)

        # return root nodes
        return set([in1, in2])

    expected = """CREATE RELATION in1 {1} WITH COLUMNS (INTEGER, INTEGER)
CREATE RELATION in2 {2} WITH COLUMNS (INTEGER, INTEGER)
PROJECT [in1_0, in1_1] FROM (in1 {1}) AS projA_0 {1}
AGG [projA_0_1, +] FROM (projA_0 {1}) GROUP BY [projA_0_0] AS aggA_0 {1}
PROJECT [in2_0, in2_1] FROM (in2 {2}) AS projA_1 {2}
AGG [projA_1_1, +] FROM (projA_1 {2}) GROUP BY [projA_1_0] AS aggA_1 {2}
PROJECT [in1_0, in1_1] FROM (in1 {1}) AS projB_0 {1}
AGG [projB_0_1, +] FROM (projB_0 {1}) GROUP BY [projB_0_0] AS aggB_0 {1}
PROJECT [in2_0, in2_1] FROM (in2 {2}) AS projB_1 {2}
AGG [projB_1_1, +] FROM (projB_1 {2}) GROUP BY [projB_1_0] AS aggB_1 {2}
CONCATMPC [aggA_0 {1}, aggA_1 {2}] AS rel {1, 2}
AGGMPC [rel_1, +] FROM (rel {1, 2}) GROUP BY [rel_0] AS aggA_obl {1}
CONCATMPC [aggB_0 {1}, aggB_1 {2}] AS rel_1 {1, 2}
AGGMPC [rel_1_1, +] FROM (rel_1 {1, 2}) GROUP BY [rel_1_0] AS aggB_obl {1}
"""
    actual = protocol()
    assert expected == actual, actual


def testMultiple():

    @scotch
    @mpc
    def protocol():

        # define inputs
        colsIn1 = [
            ("INTEGER", set([1])),
            ("INTEGER", set([1]))
        ]
        in1 = sal.create("in1", colsIn1, set([1]))
        colsIn2 = [
            ("INTEGER", set([2])),
            ("INTEGER", set([2]))
        ]
        in2 = sal.create("in2", colsIn2, set([2]))

        # combine parties' inputs into one relation
        rel = sal.concat([in1, in2], "rel")

        # specify the workflow
        projA = sal.project(rel, "projA", ["rel_0", "rel_1"])
        projB = sal.project(rel, "projB", ["rel_0", "rel_1"])

        sal.collect(projA, 1)
        sal.collect(projB, 1)

        # return root nodes
        return set([in1, in2])

    expected = """CREATE RELATION in1 {1} WITH COLUMNS (INTEGER, INTEGER)
CREATE RELATION in2 {2} WITH COLUMNS (INTEGER, INTEGER)
CONCAT [in1 {1}, in2 {2}] AS rel {1}
PROJECT [rel_0, rel_1] FROM (rel {1}) AS projA {1}
CONCAT [in1 {1}, in2 {2}] AS rel_1 {1}
PROJECT [rel_1_0, rel_1_1] FROM (rel_1 {1}) AS projB {1}
"""
    actual = protocol()
    assert expected == actual, actual


def testSingle():

    @scotch
    @mpc
    def protocol():

        # define inputs
        colsIn1 = [
            ("INTEGER", set([1])),
            ("INTEGER", set([1]))
        ]
        in1 = sal.create("in1", colsIn1, set([1]))
        colsIn2 = [
            ("INTEGER", set([2])),
            ("INTEGER", set([2]))
        ]
        in2 = sal.create("in2", colsIn2, set([2]))
        colsIn3 = [
            ("INTEGER", set([3])),
            ("INTEGER", set([3]))
        ]
        in3 = sal.create("in3", colsIn3, set([3]))

        # combine parties' inputs into one relation
        rel = sal.concat([in1, in2, in3], "rel")

        # specify the workflow
        projA = sal.project(rel, "projA", ["rel_0", "rel_1"])
        projB = sal.project(projA, "projB", ["projA_0", "projA_1"])

        sal.collect(projB, 1)

        # return root nodes
        return set([in1, in2, in3])

    expected = """CREATE RELATION in1 {1} WITH COLUMNS (INTEGER, INTEGER)
CREATE RELATION in2 {2} WITH COLUMNS (INTEGER, INTEGER)
CREATE RELATION in3 {3} WITH COLUMNS (INTEGER, INTEGER)
PROJECT [in1_0, in1_1] FROM (in1 {1}) AS projA_0 {1}
PROJECT [in2_0, in2_1] FROM (in2 {2}) AS projA_1 {1}
PROJECT [in3_0, in3_1] FROM (in3 {3}) AS projA_2 {1}
CONCAT [projA_0 {1}, projA_1 {1}, projA_2 {1}] AS rel {1}
PROJECT [rel_0, rel_1] FROM (rel {1}) AS projB {1}
"""
    actual = protocol()
    assert expected == actual, actual


def testRevealJoinOpt():

    @scotch
    @mpc
    def protocol():
        # define inputs
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

        joined = sal.join(inA, inB, "joined", "inA_0", "inB_0")

        sal.collect(joined, 1)
        # create dag
        return set([inA, inB])

    expected = """CREATE RELATION inA([inA_0 {1}, inA_1 {1}]) {1} WITH COLUMNS (INTEGER, INTEGER)
CLOSE inA([inA_0 {1}, inA_1 {1}]) {1} INTO inA_close([inA_close_0 {1}, inA_close_1 {1}]) {1, 2}
CREATE RELATION inB([inB_0 {2}, inB_1 {2}]) {2} WITH COLUMNS (INTEGER, INTEGER)
CLOSE inB([inB_0 {2}, inB_1 {2}]) {2} INTO inB_close([inB_close_0 {2}, inB_close_1 {2}]) {1, 2}
(inA_close([inA_close_0 {1}, inA_close_1 {1}]) {1, 2}) REVEALJOIN (inB_close([inB_close_0 {2}, inB_close_1 {2}]) {1, 2}) ON inA_close_0 AND inB_close_0 AS joined([joined_0 {1,2}, joined_1 {1,2}, joined_2 {1,2}]) {1}
"""
    actual = protocol()
    assert expected == actual, actual


def testHybridJoinOpt():

    @scotch
    @mpc
    def protocol():
        # define inputs
        colsInA = [
            defCol("INTEGER", [1]),
            defCol("INTEGER", [1]),
        ]
        inA = sal.create("inA", colsInA, set([1]))

        colsInB = [
            defCol("INTEGER", [1], [2]),
            defCol("INTEGER", [2])
        ]
        inB = sal.create("inB", colsInB, set([2]))

        joined = sal.join(inA, inB, "joined", "inA_0", "inB_0")
        # need the agg to prevent joined from being converted to a RevealJoin
        agg = sal.aggregate(joined, "agg", "joined_0", "joined_1", "+")

        sal.collect(agg, 1)
        # create dag
        return set([inA, inB])

    expected = """CREATE RELATION inA([inA_0 {1}, inA_1 {1}]) {1} WITH COLUMNS (INTEGER, INTEGER)
CLOSE inA([inA_0 {1}, inA_1 {1}]) {1} INTO inA_close([inA_close_0 {1}, inA_close_1 {1}]) {1, 2}
CREATE RELATION inB([inB_0 {1} {2}, inB_1 {2}]) {2} WITH COLUMNS (INTEGER, INTEGER)
CLOSE inB([inB_0 {1} {2}, inB_1 {2}]) {2} INTO inB_close([inB_close_0 {1} {2}, inB_close_1 {2}]) {1, 2}
(inA_close([inA_close_0 {1}, inA_close_1 {1}]) {1, 2}) HYBRIDJOIN (inB_close([inB_close_0 {1} {2}, inB_close_1 {2}]) {1, 2}) ON inA_close_0 AND inB_close_0 AS joined([joined_0 {1,2} {1}, joined_1 {1,2} {1}, joined_2 {1,2}]) {1, 2}
AGGMPC [joined_1, +] FROM (joined([joined_0 {1,2} {1}, joined_1 {1,2} {1}, joined_2 {1,2}]) {1, 2}) GROUP BY [joined_0] AS agg([agg_0 {1,2} {1}, agg_1 {1,2} {1}]) {1, 2}
OPEN agg([agg_0 {1,2} {1}, agg_1 {1,2} {1}]) {1, 2} INTO agg_open([agg_open_0 {1,2} {1}, agg_open_1 {1,2} {1}]) {1}
"""
    actual = protocol()
    assert expected == actual, actual


def testHybridAndRevealJoinOpt():
    # Note: for now I'm assuming that we don't overwrite a reveal join
    # with a hybrid join

    @scotch
    @mpc
    def protocol():
        # define inputs
        colsInA = [
            defCol("INTEGER", [1]),
            defCol("INTEGER", [1]),
        ]
        inA = sal.create("inA", colsInA, set([1]))

        colsInB = [
            defCol("INTEGER", [1], [2]),
            defCol("INTEGER", [2])
        ]
        inB = sal.create("inB", colsInB, set([2]))

        joined = sal.join(inA, inB, "joined", "inA_0", "inB_0")

        sal.collect(joined, 1)
        # create dag
        return set([inA, inB])

    expected = """CREATE RELATION inA([inA_0 {1}, inA_1 {1}]) {1} WITH COLUMNS (INTEGER, INTEGER)
CLOSE inA([inA_0 {1}, inA_1 {1}]) {1} INTO inA_close([inA_close_0 {1}, inA_close_1 {1}]) {1, 2}
CREATE RELATION inB([inB_0 {1} {2}, inB_1 {2}]) {2} WITH COLUMNS (INTEGER, INTEGER)
CLOSE inB([inB_0 {1} {2}, inB_1 {2}]) {2} INTO inB_close([inB_close_0 {1} {2}, inB_close_1 {2}]) {1, 2}
(inA_close([inA_close_0 {1}, inA_close_1 {1}]) {1, 2}) REVEALJOIN (inB_close([inB_close_0 {1} {2}, inB_close_1 {2}]) {1, 2}) ON inA_close_0 AND inB_close_0 AS joined([joined_0 {1,2} {1}, joined_1 {1,2} {1}, joined_2 {1,2}]) {1}
"""
    actual = protocol()
    assert expected == actual, actual

def testJoin():

    @scotch
    @mpc
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
CREATE RELATION inB([inB_0 {2}, inB_1 {2}]) {2} WITH COLUMNS (INTEGER, INTEGER)
PROJECT [inB_0, inB_1] FROM (inB([inB_0 {2}, inB_1 {2}]) {2}) AS projB([projB_0 {2}, projB_1 {2}]) {2}
CLOSE projB([projB_0 {2}, projB_1 {2}]) {2} INTO projB_close([projB_close_0 {2}, projB_close_1 {2}]) {1, 2}
(inA_close([inA_close_0 {1}, inA_close_1 {1}]) {1, 2}) JOINMPC (projB_close([projB_close_0 {2}, projB_close_1 {2}]) {1, 2}) ON inA_close_0 AND projB_close_0 AS joined([joined_0 {1,2}, joined_1 {1,2}, joined_2 {1,2}]) {1, 2}
MULTIPLYMPC [joined_0 -> joined_0 * 0] FROM (joined([joined_0 {1,2}, joined_1 {1,2}, joined_2 {1,2}]) {1, 2}) AS mult([mult_0 {1,2}, mult_1 {1,2}, mult_2 {1,2}]) {1, 2}
OPEN mult([mult_0 {1,2}, mult_1 {1,2}, mult_2 {1,2}]) {1, 2} INTO mult_open([mult_open_0 {1,2}, mult_open_1 {1,2}, mult_open_2 {1,2}]) {1}
"""
    actual = protocol()
    assert expected == actual, actual

if __name__ == "__main__":

    testSingleConcat()
    testSingleAgg()
    testSingleProj()
    testSingleMult()
    testSingleDiv()
    testMultByZero()
    testAggProj()
    testConcatPushdown()
    testRevealJoinOpt()
    testHybridJoinOpt()
    testHybridAndRevealJoinOpt()
    testJoin()
