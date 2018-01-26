import salmon.lang as sal
from salmon.comp import mpc, scotch


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


if __name__ == "__main__":

    testJoinConcat()
    testJoinConcat2()

