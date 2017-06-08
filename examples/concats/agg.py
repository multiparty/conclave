import salmon.lang as sal 
from salmon.comp import mpc, scotch

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

    opened = sal.collect(agg, "opened", 1)

    # return root nodes
    return set([in1, in2])

if __name__ == "__main__":

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
STORE RELATION agg_obl {1} INTO {1} AS opened
"""
    print(expected)
    actual = protocol()
    print(actual)
    assert(expected == actual)
