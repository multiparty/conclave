import conclave.lang as sal
from conclave.utils import *
from conclave import workflow


def protocol():

    # define inputs
    colsInA = [
        defCol("a", "INTEGER", [2]),
        defCol("b", "INTEGER", [2]),
    ]
    in1 = sal.create("govreg", colsInA, {2})
    proja = sal.project(in1, "proja", ["a", "b"])

    colsInB = [
        defCol("a", "INTEGER", [1]),
        defCol("d", "INTEGER", [1])
    ]
    in2 = sal.create("company0", colsInB, {1})

    projb = sal.project(in2, "projb", ["a", "d"])

    colsInC = [
        defCol("a", "INTEGER", [1]),
        defCol("d", "INTEGER", [1])
    ]
    in3 = sal.create("company1", colsInC, {1})
    projc = sal.project(in3, "projc", ["a", "d"])

    right_rel = sal.concat([projb, projc], 'cld')

    joined = sal.join(proja, right_rel, "joined", ["a"], ["a"])

    agg = sal.aggregate(joined, "agg", ["b"], "d", "+", "d")

    out = sal.collect(agg, 2)

    return {in1 ,in2, in3}


if __name__ == "__main__":

    workflow.run(protocol)