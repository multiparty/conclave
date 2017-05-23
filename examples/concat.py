import salmon.lang as sal 
from salmon.comp import mpc

@mpc
def protocol():

    # define inputs
    colsIn1 = [
        ("INTEGER", set([1])), 
        ("INTEGER", set([1]))
    ]
    in1 = sal.create("in1", colsIn1)
    colsIn2 = [
        ("INTEGER", set([2])), 
        ("INTEGER", set([2]))
    ]
    in2 = sal.create("in2", colsIn2)

    # combine parties' inputs into one relation
    rel = sal.concat([in1, in2], "rel")

    # specify the workflow
    # agg = sal.aggregate(inA, "agg", "inA_0", "inA_1", "+")
    # projA = sal.project(agg, "projA", ["agg_0", "agg_1"])
    # projB = sal.project(projA, "projB", ["projA_0", "projA_1"])
    opened = sal.collect(rel, "opened", 1)

    # return root nodes
    return set([in1, in2])

if __name__ == "__main__":

    print(protocol())
