import salmon.lang as sal 
from salmon.comp import mpc

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

    opened = sal.collect(agg, "opened", 1)

    # create dag
    return set([inA, inB])
    
if __name__ == "__main__":

    print(protocol())

