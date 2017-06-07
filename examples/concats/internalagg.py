import salmon.lang as sal 
from salmon.comp import mpc, scotch

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

    opened = sal.collect(proj, "opened", 1)

    return set([inA, inB])

if __name__ == "__main__":

    protocol()
