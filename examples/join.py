import salmon.lang as sal 
from salmon.comp import mpc

@mpc
def protocol():
    # define inputs
    colsInA = [
        ("INTEGER", set([1, 2])), 
        ("INTEGER", set([1, 2]))
    ]
    inA = sal.create("inA", colsInA)

    colsInB = [
        ("INTEGER", set([2])), 
        ("INTEGER", set([2]))
    ]
    inB = sal.create("inB", colsInB)

    # specify the workflow
    aggA = sal.aggregate(inA, "aggA", "inA_0", "inA_1", "+")
    projA = sal.project(aggA, "projA", None)
    
    aggB = sal.aggregate(inB, "aggB", "inB_0", "inB_1", "+")
    projB = sal.project(aggB, "projB", None)
    
    joined = sal.join(projA, projB, "joined", "projA_0", "projB_0")

    projected = sal.project(joined, "projected", None)
    aggregated = sal.aggregate(
        projected, "aggregated", "projected_0", "projected_1", "+")

    # create dag
    return set([inA, inB])
    
if __name__ == "__main__":

    print(protocol())

