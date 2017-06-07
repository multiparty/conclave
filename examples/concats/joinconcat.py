import salmon.lang as sal 
from salmon.comp import mpc, scotch

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
    comb = sal.concat(set([inC, joined]), "comb")
    opened = sal.collect(comb, "opened", 1)

    # create dag
    return set([inA, inB, inC])
    
if __name__ == "__main__":

    protocol()

