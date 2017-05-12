import salmon.lang as sal 
from salmon.comp import mpc

@mpc
def protocol():
    # define inputs
    colsInA = [
        ("int", set([1, 2, 3])), 
        ("int", set([1, 2, 3]))
    ]
    inA = sal.create("inA", colsInA)

    # specify the workflow
    projA = sal.project(inA, "projA", None)
    projB = sal.project(inA, "projB", None)
    joined = sal.join(projA, projB, "joined", 0, 0)
    agg = sal.aggregate(joined, "agg", "joined_0", "joined_1", "+")
    proj = sal.project(agg, "proj", None)

    return set([inA])

if __name__ == "__main__":

    print(protocol())
