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
    agg = sal.aggregate(inA, "agg", "inA_0", "inA_1", "+")
    projA = sal.project(agg, "projA", None)
    projB = sal.project(agg, "projB", None)
    otherAgg = sal.aggregate(agg, "otherAgg", "agg_1", "agg_0", "+")

    return set([inA])
    
if __name__ == "__main__":

    print(protocol())