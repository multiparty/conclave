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
    colsIn3 = [
        ("INTEGER", set([2])), 
        ("INTEGER", set([2]))
    ]
    in3 = sal.create("in3", colsIn3, set([3]))

    # combine parties' inputs into one relation
    rel = sal.concat(set([in1, in2, in3]), "rel")

    # specify the workflow
    projA = sal.project(rel, "projA", ["rel_0", "rel_1"])
    projB = sal.project(projA, "projB", ["projA_0", "projA_1"])

    opened = sal.collect(projB, "opened", 1)

    # return root nodes
    return set([in1, in2, in3])

if __name__ == "__main__":

    protocol()
