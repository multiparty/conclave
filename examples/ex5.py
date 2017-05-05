from salmon import *

if __name__ == "__main__":
    # define inputs
    colsInA = [
        ("int", set([1, 2, 3])), 
        ("int", set([1, 2, 3]))
    ]
    inA = create("inA", colsInA)

    # specify the workflow
    agg = aggregate(inA, "agg", 0, 1, None)
    projA = project(agg, "projA", None)
    projB = project(projA, "projB", None)
    projC = project(projA, "projC", None)
    otherAgg = aggregate(agg, "otherAgg", 0, 1, None)

    # create dag with root nodes
    dag = OpDag(set([inA]))
    
    # compile to MPC
    rewriteDag(dag)
