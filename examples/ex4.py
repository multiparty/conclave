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
    projB = project(agg, "projB", None)
    otherAgg = aggregate(agg, "otherAgg", 1, 0, None)

    # create dag with root nodes
    dag = OpDag(set([inA]))
    
    # compile to MPC
    rewriteDag(dag)
