from salmon import *

if __name__ == "__main__":
    # define inputs
    colsInA = [
        ("int", set([1, 2, 3])), 
        ("int", set([1, 2, 3]))
    ]
    inA = create("inA", colsInA)

    # specify the workflow
    projA = project(inA, "projA", None)
    projB = project(inA, "projB", None)
    joined = join(projA, projB, "projC", 0, 0)
    agg = aggregate(joined, "agg", 0, 1, None)
    proj = project(agg, "proj", None)

    # create dag with root nodes
    dag = OpDag(set([inA]))
    
    # compile to MPC
    rewriteDag(dag)
