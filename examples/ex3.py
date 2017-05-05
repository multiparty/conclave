from salmon import *

if __name__ == "__main__":
    # define inputs
    colsInA = [
        ("int", set([1, 2])), 
        ("int", set([1, 2]))
    ]
    inA = create("inA", colsInA)

    colsInB = [
        ("int", set([2])), 
        ("int", set([2]))
    ]
    inB = create("inB", colsInB)

    # specify the workflow
    aggA = aggregate(inA, "aggA", 0, 1, None)
    projA = project(aggA, "projA", None)
    
    aggB = aggregate(inB, "aggB", 0, 1, None)
    projB = project(aggB, "projB", None)
    
    joined = join(projA, projB, "joined", 0, 0)

    projected = project(joined, "projected", None)
    aggregated = aggregate(projected, "aggregated", 0, 1, None)

    # create dag
    dag = OpDag(set([inA, inB]))
    
    # compile to MPC
    rewriteDag(dag)
