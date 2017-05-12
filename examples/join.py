from salmon import *

if __name__ == "__main__":
    # define inputs
    colsInA = [
        ("INTEGER", set([1, 2])), 
        ("INTEGER", set([1, 2]))
    ]
    inA = create("inA", colsInA)

    colsInB = [
        ("INTEGER", set([2])), 
        ("INTEGER", set([2]))
    ]
    inB = create("inB", colsInB)

    # specify the workflow
    aggA = aggregate(inA, "aggA", "inA_0", "inA_1", "+")
    projA = project(aggA, "projA", None)
    
    aggB = aggregate(inB, "aggB", "inB_0", "inB_1", "+")
    projB = project(aggB, "projB", None)
    
    joined = join(projA, projB, "joined", 0, 0)

    projected = project(joined, "projected", None)
    aggregated = aggregate(
        projected, "aggregated", "projected_0", "projected_1", "+")

    # create dag
    dag = OpDag(set([inA, inB]))
    
    # compile to MPC
    rewriteDag(dag)
