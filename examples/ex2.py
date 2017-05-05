from salmon import *

if __name__ == "__main__":

    # define inputs
    colsInRel = [
        ("int", set([1, 2])), 
        ("int", set([1, 2])), 
        ("int", set([1, 2]))
    ]
    inRel = create("inRel", colsInRel)

    # specify the workflow
    aggA = aggregate(inRel, "aggA", 0, 1, None)
    projA = project(aggA, "projA", None)
    
    aggB = aggregate(inRel, "aggB", 1, 2, None)
    projB = project(aggB, "projB", None)
    
    # create dag with roots nodes
    dag = OpDag(set([inRel]))
    
    # compile to MPC
    rewriteDag(dag)
