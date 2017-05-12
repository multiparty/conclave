import salmon.lang as sal
from salmon.comp import mpc

@mpc
def protocol():

    # define inputs
    colsInRel = [
        ("INTEGER", set([1, 2])), 
        ("INTEGER", set([1, 2])), 
        ("INTEGER", set([1, 2]))
    ]
    inRel = sal.create("inRel", colsInRel)

    # specify the workflow
    aggA = sal.aggregate(inRel, "aggA", "inRel_0", "inRel_1", "+")
    projA = sal.project(aggA, "projA", None)
    
    aggB = sal.aggregate(inRel, "aggB", "inRel_1", "inRel_2", "+")
    projB = sal.project(aggB, "projB", None)

    return set([inRel])

if __name__ == "__main__":

    print(protocol())
