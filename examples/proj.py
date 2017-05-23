import salmon.lang as sal 
from salmon.comp import mpc

@mpc
def protocol():

    # define inputs
    colsInA = [
        ("INTEGER", set([1, 2, 3])), 
        ("INTEGER", set([1, 2, 3])), 
        ("INTEGER", set([1, 2, 3]))
    ]
    inA = sal.create("inA", colsInA)

    # specify the workflow
    proj = sal.project(inA, "proj", ["inA_0", "inA_2"])
    
    # return root nodes
    return set([inA])

if __name__ == "__main__":

    print(protocol())
