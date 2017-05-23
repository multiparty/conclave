import salmon.lang as sal 
from salmon.comp import mpc

@mpc
def protocol():

    # define inputs
    colsInA = [
        ("INTEGER", set([1, 2, 3])), 
        ("INTEGER", set([1, 2, 3]))
    ]
    inA = sal.create("inA", colsInA)

    colsInB = [
        ("INTEGER", set([1, 2, 3])), 
        ("INTEGER", set([1, 2, 3]))
    ]
    inB = sal.create("inB", colsInB)

    # specify the workflow
    joined = sal.join(inA, inB, "joined", "inA_0", "inB_0")
    byZero = sal.multiply(joined, "byZero", "joined_0", ["joined_0", 0])
    square = sal.multiply(byZero, "square", "byZero_1", ["byZero_1", "byZero_1"])
    opened = sal.collect(square, "opened", 1)

    # return root nodes
    return set([inA])

if __name__ == "__main__":

    print(protocol())
