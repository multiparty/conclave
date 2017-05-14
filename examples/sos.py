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

    # specify the workflow
    squares = sal.multiply(inA, "squares", "inA_1", ["inA_1", "inA_1"])
    sos = sal.aggregate(squares, "sos", "squares_0", "squares_1", "+")
    
    return set([inA])

if __name__ == "__main__":

    print(protocol())