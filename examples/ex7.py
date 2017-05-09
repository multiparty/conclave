from salmon import *

if __name__ == "__main__":

    # define inputs
    colsInA = [
        ("INTEGER", set([1, 2, 3])), 
        ("INTEGER", set([1, 2, 3]))
    ]
    inA = create("inA", colsInA)

    # specify the workflow
    squares = multiply(inA, "squares", "inA_1", ["inA_1", "inA_1"])
    sos = aggregate(squares, "sos", "squares_0", "squares_1", "+")
    
    # create dag with root nodes
    dag = OpDag(set([inA]))
    rewriteDag(dag)