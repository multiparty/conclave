from salmon import *

if __name__ == "__main__":

    # define inputs
    colsInA = [
        ("INTEGER", set([1, 2, 3])), 
        ("INTEGER", set([1, 2, 3]))
    ]
    inA = create("inA", colsInA)

    # specify the workflow
    mul = multiply(inA, "mul", "inA_0", ["inA_0", "inA_1", 100])
    
    # create dag with root nodes
    dag = OpDag(set([inA]))
    rewriteDag(dag)