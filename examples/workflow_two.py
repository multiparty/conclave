import salmon.lang as sal
from salmon.comp import mpc
from salmon.codegen import spark

@mpc
def protocol():

    # define inputs
    colsInA = [
        ("INTEGER", set([1, 2, 3, 4, 5, 6])),
        ("INTEGER", set([1, 2, 3, 4, 5, 6])),
        ("INTEGER", set([1, 2, 3, 4, 5, 6])),
        ("INTEGER", set([1, 2, 3, 4, 5, 6])),
        ("INTEGER", set([1, 2, 3, 4, 5, 6])),
        ("INTEGER", set([1, 2, 3, 4, 5, 6]))
    ]
    inA = sal.create("inA", colsInA, set([1]))

    projA = sal.project(inA, "projA", ["inA_0", "inA_1"])
    projB = sal.project(inA, "projB", ["inA_2"])
    multA = sal.multiply(inA, "multA", "inA_3", ["inA_4", "inA_5"])
    opened = sal.collect(multA, 1)
    concatA = sal.concat([projB, multA], "concatA")
    '''
    joinA = sal.join("projA", "concatA", "joinA", "projA_0", "concatA_0")
    aggA = sal.aggregate(joinA, "aggA", "joinA_0", "joinA_1", "+")
    aggB = sal.aggregate(joinA, "aggB", "joinA_3", "joinA_4", "+")
    opened = sal.collect(joinA, 1)
    '''


    # return root nodes
    return set([inA])

if __name__ == "__main__":

    dag = protocol()

    cg = spark.SparkCodeGen(dag)
    cg.generate("jointest", "/tmp")

    print("Spark code generated in /tmp/jointest.py")