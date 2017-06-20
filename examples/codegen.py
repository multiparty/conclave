import salmon.lang as sal
from salmon.comp import mpc
from salmon.codegen import spark

@mpc
def protocol():

    # define inputs
    colsInA = [
        ("INTEGER", set([1, 2, 3])),
        ("INTEGER", set([1, 2, 3])),
        ("INTEGER", set([1, 2, 3]))
    ]
    inA = sal.create("inA", colsInA, set([1]))

    # specify the workflow
    agg = sal.aggregate(inA, "agg", "inA_0", "inA_1", "+")
    projA = sal.project(agg, "projA", ["agg_0", "agg_1"])
    projB = sal.project(projA, "projB", ["projA_0", "projA_1"])
    # mult = sal.multiply(projB, "mult", "projB_0", "projB_1")
    opened = sal.collect(projB, 1)

    # return root nodes
    return set([inA])

if __name__ == "__main__":

    dag = protocol()

    cg = spark.SparkCodeGen(dag)
    cg.generate("aggtest", "/tmp")

    print("Spark code generated in /tmp/aggtest.py")
