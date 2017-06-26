import salmon.lang as sal
from salmon.comp import mpc
from salmon.codegen import spark

@mpc
def protocol():

    # define inputs
    colsInA = [
        ("INTEGER", set([1, 2, 3])),
        ("INTEGER", set([1, 2, 3])),
        ("INTEGER", set([1, 2, 3])),
        ("INTEGER", set([1, 2, 3]))
    ]
    inA = sal.create("inA", colsInA, set([1]))

    projA = sal.project(inA, "projA", ["inA_0", "inA_1"])
    projB = sal.project(inA, "projB", ["inA_2", "inA_3"])
    join_AB = sal.join(projA, projB, "join_AB", "projA_0", "projB_0")
    opened = sal.collect(join_AB, 1)

    # return root nodes
    return set([inA])

if __name__ == "__main__":

    dag = protocol()

    cg = spark.SparkCodeGen(dag)
    cg.generate("jointest", "/tmp")

    print("Spark code generated in /tmp/jointest.py")