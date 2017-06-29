import salmon.lang as sal
from salmon.comp import mpc
from salmon.codegen import spark
from salmon.utils import *

@mpc
def protocol():

    # define inputs
    colsInA = [
        defCol("INTEGER", [1]),
        defCol("INTEGER", [1]),
        defCol("INTEGER", [1]),
        defCol("INTEGER", [1]),
        defCol("INTEGER", [1]),
        defCol("INTEGER", [1])
    ]
    inA = sal.create("inA", colsInA, set([1]))

    multA = sal.multiply(inA, "multA", "inA_3", ["inA_4", "inA_5"])
    projA = sal.project(multA, "projA", ["multA_0", "multA_1"])
    projB = sal.project(multA, "projB", ["multA_2", "multA_3"])
    projC = sal.project(multA, "projC", ["multA_4", "multA_5"])
    concatA = sal.concat([projA, projB], "concatA")
    concatB = sal.concat([projB, projC], "concatB")
    joinA = sal.join(concatA, concatB, "joinA", "concatA_0", "concatB_0")
    opened = sal.collect(joinA, 1)

    # return root nodes
    return set([inA])

if __name__ == "__main__":

    dag = protocol()

    cg = spark.SparkCodeGen(dag)
    cg.generate("jointest1", "/tmp")

    print("Spark code generated in /tmp/jointest.py")