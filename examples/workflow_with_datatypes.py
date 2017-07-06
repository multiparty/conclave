import salmon.lang as sal
from salmon.comp import mpc
from salmon.codegen import spark
from salmon.utils import *

@mpc
def protocol():

    # define inputs
    colsInA = [
        defCol("STRING", [1]),
        defCol("INTEGER", [1]),
        defCol("STRING", [1]),
        defCol("INTEGER", [1]),
        defCol("FLOAT", [1]),
        defCol("FLOAT", [1])
    ]
    inA = sal.create("inA", colsInA, set([1]))

    multA = sal.multiply(inA, "multA", "inA_4", ["inA_5"])
    projA = sal.project(multA, "projA", ["multA_0", "multA_1"])
    projB = sal.project(multA, "projB", ["multA_2", "multA_3"])
    projC = sal.project(multA, "projC", ["multA_0", "multA_4"])
    projD = sal.project(multA, "projD", ["multA_2", "multA_5"])
    joinA = sal.join(projA, projB, "joinA", "projA_0", "projB_0")
    joinB = sal.join(projC, projD, "joinB", "projC_0", "projD_0")
    concatA = sal.concat([joinA, joinB], "concatA")
    opened = sal.collect(concatA, 1)

    # return root nodes
    return set([inA])

if __name__ == "__main__":

    dag = protocol()

    cg = spark.SparkCodeGen(dag)
    cg.generate("dtypes", "/tmp")

    print("Spark code generated in /tmp/dtypes.py")