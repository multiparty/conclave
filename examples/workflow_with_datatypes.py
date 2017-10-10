import salmon.lang as sal
from salmon.comp import mpc
from salmon.codegen import spark, CodeGenConfig
from salmon.utils import *

@mpc
def protocol():

    # define inputs
    colsInA = [
        defCol("a", "STRING", [1]),
        defCol("b", "INTEGER", [1]),
        defCol("c", "STRING", [1]),
        defCol("d", "INTEGER", [1]),
        defCol("e", "FLOAT", [1]),
        defCol("f", "FLOAT", [1])
    ]
    inA = sal.create("inA", colsInA, set([1]))
    multFloats = sal.multiply(inA, "mult", "e", ["e", "f"])
    projStringInt = sal.project(multFloats, "projStringInt", ["a", "b"])
    projStringInt2 = sal.project(multFloats, "projStringInt2", ["c", "d"])
    projStringFloat = sal.project(multFloats, "projStringFloat", ["a", "e"])
    projStringFloat2 = sal.project(multFloats, "projStringFloat2", ["c", "f"])
    joinIntsOnString = sal.join(projStringInt, projStringInt2, "joinIntsOnString", ["a"], ["c"])
    joinFloatsOnString = sal.join(projStringFloat, projStringFloat2, "joinFloatsOnString", ["a"], ["c"])
    # TODO(malte): this concats ints and float, which I guess we assume is fine, even though they
    # aren't the same column types?
    concat = sal.concat([joinIntsOnString, joinFloatsOnString], "concat")
    mult = sal.multiply(concat, "mult", "b", ["b", 10])
    opened = sal.collect(mult, 1)

    # return root nodes
    return set([inA])

if __name__ == "__main__":

    dag = protocol()
    config = CodeGenConfig("dtypes")

    cg = spark.SparkCodeGen(config, dag)
    cg.generate("dtypes", "/tmp")

    print("Spark code generated in {}".format(config.code_path))
