import salmon.lang as sal
from salmon.codegen import spark


def protocol():

    # define inputs
    colsInA = [
        ("INTEGER", set([1, 2, 3])),
        ("INTEGER", set([1, 2, 3])),
        ("INTEGER", set([1, 2, 3]))
    ]
    inA = sal.create("inA", colsInA, 1)

    # specify the workflow
    agg = sal.aggregate(inA, "agg", "inA_0", "inA_1", "sum")

    # return root nodes
    return set([inA])


if __name__ == "__main__":

    dag = protocol()

    cg = spark.SparkCodeGen(dag)
    cg.generate("aggtest", "/tmp")

    print("Spark code generated in /tmp/aggtest.py")