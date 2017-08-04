import salmon.lang as sal
from salmon.comp import mpc
from salmon.codegen import spark

@mpc
def protocol():

    # define inputs
    colsInA = [
        ("productID", "INTEGER", set([1, 2, 3])),
        ("price", "INTEGER", set([1, 2, 3])),
        ("amount", "INTEGER", set([1, 2, 3])),
        ("userID", "INTEGER", set([1, 2, 3]))
    ]
    inA = sal.create("inA", colsInA, set([1]))

    # specify the workflow
    proj = sal.project(inA, "projected", ["productID", "price", "amount"])
    mult = sal.multiply(proj, "subtotals", "price", ["price", "amount"])
    agg = sal.aggregate(mult, "revenue", "productID", "price", "+", "revenue")
    opened = sal.collect(mult, 1)

    # return root nodes
    return set([inA])

if __name__ == "__main__":

    dag = protocol()

    cg = spark.SparkCodeGen(dag)
    cg.generate("aggtest", "/tmp")

    print("Spark code generated in /tmp/aggtest.py")
