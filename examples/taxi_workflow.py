import salmon.lang as sal
from salmon.comp import dagonly
from salmon.codegen import spark, viz
from salmon.utils import *

@dagonly
def protocol():

    # define inputs
    colsInA = [
        defCol("INTEGER", [1, 2, 3]),
        defCol("STRING", [1, 2, 3]),
        defCol("STRING", [1, 2, 3]),
        defCol("STRING", [1, 2, 3]),
        defCol("STRING", [1, 2, 3]),
        defCol("STRING", [1, 2, 3]),
        defCol("STRING", [1, 2, 3]),
        defCol("STRING", [1, 2, 3]),
        defCol("STRING", [1, 2, 3]),
        defCol("STRING", [1, 2, 3]),
        defCol("STRING", [1, 2, 3]),
        defCol("STRING", [1, 2, 3]),
        defCol("STRING", [1, 2, 3]),
        defCol("STRING", [1, 2, 3]),
        defCol("STRING", [1, 2, 3]),
        defCol("STRING", [1, 2, 3]),
        defCol("STRING", [1, 2, 3]),
        defCol("INTEGER", [1, 2, 3])
    ]
    cab_data = sal.create("cab_data", colsInA, set([1]))

    selected_input = sal.project(cab_data, "selected_input", ["cab_data_0", "cab_data_17"])
    local_rev = sal.aggregate(selected_input, "local_rev", "selected_input_0",
                              "selected_input_1", "+")
    scaled_down = sal.divide(local_rev, "scaled_down", "local_rev_1", ["local_rev_1", 1000])
    first_val_blank = sal.multiply(scaled_down, "first_val_blank", "scaled_down_0",
                                   ["scaled_down_0", 0])
    local_rev_scaled = sal.multiply(first_val_blank, "local_rev_scaled", "first_val_blank_1",
                                    ["first_val_blank_1", 100])
    total_rev = sal.aggregate(first_val_blank, "total_rev", "first_val_blank_0",
                              "first_val_blank_1", "+")
    local_total_rev = sal.join(local_rev_scaled, total_rev, "local_total_rev",
                               "local_rev_scaled_0", "total_rev_0")
    market_share = sal.divide(local_total_rev, "market_share", "local_total_rev_1",
                              ["local_total_rev_1", "local_total_rev_2"])
    market_share_squared = sal.multiply(market_share, "market_share_squared", "market_share_1",
                                        ["market_share_1", "market_share_1"])
    hhi = sal.aggregate(market_share_squared, "hhi", "market_share_squared_1",
                        "market_share_squared_0", "+")

    opened = sal.collect(hhi, 1)

    # return root nodes
    return set([cab_data])

if __name__ == "__main__":

    dag = protocol()

    vg = viz.VizCodeGen(dag)
    vg.generate("taxi", "/tmp")

    cg = spark.SparkCodeGen(dag)
    cg.generate("taxi", "/tmp")

    print("Spark code generated in /tmp/taxi.py")
