import salmon.lang as sal
from salmon.comp import dagonly
from salmon.codegen import spark, viz, CodeGenConfig
from salmon.utils import *

@dagonly
def protocol():

    # define inputs
    colsInA = [
        defCol("companyID", "INTEGER", [1, 2, 3]),
        defCol("cab_data_1", "STRING", [1, 2, 3]),
        defCol("cab_data_2", "STRING", [1, 2, 3]),
        defCol("cab_data_3", "STRING", [1, 2, 3]),
        defCol("cab_data_4", "STRING", [1, 2, 3]),
        defCol("cab_data_5", "STRING", [1, 2, 3]),
        defCol("cab_data_6", "STRING", [1, 2, 3]),
        defCol("cab_data_7", "STRING", [1, 2, 3]),
        defCol("cab_data_8", "STRING", [1, 2, 3]),
        defCol("cab_data_9", "STRING", [1, 2, 3]),
        defCol("cab_data_10", "STRING", [1, 2, 3]),
        defCol("cab_data_11", "STRING", [1, 2, 3]),
        defCol("cab_data_12", "STRING", [1, 2, 3]),
        defCol("cab_data_13", "STRING", [1, 2, 3]),
        defCol("cab_data_14", "STRING", [1, 2, 3]),
        defCol("cab_data_15", "STRING", [1, 2, 3]),
        defCol("cab_data_16", "STRING", [1, 2, 3]),
        defCol("price", "INTEGER", [1, 2, 3])
    ]
    cab_data = sal.create("cab_data", colsInA, set([1]))

    selected_input = sal.project(cab_data, "selected_input", ["companyID", "price"])
    local_rev = sal.aggregate(selected_input, "local_rev", ["companyID"], "price", "+", "local_rev")
    scaled_down = sal.divide(local_rev, "scaled_down", "local_rev", ["local_rev", 1000])
    first_val_blank = sal.multiply(scaled_down, "first_val_blank", "companyID", ["companyID", 0])
    local_rev_scaled = sal.multiply(first_val_blank, "local_rev_scaled", "local_rev", ["local_rev", 100])
    total_rev = sal.aggregate(first_val_blank, "total_rev", ["companyID"], "local_rev", "+", "global_rev")
    local_total_rev = sal.join(local_rev_scaled, total_rev, "local_total_rev", ["companyID"], ["companyID"])
    market_share = sal.divide(local_total_rev, "market_share", "local_rev", ["local_rev", "global_rev"])
    market_share_squared = sal.multiply(market_share, "market_share_squared", "local_rev",
                                        ["local_rev", "local_rev"])
    hhi = sal.aggregate(market_share_squared, "hhi", ["companyID"], "local_rev", "+", "hhi")

    opened = sal.collect(hhi, 1)

    # return root nodes
    return set([cab_data])

if __name__ == "__main__":

    dag = protocol()

    config = CodeGenConfig()

    vg = viz.VizCodeGen(config, dag)
    vg.generate("taxi", "/tmp")

    cg = spark.SparkCodeGen(config, dag)
    cg.generate("taxi", "/tmp")

    print("Spark code generated in {}".format(config.code_path))
