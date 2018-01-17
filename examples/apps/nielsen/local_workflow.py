import salmon.lang as sal
from salmon.comp import dag_only
from salmon.codegen import spark, viz
from salmon import CodeGenConfig
from salmon.utils import *


'''
NOTE: This script is intended to be run locally on the output of preprocess_movement.py
after the data has been separated into distinct parties by retailer_code.
'''

@dag_only
def protocol():

    colsInA = [
        defCol("store_code_uc", "STRING", [1]),
        defCol("upc", "STRING", [1]),
        defCol("week_end", "STRING", [1]),
        defCol("units", "INTEGER", [1]),
        defCol("prmult", "INTEGER", [1]),
        defCol("price", "FLOAT", [1]),
        defCol("retailer_code", "STRING", [1]),
        defCol("store_zip3", "STRING", [1])
    ]
    create = sal.create("movement", colsInA, set([1]))

    # divides 'price' by 'prmult' to compute unit price.
    w_unit_p = sal.divide(create, "w_unit_p", 'unit_price', ['price', 'prmult'])

    # aggregate multiple entries for the same (store, product, week) combination
    sum_units = sal.aggregate(w_unit_p, 'sum_units', ['store_code_uc', 'upc', 'week_end'], 'units', '+', 'q')

    # add 'unit_price' to each row keyed by (store, product, week)
    total_units = sal.join(w_unit_p, sum_units, 'total_units', ['store_code_uc', 'upc', 'week_end'],
                           ['store_code_uc', 'upc', 'week_end'])

    # computed weighted unit price (multiply aggregate units sold by their per-unit price)
    wghtd_total = sal.multiply(total_units, 'wghtd_total', 'wghtd_unit_p', ['units', 'unit_price'])

    # compute some kind of weighted per-unit price by dividing by 'q' (total units sold)
    wghtd_total_final = sal.divide(wghtd_total, 'wghtd_total_final', 'wghtd_unit_p', ['wghtd_unit_p', 'q'])

    total_unit_wghts = sal.aggregate(wghtd_total_final, 'total_unit_wghts', ['store_code_uc', 'upc', 'week_end'],
                                     'wghtd_unit_p', '+', 'avg_unit_p')

    # merge in avg_unit_p
    final_join = sal.join(total_units, total_unit_wghts, 'final_join', ['store_code_uc', 'upc', 'week_end'],
                          ['store_code_uc', 'upc', 'week_end'])

    selected_cols = sal.project(final_join, 'selected_cols',
                                ['store_code_uc', 'upc', 'week_end', 'q', 'avg_unit_p', 'retailer_code', 'store_zip3'])

    opened = sal.collect(selected_cols, 1)

    return set([create])

if __name__ == "__main__":

    dag = protocol()

    config = CodeGenConfig("nielsen-local")

    vg = viz.VizCodeGen(config, dag)
    vg.generate("local_workflow", "/tmp")

    cg = spark.SparkCodeGen(config, dag)
    cg.generate("local_workflow", "/tmp")

    print("Spark code generated in {}".format(config.code_path))
