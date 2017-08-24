import salmon.lang as sal
from salmon.comp import dagonly
from salmon.codegen import spark
from salmon.utils import *

'''
NOTE: This script is intended to be run for multiple files within a directory. The directory is
identified by user-specified 'year' and 'pg' values, and the resulting RDD's are concatenated
before compute_workflow.py. For our purposes, we can just run it once and modify the generated
code to iterate over files in a directory and concatenate the results along the way.
'''

@dagonly
def protocol():

    # ['store_code_uc', 'upc', 'week_end', 'units', 'prmult', 'price', 'feature', 'display']

    # TODO: look at nielsen data and find appropriate column names / order
    colsInA = [
        defCol("store_code_uc", "INTEGER", [1]),
        defCol("upc", "INTEGER", [1]),
        defCol("week_end", "INTEGER", [1]),
        defCol("units", "INTEGER", [1]),
        defCol("prmult", "INTEGER", [1]),
        defCol("price", "FLOAT", [1]),
        defCol("feature", "STRING", [1]),
        defCol("display", "STRING", [1])
    ]
    create = sal.create("in1", colsInA, set([1]))

    w_unit_p = sal.divide(create, "w_unit_p", 'unit_price', ['price', 'prmult'])
    sum_units = sal.aggregate(w_unit_p, 'sum_units', ['store_code_uc', 'upc', 'week_end'], 'units', '+', 'q')
    total_units = sal.join(w_unit_p, sum_units, 'total_units', ['store_code_uc', 'upc', 'week_end'],
                           ['store_code_uc', 'upc', 'week_end'])
    wghtd_total = sal.multiply(total_units, 'wghtd_total', 'wghtd_unit_p', ['units', 'unit_price'])
    wghtd_total_final = sal.divide(wghtd_total, 'wghtd_total_final', 'wghtd_unit_p', ['wghtd_unit_p', 'q'])
    total_unit_wghts = sal.aggregate(wghtd_total_final, 'total_unit_wghts', ['store_code_uc', 'upc', 'week_end'],
                                     'wghtd_unit_p', '+', 'avg_unit_p')
    final_join = sal.join(total_units, total_unit_wghts, 'final_join', ['store_code_uc', 'upc', 'week_end'],
                          ['store_code_uc', 'upc', 'week_end'])

    opened = sal.collect(final_join, 1)

    return set([create])

if __name__ == "__main__":

    dag = protocol()

    cg = spark.SparkCodeGen(dag)
    cg.generate("nielsen_create", "/tmp")

    print("Spark code generated in /tmp/nielsen_create.py")