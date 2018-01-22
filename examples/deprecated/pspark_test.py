import conclave.lang as sal
from conclave.codegen.spark import SparkCodeGen
from conclave import CodeGenConfig
from conclave.utils import *
from conclave.comp import dag_only


def setup():

    colsIn1 = [
        defCol("a", "INTEGER", [1]),
        defCol("b", "INTEGER", [1]),
        defCol("c", "INTEGER", [1]),
        defCol("d", "INTEGER", [1])
    ]

    in1 = sal.create("in1", colsIn1, set([1]))

    in2 = sal.create("in2", colsIn1, set([1]))

    return [in1, in2]


@dag_only
def agg():

    in1 = setup()[0]

    agg = sal.aggregate(in1, "agg", ["a", "b"], "c", "sum", "agg1")

    out = sal.collect(agg, 1)

    return set([in1])


if __name__ == "__main__":

    dag_agg = agg()
    cfg_agg = CodeGenConfig('agg')
    cg_agg = SparkCodeGen(cfg_agg, dag_agg)
    cg_agg.generate('agg', '/tmp')
