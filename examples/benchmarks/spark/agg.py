import salmon.lang as sal
from salmon.comp import dagonly
from salmon.utils import *
from salmon.codegen import CodeGenConfig, spark


def agg():

    @dagonly
    def protocol():

        colsInA = [
            defCol('a', 'INTEGER', [1]),
            defCol('b', 'INTEGER', [1]),
            defCol('c', 'INTEGER', [1]),
            defCol('d', 'INTEGER', [1])
        ]

        in1 = sal.create("in1", colsInA, set([1]))
        agg1 = sal.aggregate(in1, 'agg1', ['a'], ['b'], '+', 'b')

        return set([in1])

    dag = protocol()
    config = CodeGenConfig('agg_spark')
    cg = spark.SparkCodeGen(config, dag)
    cg.generate('agg_spark', '/tmp')

if __name__ == "__main__":

    agg()

