import salmon.lang as sal
from salmon.comp import dagonly
from salmon.utils import *
from salmon.codegen import CodeGenConfig, spark


def join():

    @dagonly
    def protocol():

        colsInA = [
            defCol('a', 'INTEGER', [1]),
            defCol('b', 'INTEGER', [1]),
            defCol('c', 'INTEGER', [1]),
            defCol('d', 'INTEGER', [1])
        ]

        colsInB = [
            defCol('a', 'INTEGER', [1]),
            defCol('b', 'INTEGER', [1]),
            defCol('c', 'INTEGER', [1]),
            defCol('d', 'INTEGER', [1])
        ]

        in1 = sal.create("in1", colsInA, set([1]))
        in2 = sal.create("in2", colsInB, set([1]))
        join1 = sal.join(in1, in2, 'join1', ['a', 'b'], ['a', 'b'])

        return set([in1])

    dag = protocol()
    config = CodeGenConfig('join_spark')
    cg = spark.SparkCodeGen(config, dag)
    cg.generate('join_spark', '/tmp')

if __name__ == "__main__":

    join()

