import salmon.lang as sal
from salmon.comp import dagonly
from salmon.utils import *
from salmon.codegen import CodeGenConfig, spark
from random import shuffle


def project():

    @dagonly
    def protocol():

        colsInA = [
            defCol('a', 'INTEGER', [1]),
            defCol('b', 'INTEGER', [1]),
            defCol('c', 'INTEGER', [1]),
            defCol('d', 'INTEGER', [1])
        ]

        in1 = sal.create("in1", colsInA, set([1]))

        cols = ([column.name for column in in1.outRel.columns])
        shuffle(cols)

        proja = sal.project(in1, "proja", cols)

        return set([in1])

    dag = protocol()
    config = CodeGenConfig('project_spark')
    cg = spark.SparkCodeGen(config, dag)
    cg.generate('project_spark', '/tmp')

if __name__ == "__main__":

    project()
