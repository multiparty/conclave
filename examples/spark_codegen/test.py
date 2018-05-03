import conclave.lang as sal
from conclave.codegen.spark import SparkCodeGen
from conclave import CodeGenConfig
from conclave.utils import *
from conclave.comp import dag_only
import os


def generate(dag, name):

    cfg = CodeGenConfig('cfg')
    cg = SparkCodeGen(cfg, dag)

    actual = cg._generate('code', '/tmp')[1]

    with open('/tmp/' + name + '.py', 'w') as out:
        out.write(actual)


def setup():

    cols = [
        defCol("a", "INTEGER", [1]),
        defCol("b", "INTEGER", [1]),
        defCol("c", "INTEGER", [1]),
        defCol("d", "INTEGER", [1])
    ]

    in_1 = sal.create("in_1", cols, set([1]))

    return in_1


def sort_by():

    @dag_only
    def protocol():

        inpts = setup()
        in_1 = inpts

        sorted = sal.sort_by(in_1, 'sorted1', 'a')
        out = sal.collect(sorted, 1)

        return set([in_1])

    dag = protocol()

    return dag


def index():

    @dag_only
    def protocol():

        inpts = setup()
        in_1 = inpts

        indexed = sal.index(in_1, 'index_1', 'index')
        out = sal.collect(indexed, 1)

        return set([in_1])

    dag = protocol()

    return dag


def comp_neighs():

    @dag_only
    def protocol():

        inpts = setup()
        in_1 = inpts

        comp = sal._comp_neighs(in_1, 'comp', 'a')
        out = sal.collect(comp, 1)

        return set([in_1])

    dag = protocol()

    return dag


if __name__ == "__main__":

    dag = comp_neighs()
    generate(dag, 'comp')
