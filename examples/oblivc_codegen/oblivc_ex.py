import conclave.lang as sal
from conclave.codegen.oblivc import OblivcCodeGen
import conclave.config as config
from conclave.utils import *
from conclave.comp import dag_only

import sys


def generate(dag):

    oc_conf = config.OblivcConfig(sys.argv[2], "localhost:9000")

    cfg = config.CodeGenConfig('cfg')
    cfg.input_path = sys.argv[1]

    cfg.with_oc_config(oc_conf)

    cg = OblivcCodeGen(cfg, dag, 1)

    cg.generate('oc_join', '/tmp')


@dag_only
def proj():

    colsInA = [
        defCol('a', 'INTEGER', [1]),
        defCol('b', 'INTEGER', [1]),
        defCol('c', 'INTEGER', [1]),
        defCol('d', 'INTEGER', [1])
    ]

    colsInB = [
        defCol("a", "INTEGER", [2]),
        defCol("b", "INTEGER", [2]),
        defCol("c", "INTEGER", [2]),
        defCol("d", "INTEGER", [2])
    ]

    in1 = sal.create("in1", colsInA, set([1]))
    in2 = sal.create("in2", colsInB, set([2]))

    cl1 = sal._close(in1, "cl1", set([1, 2, 3]))
    cl2 = sal._close(in2, "cl2", set([1, 2, 3]))

    rel = sal.concat([cl1, cl2], "rel")

    return set([in1, in2])


if __name__ == "__main__":

    dag = proj()
    generate(dag)
