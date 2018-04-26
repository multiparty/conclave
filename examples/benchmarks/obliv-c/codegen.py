import conclave.lang as sal
from conclave.codegen.oblivc import OblivcCodeGen
import conclave.config as config
from conclave.utils import *
from conclave.comp import dag_only

import sys


def generate(dag_one, name):
    """
    sys.argv[1] - file path to directory containing input file
    (full path is <path> + <input_rel_name> + '.csv')

    sys.argv[2] - path to obliv-c compiler (at /obliv-c/bin/oblivcc)

    sys.argv[3] - <host_ip>:<port>
    """

    oc_conf = config.OblivcConfig(sys.argv[2], sys.argv[3])

    cfg = config.CodeGenConfig(name)
    cfg.input_path = '/home/ubuntu/'

    cfg.with_oc_config(oc_conf)

    cg1 = OblivcCodeGen(cfg, dag_one, sys.argv[1])
    cg1.generate('protocol', '/home/ubuntu/')


def setup_four_cols():

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

    return [in1, in2]


def setup_two_cols():

    colsInA = [
        defCol('a', 'INTEGER', [1]),
        defCol('b', 'INTEGER', [1])
    ]

    colsInB = [
        defCol("a", "INTEGER", [2]),
        defCol("b", "INTEGER", [2])
    ]

    in1 = sal.create("in1", colsInA, set([1]))
    in2 = sal.create("in2", colsInB, set([2]))

    return [in1, in2]


@dag_only
def agg():

    in_rels = setup_two_cols()
    in1 = in_rels[0]
    in2 = in_rels[1]

    cl1 = sal._close(in1, "cl1", set([1, 2]))
    cl2 = sal._close(in2, "cl2", set([1, 2]))

    rel = sal.concat([cl1, cl2], "rel")

    agg = sal.aggregate(rel, 'agg1', ['b'], 'a', '+', 'b')

    opened = sal._open(agg, "opened", 1)

    return set([in1, in2])


@dag_only
def join():

    in_rels = setup_two_cols()
    in1 = in_rels[0]
    in2 = in_rels[1]

    cl1 = sal._close(in1, "cl1", set([1, 2]))
    cl2 = sal._close(in2, "cl2", set([1, 2]))

    join = sal.join(cl1, cl2, 'join1', ['a'], ['a'])

    opened = sal._open(join, "opened", 1)

    return set([in1, in2])


@dag_only
def concat():

    in_rels = setup_two_cols()
    in1 = in_rels[0]
    in2 = in_rels[1]

    cl1 = sal._close(in1, "cl1", set([1, 2]))
    cl2 = sal._close(in2, "cl2", set([1, 2]))

    rel = sal.concat([cl1, cl2], "rel")

    opened = sal._open(rel, "opened", 1)

    return set([in1, in2])


@dag_only
def col_divide():

    in_rels = setup_two_cols()
    in1 = in_rels[0]
    in2 = in_rels[1]

    cl1 = sal._close(in1, "cl1", set([1, 2]))
    cl2 = sal._close(in2, "cl2", set([1, 2]))

    rel = sal.concat([cl1, cl2], "rel")

    div = sal.divide(rel, "div1", "a", ["a", "b"])

    opened = sal._open(div, "opened", 1)

    return set([in1, in2])


@dag_only
def scal_divide():

    in_rels = setup_two_cols()
    in1 = in_rels[0]
    in2 = in_rels[1]

    cl1 = sal._close(in1, "cl1", set([1, 2]))
    cl2 = sal._close(in2, "cl2", set([1, 2]))

    rel = sal.concat([cl1, cl2], "rel")

    div = sal.divide(rel, "div1", "a", ["a", 10])

    opened = sal._open(div, "opened", 1)

    return set([in1, in2])


@dag_only
def col_multiply():

    in_rels = setup_two_cols()
    in1 = in_rels[0]
    in2 = in_rels[1]

    cl1 = sal._close(in1, "cl1", set([1, 2]))
    cl2 = sal._close(in2, "cl2", set([1, 2]))

    rel = sal.concat([cl1, cl2], "rel")

    mul = sal.multiply(rel, "mul1", "a", ["a", "b"])

    opened = sal._open(mul, "opened", 1)

    return set([in1, in2])


@dag_only
def scal_multiply():

    in_rels = setup_two_cols()
    in1 = in_rels[0]
    in2 = in_rels[1]

    cl1 = sal._close(in1, "cl1", set([1, 2]))
    cl2 = sal._close(in2, "cl2", set([1, 2]))

    rel = sal.concat([cl1, cl2], "rel")

    mul = sal.multiply(rel, "mul1", "a", ["a", 10])

    opened = sal._open(mul, "opened", 1)

    return set([in1, in2])


@dag_only
def project():

    in_rels = setup_four_cols()
    in1 = in_rels[0]
    in2 = in_rels[1]

    cl1 = sal._close(in1, "cl1", set([1, 2]))
    cl2 = sal._close(in2, "cl2", set([1, 2]))

    rel = sal.concat([cl1, cl2], "rel")

    proj = sal.project(rel, 'proj1', ['b', 'a', 'd', 'c'])

    opened = sal._open(proj, "opened", 1)

    return set([in1, in2])


if __name__ == "__main__":

    dag = agg()
    generate(dag, 'agg')

    dag = join()
    generate(dag, 'join')

    dag = concat()
    generate(dag, 'concat')

    dag = col_divide()
    generate(dag, 'col_divide')

    dag = scal_divide()
    generate(dag, 'scal_divide')

    dag = col_multiply()
    generate(dag, 'col_multiply')

    dag = scal_multiply()
    generate(dag, 'scal_multiply')

    dag = project()
    generate(dag, 'project')
