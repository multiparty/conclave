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
    """

    oc_conf = config.OblivcConfig(sys.argv[2], "localhost:9000")

    cfg = config.CodeGenConfig(name)
    cfg.input_path = sys.argv[1]
    cfg.use_leaky_ops = False

    cfg.with_oc_config(oc_conf)

    cg1 = OblivcCodeGen(cfg, dag_one, 1)
    cg1.generate('protocol1', '/tmp/prot/')

    cg2 = OblivcCodeGen(cfg, dag_one, 2)
    cg2.generate('protocol2', '/tmp/prot/')


def generate_leaky(dag_one, name):

    oc_conf = config.OblivcConfig(sys.argv[2], "localhost:9000")

    cfg = config.CodeGenConfig(name)
    cfg.input_path = sys.argv[1]

    cfg.with_oc_config(oc_conf)

    cg1 = OblivcCodeGen(cfg, dag_one, 1)
    cg1.generate('protocol1', '/tmp/prot/')

    cg2 = OblivcCodeGen(cfg, dag_one, 2)
    cg2.generate('protocol2', '/tmp/prot/')


def setup():

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


def setup_three():

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

    colsInC = [
        defCol("a", "INTEGER", [2]),
        defCol("b", "INTEGER", [2]),
        defCol("c", "INTEGER", [2]),
        defCol("d", "INTEGER", [2])
    ]

    in1 = sal.create("in1", colsInA, set([1]))
    in2 = sal.create("in2", colsInB, set([2]))
    in3 = sal.create("in3", colsInC, set([2]))

    return [in1, in2, in3]

@dag_only
def agg():

    in_rels = setup()
    in1 = in_rels[0]
    in2 = in_rels[1]

    cl1 = sal._close(in1, "cl1", set([1, 2]))
    cl2 = sal._close(in2, "cl2", set([1, 2]))

    rel = sal.concat([cl1, cl2], "rel")

    agg = sal.aggregate(rel, 'agg1', ['c'], 'a', '+', 'c_agg')

    opened = sal._open(agg, "opened", 1)

    return set([in1, in2])


@dag_only
def join():

    in_rels = setup()
    in1 = in_rels[0]
    in2 = in_rels[1]

    cl1 = sal._close(in1, "cl1", set([1, 2]))
    cl2 = sal._close(in2, "cl2", set([1, 2]))

    join = sal.join(cl1, cl2, 'join1', ['a'], ['a'])

    opened = sal._open(join, "opened", 1)

    return set([in1, in2])


@dag_only
def concat():

    in_rels = setup()
    in1 = in_rels[0]
    in2 = in_rels[1]

    cl1 = sal._close(in1, "cl1", set([1, 2]))
    cl2 = sal._close(in2, "cl2", set([1, 2]))

    rel = sal.concat([cl1, cl2], "rel")

    opened = sal._open(rel, "opened", 1)

    return set([in1, in2])


@dag_only
def distinct_count():

    in_rels = setup()
    in1 = in_rels[0]
    in2 = in_rels[1]

    cl1 = sal._close(in1, "cl1", set([1, 2]))
    cl2 = sal._close(in2, "cl2", set([1, 2]))

    rel = sal.concat([cl1, cl2], "rel")

    dis = sal.distinct_count(rel, 'dis', 'a')

    opened = sal._open(dis, "opened", 1)

    return set([in1, in2])


@dag_only
def divide():

    in_rels = setup()
    in1 = in_rels[0]
    in2 = in_rels[1]

    cl1 = sal._close(in1, "cl1", set([1, 2]))
    cl2 = sal._close(in2, "cl2", set([1, 2]))

    rel = sal.concat([cl1, cl2], "rel")

    div = sal.divide(rel, "div1", "a", ["a", "b", "c"])

    div2 = sal.divide(div, "div2", "e", ["b", "d"])

    opened = sal._open(div2, "opened", 1)

    return set([in1, in2])


@dag_only
def multiply():

    in_rels = setup()
    in1 = in_rels[0]
    in2 = in_rels[1]

    cl1 = sal._close(in1, "cl1", set([1, 2]))
    cl2 = sal._close(in2, "cl2", set([1, 2]))

    rel = sal.concat([cl1, cl2], "rel")

    mul = sal.multiply(rel, "mul1", "a", ["a", "b", "c"])

    mul2 = sal.multiply(mul, "mul2", "e", ["b", "d"])

    opened = sal._open(mul2, "opened", 1)

    return set([in1, in2])


@dag_only
def sort_by():

    in_rels = setup()
    in1 = in_rels[0]
    in2 = in_rels[1]

    cl1 = sal._close(in1, "cl1", set([1, 2]))
    cl2 = sal._close(in2, "cl2", set([1, 2]))

    rel = sal.concat([cl1, cl2], "rel")

    sb = sal.sort_by(rel, 'sb', 'a')

    opened = sal._open(sb, "opened", 1)

    return set([in1, in2])


@dag_only
def project():

    in_rels = setup()
    in1 = in_rels[0]
    in2 = in_rels[1]

    cl1 = sal._close(in1, "cl1", set([1, 2]))
    cl2 = sal._close(in2, "cl2", set([1, 2]))

    rel = sal.concat([cl1, cl2], "rel")

    proj = sal.project(rel, 'proj1', ['b', 'a', 'c'])

    opened = sal._open(proj, "opened", 1)

    return set([in1, in2])


if __name__ == "__main__":

    # dag = agg()
    # generate(dag, 'agg')
    #
    # dag = join()
    # generate(dag, 'join')
    #
    # dag = agg()
    # generate_leaky(dag, 'aggLeaky')
    #
    # dag = join()
    # generate_leaky(dag, 'joinLeaky')
    #
    # dag = concat()
    # generate(dag, 'concat')
    #
    # dag = multiply()
    # generate(dag, 'multiply')
    #
    # dag = divide()
    # generate(dag, 'divide')
    #
    # dag = sort_by()
    # generate_leaky(dag, 'sort_by')
    #
    # dag = project()
    # generate(dag, 'project')

    dag = distinct_count()
    generate(dag, 'dis')
