import salmon.lang as sal
from salmon.codegen.spark import SparkCodeGen
from salmon import CodeGenConfig
from salmon.utils import *
from salmon.comp import dagonly
import os


def setup():

    cols = [
        defCol("a", "INTEGER", [1]),
        defCol("b", "INTEGER", [1]),
        defCol("c", "INTEGER", [1]),
        defCol("d", "INTEGER", [1])
    ]

    in_1 = sal.create("in_1", cols, set([1]))

    in_2 = sal.create("in_2", cols, set([1]))

    return [in_1, in_2]


@dagonly
def divide():
    inpts = setup()
    in_1 = inpts[0]

    div = sal.divide(in_1, "div", "a", ["a", "b"])
    out = sal.collect(div, 1)

    return set([in_1])


@dagonly
def multiply():
    inpts = setup()
    in_1 = inpts[0]

    mult = sal.multiply(in_1, "mult", "a", ["a", "b"])
    out = sal.collect(mult, 1)

    return set([in_1])


@dagonly
def project():
    inpts = setup()
    in_1 = inpts[0]

    proj = sal.project(in_1, "proj", ["a", "b"])
    out = sal.collect(proj, 1)

    return set([in_1])


@dagonly
def join():
    inpts = setup()
    in_1, in_2 = inpts[0], inpts[1]

    join = sal.join(in_1, in_2, 'join', ['a', 'b', 'c', 'd'], ['a', 'b', 'c', 'd'])
    out = sal.collect(join, 1)

    return set([in_1, in_2])


@dagonly
def agg():
    inpts = setup()
    in_1 = inpts[0]

    agg = sal.aggregate(in_1, "agg", ["a", "b"], "c", "sum", "agg_1")
    out = sal.collect(agg, 1)

    return set([in_1])


@dagonly
def concat():
    inpts = setup()
    in_1, in_2 = inpts[0], inpts[1]

    cc = sal.concat([in_1, in_2], "cc")
    out = sal.collect(cc, 1)

    return set([in_1, in_2])


@dagonly
def distinct():
    inpts = setup()
    in_1 = inpts[0]

    dist = sal.distinct(in_1, "dist", ["a", "b"])
    out = sal.collect(dist, 1)

    return set([in_1])


@dagonly
def index():
    inpts = setup()
    in_1 = inpts[0]

    ind = sal.index(in_1, "ind")
    out = sal.collect(ind, 1)

    return set([in_1])


@dagonly
def workflow_one():

    inpts = setup()
    in_1, in_2 = inpts[0], inpts[1]

    div_1 = sal.divide(in_1, "div", "a", ["a", "b"])
    mult_2 = sal.multiply(in_2, "mult", "a", ["a", "b"])
    proj_1 = sal.project(div_1, "proj", ["a", "b"])
    join = sal.join(proj_1, mult_2, "join", ["a", "b"], ["a", "b"])
    agg = sal.aggregate(join, "agg", ["a", "b"], "c", "sum", "agg_1")
    out = sal.collect(agg, 1)

    return set([in_1, in_2])


@dagonly
def workflow_two():

    inpts = setup()
    in_1, in_2 = inpts[0], inpts[1]

    cc = sal.concat([in_1, in_2], "cc")

    dist = sal.distinct(cc, "dist", ["a", "b", "c"])

    out = sal.collect(dist, 1)

    return set([in_1, in_2])


def test_workflow(dag, name):

    cfg = CodeGenConfig('cfg')

    expected_rootdir = \
        "{}/spark_expected".format(os.path.dirname(os.path.realpath(__file__)))

    cg = SparkCodeGen(cfg, dag)

    actual = cg._generate('code', '/tmp')[1]
    # uncomment this to regenerate (needed if .tmpl files change)
    # open(expected_rootdir + '/{}'.format(name), 'w').write(actual)

    expected = open(expected_rootdir + '/{}'.format(name), 'r').read()
    if actual == expected:
        print("{} passed".format(name))
    else:
        raise Exception("Error: {}".format(name))


if __name__ == "__main__":

    ops = [
        (divide(), 'divide'),
        (multiply(), 'multiply'),
        (project(), 'project'),
        (join(), 'join'),
        (agg(), 'agg'),
        (concat(), 'concat'),
        (distinct(), 'distinct'),
        (index(), 'index'),
        (workflow_one(), 'workflow_one'),
        (workflow_two(), 'workflow_two')
    ]

    for op in ops:
        test_workflow(op[0], op[1])
