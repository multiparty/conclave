import salmon.lang as sal
from salmon.codegen.python import PythonCodeGen
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
def agg():
    inpts = setup()
    in_1 = inpts[0]

    agg = sal.aggregate(in_1, "agg", ["a", "b"], "c", "sum", "agg_1")
    out = sal.collect(agg, 1)

    return set([in_1])


@dagonly
def multiply():
    inpts = setup()
    in_1 = inpts[0]

    mult = sal.multiply(in_1, "mult", "a", ["a", "b"])
    out = sal.collect(mult, 1)

    return set([in_1])


@dagonly
def join():
    inpts = setup()
    in_1, in_2 = inpts[0], inpts[1]

    join = sal.join(in_1, in_2, 'join', ['a', 'b', 'c', 'd'], ['a', 'b', 'c', 'd'])
    out = sal.collect(join, 1)

    return set([in_1, in_2])


@dagonly
def project():
    inpts = setup()
    in_1 = inpts[0]

    proj = sal.project(in_1, "proj_1", ["a", "b"])
    out = sal.collect(proj, 1)

    return set([in_1])


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
def sort_by():
    inpts = setup()
    in_1 = inpts[0]

    sb = sal.sort_by(in_1, "sort_by", "a")
    out = sal.collect(sb, 1)

    return set([in_1])


@dagonly
def comp_neighs():
    inpts = setup()
    in_1 = inpts[0]

    cn = sal._comp_neighs(in_1, 'comp_neighs', 'b')
    out = sal.collect(cn, 1)

    return set([in_1])


@dagonly
def workflow_one():

    inpts = setup()
    in_1, in_2 = inpts[0], inpts[1]

    mult = sal.multiply(in_1, "mult", "a", ["b", "c"])
    proj_2 = sal.project(in_2, "proj_2", ["a", "b"])
    join = sal.join(mult, proj_2, "join", ["a", "b"], ["a", "b"])
    agg = sal.aggregate(join, "agg", ["a", "b"], "c", "sum", "agg_1")
    out = sal.collect(agg, 1)

    return set([in_1, in_2])


def test_workflow(dag, name):

    cfg = CodeGenConfig('cfg')

    expected_rootdir = \
        "{}/python_expected".format(os.path.dirname(os.path.realpath(__file__)))

    cg = PythonCodeGen(cfg, dag)

    actual = cg._generate('code', '/tmp')[1]
    # uncomment this to regenerate (needed if .tmpl files change)
    # open(expected_rootdir + '/{}'.format(name), 'w').write(actual)

    expected = open(expected_rootdir + '/{}'.format(name), 'r').read()
    if actual == expected:
        print("{} passed".format(name))
    else:
        raise Exception("Error: {}".format(name))


if __name__ == "__main__":

    dag_1 = workflow_one()

    ops = [
        (agg(), 'agg'),
        (multiply(), 'multiply'),
        (join(), 'join'),
        (project(), 'project'),
        (distinct(), 'distinct'),
        (index(), 'index'),
        (sort_by(), 'sort_by'),
        (comp_neighs(), 'comp_neighs'),
        (workflow_one(), 'workflow_one')
    ]

    for op in ops:
        test_workflow(op[0], op[1])


