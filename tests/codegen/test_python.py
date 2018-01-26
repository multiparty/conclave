from unittest import TestCase
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


class TestPython(TestCase):

    def check_workflow(self, dag, name):
        expected_rootdir = \
            "{}/python_expected".format(os.path.dirname(os.path.realpath(__file__)))

        cfg = CodeGenConfig('cfg')
        cg = PythonCodeGen(cfg, dag)

        actual = cg._generate('code', '/tmp')[1]

        with open(expected_rootdir + '/{}'.format(name), 'r') as f:
            expected = f.read()

        self.assertEqual(expected, actual)

    def test_agg(self):

        @dagonly
        def protocol():
            inpts = setup()
            in_1 = inpts[0]

            agg = sal.aggregate(in_1, "agg", ["a", "b"], "c", "sum", "agg_1")
            out = sal.collect(agg, 1)

            return set([in_1])

        dag = protocol()
        self.check_workflow(dag, 'agg')

    def test_multiply(self):

        @dagonly
        def protocol():
            inpts = setup()
            in_1 = inpts[0]

            mult = sal.multiply(in_1, "mult", "a", ["a", "b"])
            out = sal.collect(mult, 1)

            return set([in_1])

        dag = protocol()
        self.check_workflow(dag, 'multiply')

    def test_join(self):

        @dagonly
        def protocol():
            inpts = setup()
            in_1, in_2 = inpts[0], inpts[1]

            join = sal.join(in_1, in_2, 'join', ['a', 'b', 'c', 'd'], ['a', 'b', 'c', 'd'])
            out = sal.collect(join, 1)

            return set([in_1, in_2])

        dag = protocol()
        self.check_workflow(dag, 'join')

    def test_project(self):

        @dagonly
        def protocol():
            inpts = setup()
            in_1 = inpts[0]

            proj = sal.project(in_1, "proj_1", ["a", "b"])
            out = sal.collect(proj, 1)

            return set([in_1])

        dag = protocol()
        self.check_workflow(dag, 'project')

    def test_distinct(self):

        @dagonly
        def protocol():
            inpts = setup()
            in_1 = inpts[0]

            dist = sal.distinct(in_1, "dist", ["a", "b"])
            out = sal.collect(dist, 1)

            return set([in_1])

        dag = protocol()
        self.check_workflow(dag, 'distinct')

    def test_index(self):

        @dagonly
        def protocol():
            inpts = setup()
            in_1 = inpts[0]

            ind = sal.index(in_1, "ind")
            out = sal.collect(ind, 1)

            return set([in_1])

        dag = protocol()
        self.check_workflow(dag, 'index')

    def test_sort_by(self):

        @dagonly
        def protocol():
            inpts = setup()
            in_1 = inpts[0]

            sb = sal.sort_by(in_1, "sort_by", "a")
            out = sal.collect(sb, 1)

            return set([in_1])

        dag = protocol()
        self.check_workflow(dag, 'sort_by')

    def test_comp_neighs(self):

        @dagonly
        def protocol():
            inpts = setup()
            in_1 = inpts[0]

            cn = sal._comp_neighs(in_1, 'comp_neighs', 'b')
            out = sal.collect(cn, 1)

            return set([in_1])

        dag = protocol()
        self.check_workflow(dag, 'comp_neighs')

    def test_workflow_one(self):

        @dagonly
        def protocol():
            inpts = setup()
            in_1, in_2 = inpts[0], inpts[1]

            mult = sal.multiply(in_1, "mult", "a", ["b", "c"])
            proj_2 = sal.project(in_2, "proj_2", ["a", "b"])
            join = sal.join(mult, proj_2, "join", ["a", "b"], ["a", "b"])
            agg = sal.aggregate(join, "agg", ["a", "b"], "c", "sum", "agg_1")
            out = sal.collect(agg, 1)

            return set([in_1, in_2])

        dag = protocol()
        self.check_workflow(dag, 'workflow_one')


