import os
from unittest import TestCase

import conclave.lang as sal
from conclave import CodeGenConfig
from conclave.codegen.spark import SparkCodeGen
from conclave.comp import dag_only
from conclave.utils import *


def setup():
    cols = [
        defCol("a", "INTEGER", [1]),
        defCol("b", "INTEGER", [1]),
        defCol("c", "INTEGER", [1]),
        defCol("d", "INTEGER", [1])
    ]

    in_1 = sal.create("in_1", cols, {1})

    in_2 = sal.create("in_2", cols, {1})

    return [in_1, in_2]


class TestSpark(TestCase):

    def check_workflow(self, dag, name):
        expected_rootdir = \
            "{}/spark_expected".format(os.path.dirname(os.path.realpath(__file__)))

        cfg = CodeGenConfig('cfg')
        cg = SparkCodeGen(cfg, dag)

        actual = cg._generate('code', '/tmp')[1]

        with open(expected_rootdir + '/{}'.format(name), 'r') as f:
            expected = f.read()

        self.assertEqual(expected, actual)

    def test_sort_by(self):
        @dag_only
        def protocol():
            inpts = setup()
            in_1 = inpts[0]

            sorted = sal.sort_by(in_1, 'sorted1', 'a')
            out = sal.collect(sorted, 1)

            return {in_1}

        dag = protocol()
        self.check_workflow(dag, 'sort_by')

    def test_divide(self):
        @dag_only
        def protocol():
            inpts = setup()
            in_1 = inpts[0]

            div = sal.divide(in_1, "div", "a", ["a", "b"])
            out = sal.collect(div, 1)

            return {in_1}

        dag = protocol()
        self.check_workflow(dag, 'divide')

    def test_multiply(self):
        @dag_only
        def protocol():
            inpts = setup()
            in_1 = inpts[0]

            mult = sal.multiply(in_1, "mult", "a", ["a", "b"])
            out = sal.collect(mult, 1)

            return {in_1}

        dag = protocol()
        self.check_workflow(dag, 'multiply')

    def test_project(self):
        @dag_only
        def protocol():
            inpts = setup()
            in_1 = inpts[0]

            proj = sal.project(in_1, "proj", ["a", "b"])
            out = sal.collect(proj, 1)

            return {in_1}

        dag = protocol()
        self.check_workflow(dag, 'project')

    def test_join(self):
        @dag_only
        def protocol():
            inpts = setup()
            in_1, in_2 = inpts[0], inpts[1]

            join = sal.join(in_1, in_2, 'join', ['a', 'b', 'c', 'd'], ['a', 'b', 'c', 'd'])
            out = sal.collect(join, 1)

            return {in_1, in_2}

        dag = protocol()
        self.check_workflow(dag, 'join')

    def test_agg(self):
        @dag_only
        def protocol():
            inpts = setup()
            in_1 = inpts[0]

            agg = sal.aggregate(in_1, "agg", ["a", "b"], "c", "sum", "agg_1")
            out = sal.collect(agg, 1)

            return {in_1}

        dag = protocol()
        self.check_workflow(dag, 'agg')

    def test_concat(self):
        @dag_only
        def protocol():
            inpts = setup()
            in_1, in_2 = inpts[0], inpts[1]

            cc = sal.concat([in_1, in_2], "cc")
            out = sal.collect(cc, 1)

            return {in_1, in_2}

        dag = protocol()
        self.check_workflow(dag, 'concat')

    def test_distinct(self):
        @dag_only
        def protocol():
            inpts = setup()
            in_1 = inpts[0]

            dist = sal.distinct(in_1, "dist", ["a", "b"])
            out = sal.collect(dist, 1)

            return {in_1}

        dag = protocol()
        self.check_workflow(dag, 'distinct')

    def test_index(self):
        @dag_only
        def protocol():
            inpts = setup()
            in_1 = inpts[0]

            ind = sal.index(in_1, "index_1", "index")
            out = sal.collect(ind, 1)

            return {in_1}

        dag = protocol()
        self.check_workflow(dag, 'index')

    def test_workflow_one(self):
        @dag_only
        def protocol():
            inpts = setup()
            in_1, in_2 = inpts[0], inpts[1]

            div_1 = sal.divide(in_1, "div", "a", ["a", "b"])
            mult_2 = sal.multiply(in_2, "mult", "a", ["a", "b"])
            proj_1 = sal.project(div_1, "proj", ["a", "b"])
            join = sal.join(proj_1, mult_2, "join", ["a", "b"], ["a", "b"])
            agg = sal.aggregate(join, "agg", ["a", "b"], "c", "sum", "agg_1")
            out = sal.collect(agg, 1)

            return {in_1, in_2}

        dag = protocol()
        self.check_workflow(dag, 'workflow_one')

    def test_workflow_two(self):
        @dag_only
        def protocol():
            inpts = setup()
            in_1, in_2 = inpts[0], inpts[1]

            cc = sal.concat([in_1, in_2], "cc")

            dist = sal.distinct(cc, "dist", ["a", "b", "c"])

            out = sal.collect(dist, 1)

            return {in_1, in_2}

        dag = protocol()
        self.check_workflow(dag, 'workflow_two')
