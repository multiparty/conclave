import os
from unittest import TestCase

import conclave.lang as sal
from conclave.comp import mpc, scotch
from conclave.utils import *


class TestConclave(TestCase):

    def check_workflow(self, code, name):
        expected_rootdir = "{}/rewrite_expected".format(os.path.dirname(os.path.realpath(__file__)))

        with open(expected_rootdir + '/{}'.format(name), 'r') as f:
            expected = f.read()

        self.assertEqual(expected, code)

    def test_single_concat(self):
        @scotch
        @mpc
        def protocol():
            # define inputs
            cols_in_1 = [
                defCol("a", "INTEGER", [1]),
                defCol("b", "INTEGER", [1])
            ]
            in_1 = sal.create("in_1", cols_in_1, {1})
            cols_in_2 = [
                defCol("a", "INTEGER", [2]),
                defCol("b", "INTEGER", [2])
            ]
            in_2 = sal.create("in_2", cols_in_2, {2})
            cols_in_3 = [
                defCol("a", "INTEGER", [3]),
                defCol("b", "INTEGER", [3])
            ]
            in_3 = sal.create("in_3", cols_in_3, {3})

            # combine parties' inputs into one relation
            rel = sal.concat([in_1, in_2, in_3], "rel")

            sal.collect(rel, 1)

            # return root nodes
            return {in_1, in_2, in_3}

        actual = protocol()
        self.check_workflow(actual, 'concat')

    def test_single_agg(self):
        @scotch
        @mpc
        def protocol():
            # define inputs
            cols_in_1 = [
                defCol("a", "INTEGER", [1]),
                defCol("b", "INTEGER", [1])
            ]
            in_1 = sal.create("in_1", cols_in_1, {1})
            cols_in_2 = [
                defCol("a", "INTEGER", [2]),
                defCol("b", "INTEGER", [2])
            ]
            in_2 = sal.create("in_2", cols_in_2, {2})

            # combine parties' inputs into one relation
            rel = sal.concat([in_1, in_2], "rel")

            # specify the workflow
            agg = sal.aggregate(rel, "agg", ["a"], "b", "+", "total_b")

            sal.collect(agg, 1)

            # return root nodes
            return {in_1, in_2}

        actual = protocol()
        self.check_workflow(actual, 'agg')

    def test_single_proj(self):
        @scotch
        @mpc
        def protocol():
            # define inputs
            cols_in_1 = [
                defCol("a", "INTEGER", [1]),
                defCol("b", "INTEGER", [1])
            ]
            in_1 = sal.create("in_1", cols_in_1, {1})
            cols_in_2 = [
                defCol("a", "INTEGER", [2]),
                defCol("b", "INTEGER", [2])
            ]
            in_2 = sal.create("in_2", cols_in_2, {2})

            # combine parties' inputs into one relation
            rel = sal.concat([in_1, in_2], "rel")

            # specify the workflow
            proj = sal.project(rel, "proj", ["a", "b"])

            sal.collect(proj, 1)

            # return root nodes
            return {in_1, in_2}

        actual = protocol()
        self.check_workflow(actual, 'proj')

    def test_single_mult(self):
        @scotch
        @mpc
        def protocol():
            # define inputs
            cols_in_1 = [
                defCol("a", "INTEGER", [1]),
                defCol("b", "INTEGER", [1])
            ]
            in_1 = sal.create("in_1", cols_in_1, {1})
            cols_in_2 = [
                defCol("a", "INTEGER", [2]),
                defCol("b", "INTEGER", [2])
            ]
            in_2 = sal.create("in_2", cols_in_2, {2})

            # combine parties' inputs into one relation
            rel = sal.concat([in_1, in_2], "rel")

            # specify the workflow
            mult = sal.multiply(rel, "mult", "a", ["a", 1])

            sal.collect(mult, 1)

            # return root nodes
            return {in_1, in_2}

        actual = protocol()
        self.check_workflow(actual, 'mult')

    def test_single_div(self):
        @scotch
        @mpc
        def protocol():
            # define inputs
            cols_in_1 = [
                defCol("a", "INTEGER", [1]),
                defCol("b", "INTEGER", [1])
            ]
            in_1 = sal.create("in_1", cols_in_1, {1})
            cols_in_2 = [
                defCol("a", "INTEGER", [2]),
                defCol("b", "INTEGER", [2])
            ]
            in_2 = sal.create("in_2", cols_in_2, {2})

            # combine parties' inputs into one relation
            rel = sal.concat([in_1, in_2], "rel")

            # specify the workflow
            mult = sal.divide(rel, "mult", "a", ["a", "b"])

            sal.collect(mult, 1)

            # return root nodes
            return {in_1, in_2}

        actual = protocol()
        self.check_workflow(actual, 'div')

    def test_single_filter(self):
        @scotch
        @mpc
        def protocol():
            # define inputs
            cols_in_1 = [
                defCol("a", "INTEGER", [1]),
                defCol("b", "INTEGER", [1])
            ]
            in_1 = sal.create("in_1", cols_in_1, {1})
            cols_in_2 = [
                defCol("a", "INTEGER", [2]),
                defCol("b", "INTEGER", [2])
            ]
            in_2 = sal.create("in_2", cols_in_2, {2})

            # combine parties' inputs into one relation
            rel = sal.concat([in_1, in_2], "rel")

            # specify the workflow
            filtered = sal.filter(rel, "filtered", "a", "==", scalar=42)

            sal.collect(filtered, 1)

            # return root nodes
            return {in_1, in_2}

        actual = protocol()
        self.check_workflow(actual, 'filter')
