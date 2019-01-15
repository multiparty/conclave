import os
from unittest import TestCase

import conclave.lang as cc
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
            in_1 = cc.create("in_1", cols_in_1, {1})
            cols_in_2 = [
                defCol("a", "INTEGER", [2]),
                defCol("b", "INTEGER", [2])
            ]
            in_2 = cc.create("in_2", cols_in_2, {2})
            cols_in_3 = [
                defCol("a", "INTEGER", [3]),
                defCol("b", "INTEGER", [3])
            ]
            in_3 = cc.create("in_3", cols_in_3, {3})

            # combine parties' inputs into one relation
            rel = cc.concat([in_1, in_2, in_3], "rel")

            cc.collect(rel, 1)

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
            in_1 = cc.create("in_1", cols_in_1, {1})
            cols_in_2 = [
                defCol("a", "INTEGER", [2]),
                defCol("b", "INTEGER", [2])
            ]
            in_2 = cc.create("in_2", cols_in_2, {2})

            # combine parties' inputs into one relation
            rel = cc.concat([in_1, in_2], "rel")

            # specify the workflow
            agg = cc.aggregate(rel, "agg", ["a"], "b", "+", "total_b")

            cc.collect(agg, 1)

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
            in_1 = cc.create("in_1", cols_in_1, {1})
            cols_in_2 = [
                defCol("a", "INTEGER", [2]),
                defCol("b", "INTEGER", [2])
            ]
            in_2 = cc.create("in_2", cols_in_2, {2})

            # combine parties' inputs into one relation
            rel = cc.concat([in_1, in_2], "rel")

            # specify the workflow
            proj = cc.project(rel, "proj", ["a", "b"])

            cc.collect(proj, 1)

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
            in_1 = cc.create("in_1", cols_in_1, {1})
            cols_in_2 = [
                defCol("a", "INTEGER", [2]),
                defCol("b", "INTEGER", [2])
            ]
            in_2 = cc.create("in_2", cols_in_2, {2})

            # combine parties' inputs into one relation
            rel = cc.concat([in_1, in_2], "rel")

            # specify the workflow
            mult = cc.multiply(rel, "mult", "a", ["a", 1])

            cc.collect(mult, 1)

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
            in_1 = cc.create("in_1", cols_in_1, {1})
            cols_in_2 = [
                defCol("a", "INTEGER", [2]),
                defCol("b", "INTEGER", [2])
            ]
            in_2 = cc.create("in_2", cols_in_2, {2})

            # combine parties' inputs into one relation
            rel = cc.concat([in_1, in_2], "rel")

            # specify the workflow
            mult = cc.divide(rel, "mult", "a", ["a", "b"])

            cc.collect(mult, 1)

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
            in_1 = cc.create("in_1", cols_in_1, {1})
            cols_in_2 = [
                defCol("a", "INTEGER", [2]),
                defCol("b", "INTEGER", [2])
            ]
            in_2 = cc.create("in_2", cols_in_2, {2})

            # combine parties' inputs into one relation
            rel = cc.concat([in_1, in_2], "rel")

            # specify the workflow
            filtered = cc.cc_filter(rel, "filtered", "a", "==", scalar=42)

            cc.collect(filtered, 1)

            # return root nodes
            return {in_1, in_2}

        actual = protocol()
        self.check_workflow(actual, 'filter')
