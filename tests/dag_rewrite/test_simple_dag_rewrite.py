from unittest import TestCase
import warnings
import salmon.lang as sal
from salmon.comp import mpc, scotch
from salmon.utils import *
import os


def ignore_resource_warnings(test_func):
    def do_test(self, *args, **kwargs):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", ResourceWarning)
            test_func(self, *args, **kwargs)
    return do_test


class TestConclave(TestCase):

    @ignore_resource_warnings
    def check_workflow(self, code, name):
        expected_rootdir = "{}/rewrite_expected".format(os.path.dirname(os.path.realpath(__file__)))

        # uncomment this to regenerate (needed if rewrite logic changes)
        # open(expected_rootdir + '/{}'.format(name), 'w').write(code)
        expected = open(expected_rootdir + '/{}'.format(name), 'r').read()
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
            in_1 = sal.create("in_1", cols_in_1, set([1]))
            cols_in_2 = [
                defCol("a", "INTEGER", [2]),
                defCol("b", "INTEGER", [2])
            ]
            in_2 = sal.create("in_2", cols_in_2, set([2]))
            cols_in_3 = [
                defCol("a", "INTEGER", [3]),
                defCol("b", "INTEGER", [3])
            ]
            in_3 = sal.create("in_3", cols_in_3, set([3]))

            # combine parties' inputs into one relation
            rel = sal.concat([in_1, in_2, in_3], "rel")

            sal.collect(rel, 1)

            # return root nodes
            return set([in_1, in_2, in_3])

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
            in_1 = sal.create("in_1", cols_in_1, set([1]))
            cols_in_2 = [
                defCol("a", "INTEGER", [2]),
                defCol("b", "INTEGER", [2])
            ]
            in_2 = sal.create("in_2", cols_in_2, set([2]))

            # combine parties' inputs into one relation
            rel = sal.concat([in_1, in_2], "rel")

            # specify the workflow
            agg = sal.aggregate(rel, "agg", ["a"], "b", "+", "total_b")

            sal.collect(agg, 1)

            # return root nodes
            return set([in_1, in_2])

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
            in_1 = sal.create("in_1", cols_in_1, set([1]))
            cols_in_2 = [
                defCol("a", "INTEGER", [2]),
                defCol("b", "INTEGER", [2])
            ]
            in_2 = sal.create("in_2", cols_in_2, set([2]))

            # combine parties' inputs into one relation
            rel = sal.concat([in_1, in_2], "rel")

            # specify the workflow
            proj = sal.project(rel, "proj", ["a", "b"])

            sal.collect(proj, 1)

            # return root nodes
            return set([in_1, in_2])

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
            in_1 = sal.create("in_1", cols_in_1, set([1]))
            cols_in_2 = [
                defCol("a", "INTEGER", [2]),
                defCol("b", "INTEGER", [2])
            ]
            in_2 = sal.create("in_2", cols_in_2, set([2]))

            # combine parties' inputs into one relation
            rel = sal.concat([in_1, in_2], "rel")

            # specify the workflow
            mult = sal.multiply(rel, "mult", "a", ["a", 1])

            sal.collect(mult, 1)

            # return root nodes
            return set([in_1, in_2])

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
            in_1 = sal.create("in_1", cols_in_1, set([1]))
            cols_in_2 = [
                defCol("a", "INTEGER", [2]),
                defCol("b", "INTEGER", [2])
            ]
            in_2 = sal.create("in_2", cols_in_2, set([2]))

            # combine parties' inputs into one relation
            rel = sal.concat([in_1, in_2], "rel")

            # specify the workflow
            mult = sal.divide(rel, "mult", "a", ["a", "b"])

            sal.collect(mult, 1)

            # return root nodes
            return set([in_1, in_2])

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
            in_1 = sal.create("in_1", cols_in_1, set([1]))
            cols_in_2 = [
                defCol("a", "INTEGER", [2]),
                defCol("b", "INTEGER", [2])
            ]
            in_2 = sal.create("in_2", cols_in_2, set([2]))

            # combine parties' inputs into one relation
            rel = sal.concat([in_1, in_2], "rel")

            # specify the workflow
            mult = sal.filter(rel, "filtered", "a", "=", 42)

            sal.collect(mult, 1)

            # return root nodes
            return set([in_1, in_2])

        actual = protocol()
        self.check_workflow(actual, 'filter')



