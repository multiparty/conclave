from unittest import TestCase
import warnings
import salmon.lang as sal
from salmon.comp import mpc, scotch
from salmon.utils import *
import os


# suppresses annoying warnings about open files
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

    def test_mult_by_zer0(self):

        @scotch
        @mpc
        def protocol():

            # define inputs
            cols_in_1 = [
                defCol("a", "INTEGER", [1]),
                defCol("b", "INTEGER", [1])
            ]
            in_1 = sal.create("in1", cols_in_1, set([1]))
            cols_in_2 = [
                defCol("a", "INTEGER", [2]),
                defCol("b", "INTEGER", [2])
            ]
            in_2 = sal.create("in_2", cols_in_2, set([2]))

            # combine parties' inputs into one relation
            rel = sal.concat([in_1, in_2], "rel")

            # specify the workflow
            mult = sal.multiply(rel, "mult", "a", ["a", 0])

            sal.collect(mult, 1)

            # return root nodes
            return set([in_1, in_2])

        actual = protocol()
        self.check_workflow(actual, 'mult_by_zero')

    def test_concat_pushdown(self):

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
            proj = sal.project(rel, "proj", ["a", "b"])
            agg = sal.aggregate(proj, "agg", ["a"], "b", "+", "total_b")

            sal.collect(agg, 1)

            # return root nodes
            return set([in_1, in_2, in_3])

        actual = protocol()
        self.check_workflow(actual, 'concat_pushdown')

    def test_agg_proj(self):

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
            proj_a = sal.project(rel, "proj_a", ["a", "b"])
            proj_b = sal.project(proj_a, "proj_b", ["a", "b"])
            agg = sal.aggregate(proj_b, "agg", ["a"], "b", "+", "total_b")
            proj_c = sal.project(agg, "proj_c", ["a", "total_b"])

            sal.collect(proj_c, 1)

            # return root nodes
            return set([in_1, in_2])

        actual = protocol()
        self.check_workflow(actual, 'agg_proj')

    def test_join(self):

        @scotch
        @mpc
        def protocol():
            cols_in_1 = [
                defCol("a", "INTEGER", [1]),
                defCol("b", "INTEGER", [1]),
            ]
            in_1 = sal.create("in_1", cols_in_1, set([1]))

            cols_in_2 = [
                defCol("c", "INTEGER", [2]),
                defCol("d", "INTEGER", [2])
            ]
            in_2 = sal.create("in_2", cols_in_2, set([2]))
            proj_b = sal.project(in_2, "proj_b", ["c", "d"])

            joined = sal.join(in_1, proj_b, "joined", ["a"], ["c"])
            sal.collect(joined, 1)
            return set([in_1, in_2])

        actual = protocol()
        self.check_workflow(actual, 'join')

    def test_hyrbid_join_opt(self):

        @scotch
        @mpc
        def protocol():
            # define inputs
            cols_in_1 = [
                defCol("a", "INTEGER", [1]),
                defCol("b", "INTEGER", [1]),
            ]
            in_1 = sal.create("in_1", cols_in_1, set([1]))

            cols_in_2 = [
                defCol("c", "INTEGER", [1], [2]),
                defCol("d", "INTEGER", [2])
            ]
            in_2 = sal.create("in_2", cols_in_2, set([2]))

            joined = sal.join(in_1, in_2, "joined", ["a"], ["c"])

            sal.collect(joined, 1)
            # create dag
            return set([in_1, in_2])

        actual = protocol()
        self.check_workflow(actual, 'hybrid_join')

    def test_taxi(self):

        @scotch
        @mpc
        def protocol():
            cols_in_1 = [
                defCol("companyID", "INTEGER", [1]),
                defCol("price", "INTEGER", [1])
            ]
            in_1 = sal.create("in_1", cols_in_1, set([1]))
            cols_in_2 = [
                defCol("companyID", "INTEGER", [2]),
                defCol("price", "INTEGER", [2])
            ]
            in_2 = sal.create("in_2", cols_in_2, set([2]))
            cols_in_3 = [
                defCol("companyID", "INTEGER", [3]),
                defCol("price", "INTEGER", [3])
            ]
            in_3 = sal.create("in_3", cols_in_3, set([3]))

            cab_data = sal.concat([in_1, in_2, in_3], "cab_data")

            selected_input = sal.project(
                cab_data, "selected_input", ["companyID", "price"])
            local_rev = sal.aggregate(selected_input, "local_rev", [
                "companyID"], "price", "+", "local_rev")
            scaled_down = sal.divide(
                local_rev, "scaled_down", "local_rev", ["local_rev", 1000])
            first_val_blank = sal.multiply(
                scaled_down, "first_val_blank", "companyID", ["companyID", 0])
            local_rev_scaled = sal.multiply(
                first_val_blank, "local_rev_scaled", "local_rev", ["local_rev", 100])
            total_rev = sal.aggregate(first_val_blank, "total_rev", [
                "companyID"], "local_rev", "+", "global_rev")
            local_total_rev = sal.join(local_rev_scaled, total_rev, "local_total_rev", [
                "companyID"], ["companyID"])
            market_share = sal.divide(local_total_rev, "market_share", "local_rev", [
                "local_rev", "global_rev"])
            market_share_squared = sal.multiply(market_share, "market_share_squared", "local_rev",
                                                ["local_rev", "local_rev", 1])
            hhi = sal.aggregate(market_share_squared, "hhi", [
                "companyID"], "local_rev", "+", "hhi")

            sal.collect(hhi, 1)

            # return root nodes
            return set([in_1, in_2, in_3])

        actual = protocol()
        self.check_workflow(actual, 'taxi')

    def test_agg_pushdown(self):

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
            in_2 = sal.create("in2", cols_in_2, set([2]))
            cols_in_3 = [
                defCol("a", "INTEGER", [3]),
                defCol("b", "INTEGER", [3])
            ]
            in_3 = sal.create("in_3", cols_in_3, set([3]))

            # combine parties' inputs into one relation
            rel = sal.concat([in_1, in_2, in_3], "rel")
            proj = sal.project(rel, "proj", ["a", "b"])
            agg = sal.aggregate(proj, "agg", ["a"], "b", "+", "total_b")
            div = sal.divide(agg, "div", "a", ["a", 1])
            mult = sal.multiply(div, "mult", "a", ["a", 1])

            sal.collect(mult, 1)

            # return root nodes
            return set([in_1, in_2, in_3])

        actual = protocol()
        self.check_workflow(actual, 'agg_pushdown')

