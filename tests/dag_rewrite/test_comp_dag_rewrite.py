import os
from unittest import TestCase

import conclave.dag as ccdag
import conclave.lang as cc
from conclave import CodeGenConfig
from conclave.codegen.scotch import ScotchCodeGen
from conclave.comp import mpc, scotch, rewrite_dag
from conclave.utils import *


class TestConclave(TestCase):

    def check_workflow(self, code, name):
        expected_rootdir = "{}/rewrite_expected".format(os.path.dirname(os.path.realpath(__file__)))

        with open(expected_rootdir + '/{}'.format(name), 'r') as f:
            expected = f.read()

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
            in_1 = cc.create("in1", cols_in_1, {1})
            cols_in_2 = [
                defCol("a", "INTEGER", [2]),
                defCol("b", "INTEGER", [2])
            ]
            in_2 = cc.create("in_2", cols_in_2, {2})

            # combine parties' inputs into one relation
            rel = cc.concat([in_1, in_2], "rel")

            # specify the workflow
            mult = cc.multiply(rel, "mult", "a", ["a", 0])

            cc.collect(mult, 1)

            # return root nodes
            return {in_1, in_2}

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
            proj = cc.project(rel, "proj", ["a", "b"])
            agg = cc.aggregate(proj, "agg", ["a"], "b", "+", "total_b")

            cc.collect(agg, 1)

            # return root nodes
            return {in_1, in_2, in_3}

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
            in_1 = cc.create("in_1", cols_in_1, {1})
            cols_in_2 = [
                defCol("a", "INTEGER", [2]),
                defCol("b", "INTEGER", [2])
            ]
            in_2 = cc.create("in_2", cols_in_2, {2})

            # combine parties' inputs into one relation
            rel = cc.concat([in_1, in_2], "rel")

            # specify the workflow
            proj_a = cc.project(rel, "proj_a", ["a", "b"])
            proj_b = cc.project(proj_a, "proj_b", ["a", "b"])
            agg = cc.aggregate(proj_b, "agg", ["a"], "b", "+", "total_b")
            proj_c = cc.project(agg, "proj_c", ["a", "total_b"])

            cc.collect(proj_c, 1)

            # return root nodes
            return {in_1, in_2}

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
            in_1 = cc.create("in_1", cols_in_1, {1})

            cols_in_2 = [
                defCol("c", "INTEGER", [2]),
                defCol("d", "INTEGER", [2])
            ]
            in_2 = cc.create("in_2", cols_in_2, {2})
            proj_b = cc.project(in_2, "proj_b", ["c", "d"])

            joined = cc.join(in_1, proj_b, "joined", ["a"], ["c"])
            cc.collect(joined, 1)
            return {in_1, in_2}

        actual = protocol()
        self.check_workflow(actual, 'join')

    def test_hybrid_agg_opt(self):
        @scotch
        @mpc
        def protocol():
            cols_in_1 = [
                defCol("a", "INTEGER", [1]),
                defCol("b", "INTEGER", [1])
            ]
            in_1 = cc.create("in_1", cols_in_1, {1})
            cols_in_2 = [
                defCol("a", "INTEGER", [1], [2]),
                defCol("b", "INTEGER", [2])
            ]
            in_2 = cc.create("in_2", cols_in_2, {2})
            cc.collect(cc.aggregate(cc.concat([in_1, in_2], "rel"), "agg", ["a"], "b", "+", "total_b"), 1)
            return {in_1, in_2}

        actual = protocol()
        self.check_workflow(actual, 'hybrid_agg')

    def test_hybrid_join_opt(self):
        @scotch
        @mpc
        def protocol():
            # define inputs
            cols_in_1 = [
                defCol("a", "INTEGER", [1]),
                defCol("b", "INTEGER", [1]),
            ]
            in_1 = cc.create("in_1", cols_in_1, {1})

            cols_in_2 = [
                defCol("c", "INTEGER", [1], [2]),
                defCol("d", "INTEGER", [2])
            ]
            in_2 = cc.create("in_2", cols_in_2, {2})

            result = cc.join(in_1, in_2, "result", ["a"], ["c"])

            cc.collect(result, 1)
            # create dag
            return {in_1, in_2}

        actual = protocol()
        self.check_workflow(actual, 'hybrid_join')

    def test_hybrid_join_opt_non_leaky(self):
        def protocol():
            # define inputs
            cols_in_1 = [
                defCol("a", "INTEGER", [1]),
                defCol("b", "INTEGER", [1]),
            ]
            in_1 = cc.create("in_1", cols_in_1, {1})

            cols_in_2 = [
                defCol("c", "INTEGER", [1], [2]),
                defCol("d", "INTEGER", [2])
            ]
            in_2 = cc.create("in_2", cols_in_2, {2})

            result = cc.join(in_1, in_2, "result", ["a"], ["c"])

            cc.collect(result, 1)
            # create dag
            return {in_1, in_2}

        dag = rewrite_dag(ccdag.OpDag(protocol()), use_leaky_ops=False)
        actual = ScotchCodeGen(CodeGenConfig(), dag)._generate(0, 0)

        self.check_workflow(actual, 'hybrid_join_non_leaky')

    def test_hybrid_join_opt_party_two(self):
        @scotch
        @mpc
        def protocol():
            # define inputs
            cols_in_1 = [
                defCol("a", "INTEGER", [1], [2]),
                defCol("b", "INTEGER", [1]),
            ]
            in_1 = cc.create("in_1", cols_in_1, {1})

            cols_in_2 = [
                defCol("c", "INTEGER", [2]),
                defCol("d", "INTEGER", [2])
            ]
            in_2 = cc.create("in_2", cols_in_2, {2})

            result = cc.join(in_1, in_2, "result", ["a"], ["c"])

            cc.collect(result, 1)
            # create dag
            return {in_1, in_2}

        actual = protocol()
        self.check_workflow(actual, 'hybrid_join_party_two')

    def test_ssn(self):
        @scotch
        @mpc
        def protocol():
            govreg_cols = [
                defCol("a", "INTEGER", [1]),
                defCol("b", "INTEGER", [1])
            ]
            govreg = cc.create("a_govreg", govreg_cols, {1})
            govreg_dummy = cc.project(govreg, "govreg_dummy", ["a", "b"])

            company0_cols = [
                defCol("c", "INTEGER", [1], [2]),
                defCol("d", "INTEGER", [2])
            ]
            company0 = cc.create("company0", company0_cols, {2})
            company0_dummy = cc.project(company0, "company0_dummy", ["c", "d"])

            company1_cols = [
                defCol("c", "INTEGER", [1], [3]),
                defCol("d", "INTEGER", [3])
            ]
            company1 = cc.create("company1", company1_cols, {3})
            company1_dummy = cc.project(company1, "company1_dummy", ["c", "d"])

            companies = cc.concat([company0_dummy, company1_dummy], "companies")

            joined = cc.join(govreg_dummy, companies, "joined", ["a"], ["c"])
            actual = cc.aggregate(joined, "actual", ["b"], "d", "+", "total")
            cc.collect(actual, 1)

            return {govreg, company0, company1}

        actual = protocol()
        self.check_workflow(actual, 'ssn')

    def test_taxi(self):
        @scotch
        @mpc
        def protocol():
            cols_in_1 = [
                defCol("companyID", "INTEGER", [1]),
                defCol("price", "INTEGER", [1])
            ]
            in_1 = cc.create("in_1", cols_in_1, {1})
            cols_in_2 = [
                defCol("companyID", "INTEGER", [2]),
                defCol("price", "INTEGER", [2])
            ]
            in_2 = cc.create("in_2", cols_in_2, {2})
            cols_in_3 = [
                defCol("companyID", "INTEGER", [3]),
                defCol("price", "INTEGER", [3])
            ]
            in_3 = cc.create("in_3", cols_in_3, {3})

            cab_data = cc.concat([in_1, in_2, in_3], "cab_data")

            selected_input = cc.project(
                cab_data, "selected_input", ["companyID", "price"])
            local_rev = cc.aggregate(selected_input, "local_rev", [
                "companyID"], "price", "+", "local_rev")
            scaled_down = cc.divide(
                local_rev, "scaled_down", "local_rev", ["local_rev", 1000])
            first_val_blank = cc.multiply(
                scaled_down, "first_val_blank", "companyID", ["companyID", 0])
            local_rev_scaled = cc.multiply(
                first_val_blank, "local_rev_scaled", "local_rev", ["local_rev", 100])
            total_rev = cc.aggregate(first_val_blank, "total_rev", [
                "companyID"], "local_rev", "+", "global_rev")
            local_total_rev = cc.join(local_rev_scaled, total_rev, "local_total_rev", [
                "companyID"], ["companyID"])
            market_share = cc.divide(local_total_rev, "market_share", "local_rev", [
                "local_rev", "global_rev"])
            market_share_squared = cc.multiply(market_share, "market_share_squared", "local_rev",
                                               ["local_rev", "local_rev", 1])
            hhi = cc.aggregate(market_share_squared, "hhi", [
                "companyID"], "local_rev", "+", "hhi")

            cc.collect(hhi, 1)

            # return root nodes
            return {in_1, in_2, in_3}

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
            in_1 = cc.create("in_1", cols_in_1, {1})
            cols_in_2 = [
                defCol("a", "INTEGER", [2]),
                defCol("b", "INTEGER", [2])
            ]
            in_2 = cc.create("in2", cols_in_2, {2})
            cols_in_3 = [
                defCol("a", "INTEGER", [3]),
                defCol("b", "INTEGER", [3])
            ]
            in_3 = cc.create("in_3", cols_in_3, {3})

            # combine parties' inputs into one relation
            rel = cc.concat([in_1, in_2, in_3], "rel")
            proj = cc.project(rel, "proj", ["a", "b"])
            agg = cc.aggregate(proj, "agg", ["a"], "b", "+", "total_b")
            div = cc.divide(agg, "div", "a", ["a", 1])
            mult = cc.multiply(div, "mult", "a", ["a", 1])

            cc.collect(mult, 1)

            # return root nodes
            return {in_1, in_2, in_3}

        actual = protocol()
        self.check_workflow(actual, 'agg_pushdown')
