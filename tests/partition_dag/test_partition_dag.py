import os
from unittest import TestCase

import conclave.dag as ccdag
import conclave.lang as cc
import conclave.partition as part
from conclave.comp import mpc, rewrite_dag
from conclave.utils import *


class TestConclave(TestCase):

    def check_workflow(self, dag, name):
        mapping = part.heupart(dag, ["sharemind"], ["spark"])
        actual = "###".join([fmwk + str(subdag) + str(parties)
                             for (fmwk, subdag, parties) in mapping])
        print(actual)
        expected_rootdir = "{}/part_expected".format(os.path.dirname(os.path.realpath(__file__)))

        with open(expected_rootdir + '/{}'.format(name), 'r') as f:
            expected = f.read()

        self.assertEqual(expected, actual)

    def test_partition_taxi(self):
        @mpc(1)
        def protocol():
            cols_in_1 = [
                defCol("companyID", "INTEGER", [1]),
                defCol("price", "INTEGER", [1])
            ]
            in_1 = cc.create("yellow1", cols_in_1, {1})
            cols_in_2 = [
                defCol("companyID", "INTEGER", [2]),
                defCol("price", "INTEGER", [2])
            ]
            in_2 = cc.create("yellow2", cols_in_2, {2})
            cols_in_3 = [
                defCol("companyID", "INTEGER", [3]),
                defCol("price", "INTEGER", [3])
            ]
            in_3 = cc.create("yellow3", cols_in_3, {3})

            cab_data = cc.concat([in_1, in_2, in_3], "cab_data")

            selected_input = cc.project(
                cab_data, "selected_input", ["companyID", "price"])
            local_rev = cc.aggregate(selected_input, "local_rev", [
                "companyID"], "price", "sum", "local_rev")
            scaled_down = cc.divide(
                local_rev, "scaled_down", "local_rev", ["local_rev", 1000])
            first_val_blank = cc.multiply(
                scaled_down, "first_val_blank", "companyID", ["companyID", 0])
            local_rev_scaled = cc.multiply(
                first_val_blank, "local_rev_scaled", "local_rev", ["local_rev", 100])
            total_rev = cc.aggregate(first_val_blank, "total_rev", [
                "companyID"], "local_rev", "sum", "global_rev")
            local_total_rev = cc.join(local_rev_scaled, total_rev, "local_total_rev", [
                "companyID"], ["companyID"])
            market_share = cc.divide(local_total_rev, "market_share", "local_rev", [
                "local_rev", "global_rev"])
            market_share_squared = cc.multiply(market_share, "market_share_squared", "local_rev",
                                               ["local_rev", "local_rev", 1])
            hhi = cc.aggregate(market_share_squared, "hhi", [
                "companyID"], "local_rev", "sum", "hhi")
            # dummy projection to force non-mpc subdag
            hhi_only = cc.project(
                hhi, "hhi_only", ["companyID", "hhi"])

            cc.collect(hhi_only, 1)

            # return root nodes
            return {in_1, in_2, in_3}

        dag = protocol()
        self.check_workflow(dag, 'taxi')

    def test_partition_ssn(self):
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
            actual = cc.aggregate(joined, "actual", ["b"], "d", "sum", "total")
            cc.collect(actual, 1)

            return {govreg, company0, company1}

        dag = rewrite_dag(ccdag.OpDag(protocol()), use_leaky_ops=True)
        self.check_workflow(dag, "ssn")

    def test_inputs_out_of_order(self):
        def protocol():
            # define inputs
            cols_in_a = [
                defCol("a", "INTEGER", [1]),
                defCol("b", "INTEGER", [1]),
            ]
            in_1 = cc.create("in_1", cols_in_a, {1})
            in_1.is_mpc = False

            proj_a = cc.project(in_1, "proj_a", ["a", "b"])
            proj_a.is_mpc = False
            proj_a.out_rel.stored_with = {1}

            cols_in_b = [
                defCol("c", "INTEGER", [1], [2]),
                defCol("d", "INTEGER", [2])
            ]
            in_2 = cc.create("in_2", cols_in_b, {2})
            in_2.is_mpc = False

            proj_b = cc.project(in_2, "proj_b", ["c", "d"])
            proj_b.is_mpc = False
            proj_b.out_rel.stored_with = {2}

            cols_in_c = [
                defCol("c", "INTEGER", [1], [3]),
                defCol("d", "INTEGER", [3])
            ]
            in_3 = cc.create("beforeOthers", cols_in_c, {1, 2, 3})
            in_3.is_mpc = True

            cl_a = cc._close(proj_a, "cl_a", {1, 2, 3})
            cl_a.is_mpc = True
            cl_b = cc._close(proj_b, "cl_b", {1, 2, 3})
            cl_b.is_mpc = True
            cl_c = cc._close(in_3, "cl_c", {1, 2, 3})
            cl_c.is_mpc = True

            right_closed = cc.concat([cl_a, cl_b, cl_c], "a")
            right_closed.is_mpc = True
            right_closed.out_rel.stored_with = {1, 2, 3}

            shuffled_a = cc.shuffle(cl_a, "shuffled_a")
            shuffled_a.is_mpc = True
            cc._open(shuffled_a, "ssn_opened", 1)

            return ccdag.OpDag({in_1, in_2, in_3})

        dag = protocol()
        self.check_workflow(dag, 'out_of_order')

    def test_partition_hybrid_join(self):
        def protocol():
            cols_in_a = [
                defCol("a", "INTEGER", [1]),
                defCol("b", "INTEGER", [1]),
            ]
            in_a = cc.create("in_a", cols_in_a, {1})
            proj_a = cc.project(in_a, "proj_a", ["a", "b"])

            cols_in_b = [
                defCol("c", "INTEGER", [1], [2]),
                defCol("d", "INTEGER", [2])
            ]
            in_b = cc.create("in_b", cols_in_b, {2})
            proj_b = cc.project(in_b, "proj_b", ["c", "d"])

            joined = cc.join(proj_a, proj_b, "joined", ["a"], ["c"])
            cc.collect(joined, 1)

            return {in_a, in_b}

        dag = rewrite_dag(ccdag.OpDag(protocol()), use_leaky_ops=True)
        self.check_workflow(dag, 'hybrid_join')
