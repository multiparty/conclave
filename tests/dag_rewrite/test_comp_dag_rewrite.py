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
        self.maxDiff = None
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
            agg = cc.aggregate(proj, "agg", ["a"], "b", "sum", "total_b")

            cc.collect(agg, 1)

            # return root nodes
            return {in_1, in_2, in_3}

        actual = protocol()
        self.check_workflow(actual, 'concat_pushdown')

    def test_pushdown_into_filter(self):
        @scotch
        @mpc
        def protocol():
            # define inputs
            cols_in_1 = [
                defCol("a", "INTEGER", [1]),
                defCol("b", "INTEGER", [1]),
                defCol("c", "INTEGER", [1])
            ]
            in_1 = cc.create("in_1", cols_in_1, {1})
            cols_in_2 = [
                defCol("a", "INTEGER", [2]),
                defCol("b", "INTEGER", [2]),
                defCol("c", "INTEGER", [2])
            ]
            in_2 = cc.create("in_2", cols_in_2, {2})

            # combine parties' inputs into one relation
            rel = cc.concat([in_1, in_2], "rel")

            projected = cc.project(rel, "projected", ["c", "b"])

            # specify the workflow
            filtered = cc.cc_filter(projected, "filtered", "c", "==", other_col_name="b")

            cc.collect(filtered, 1)

            # return root nodes
            return {in_1, in_2}

        actual = protocol()
        self.check_workflow(actual, "pushdown_into_filter")

    def test_concat_pushdown_proj_change_col_num(self):
        @scotch
        @mpc
        def protocol():
            # define inputs
            cols_in_1 = [
                defCol("a", "INTEGER", [1]),
                defCol("b", "INTEGER", [1]),
                defCol("c", "INTEGER", [1])
            ]
            in_1 = cc.create("in_1", cols_in_1, {1})
            cols_in_2 = [
                defCol("a", "INTEGER", [2]),
                defCol("b", "INTEGER", [2]),
                defCol("c", "INTEGER", [2])
            ]
            in_2 = cc.create("in_2", cols_in_2, {2})
            cols_in_3 = [
                defCol("a", "INTEGER", [3]),
                defCol("b", "INTEGER", [3]),
                defCol("c", "INTEGER", [3])
            ]
            in_3 = cc.create("in_3", cols_in_3, {3})

            # combine parties' inputs into one relation
            rel = cc.concat([in_1, in_2, in_3], "rel")
            proj = cc.project(rel, "proj", ["a", "b"])
            agg = cc.aggregate(proj, "agg", ["a"], "b", "sum", "total_b")

            cc.collect(agg, 1)

            # return root nodes
            return {in_1, in_2, in_3}

        actual = protocol()
        self.check_workflow(actual, 'concat_pushdown_proj_change_col_num')

    def test_concat_pushdown_rearrange_cols(self):
        @scotch
        @mpc
        def protocol():
            # define inputs
            cols_in_1 = [
                defCol("a", "INTEGER", [1]),
                defCol("b", "INTEGER", [1]),
                defCol("c", "INTEGER", [1])
            ]
            in_1 = cc.create("in_1", cols_in_1, {1})
            cols_in_2 = [
                defCol("a", "INTEGER", [2]),
                defCol("b", "INTEGER", [2]),
                defCol("c", "INTEGER", [2])
            ]
            in_2 = cc.create("in_2", cols_in_2, {2})
            cols_in_3 = [
                defCol("a", "INTEGER", [3]),
                defCol("b", "INTEGER", [3]),
                defCol("c", "INTEGER", [3])
            ]
            in_3 = cc.create("in_3", cols_in_3, {3})

            # combine parties' inputs into one relation
            rel = cc.concat([in_1, in_2, in_3], "rel")
            proj = cc.project(rel, "proj", ["c", "a"])
            agg = cc.aggregate(proj, "agg", ["c"], "a", "sum", "total_b")

            cc.collect(agg, 1)

            # return root nodes
            return {in_1, in_2, in_3}

        actual = protocol()
        self.check_workflow(actual, 'concat_pushdown_rearrange_cols')

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
            agg = cc.aggregate(proj_b, "agg", ["a"], "b", "sum", "total_b")
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
            cc.collect(cc.aggregate(cc.concat([in_1, in_2], "rel"), "agg", ["a"], "b", "sum", "total_b"), 1)
            return {in_1, in_2}

        dag = rewrite_dag(ccdag.OpDag(protocol()), use_leaky_ops=True)
        actual = ScotchCodeGen(CodeGenConfig(), dag)._generate(0, 0)
        self.check_workflow(actual, "hybrid_agg_leaky")

    def test_hybrid_join_opt(self):
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

        dag = rewrite_dag(ccdag.OpDag(protocol()), use_leaky_ops=True)
        actual = ScotchCodeGen(CodeGenConfig(), dag)._generate(0, 0)
        self.check_workflow(actual, 'hybrid_join_leaky')

    def test_hybrid_join_party_two_opt(self):
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

        dag = rewrite_dag(ccdag.OpDag(protocol()), use_leaky_ops=True)
        actual = ScotchCodeGen(CodeGenConfig(), dag)._generate(0, 0)
        self.check_workflow(actual, 'hybrid_join_leaky_party_two')

    def test_public_join(self):
        def protocol():
            left_one_cols = [
                defCol("a", "INTEGER", 1, 2, 3),
                defCol("b", "INTEGER", 1)
            ]
            left_one = cc.create("left_one", left_one_cols, {1})

            right_one_cols = [
                defCol("c", "INTEGER", 1, 2, 3),
                defCol("d", "INTEGER", 1)
            ]
            right_one = cc.create("right_one", right_one_cols, {1})

            left_two_cols = [
                defCol("a", "INTEGER", 1, 2, 3),
                defCol("b", "INTEGER", 2)
            ]
            left_two = cc.create("left_two", left_two_cols, {2})

            right_two_cols = [
                defCol("c", "INTEGER", 1, 2, 3),
                defCol("d", "INTEGER", 2)
            ]
            right_two = cc.create("right_two", right_two_cols, {2})

            left = cc.concat([left_one, left_two], "left")
            right = cc.concat([right_one, right_two], "right")

            joined = cc.join(left, right, "joined", ["a"], ["c"])
            cc.collect(joined, 1)

            return {left_one, left_two, right_one, right_two}

        dag = rewrite_dag(ccdag.OpDag(protocol()))
        actual = ScotchCodeGen(CodeGenConfig(), dag)._generate(0, 0)
        self.check_workflow(actual, 'public_join')

    def test_ssn(self):
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
            res = cc.aggregate(joined, "actual", ["b"], "d", "sum", "total")
            cc.collect(res, 1)

            return {govreg, company0, company1}

        dag = rewrite_dag(ccdag.OpDag(protocol()), use_leaky_ops=True)
        actual = ScotchCodeGen(CodeGenConfig(), dag)._generate(0, 0)
        print(actual)
        self.check_workflow(actual, "ssn_leaky")

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
            agg = cc.aggregate(proj, "agg", ["a"], "b", "sum", "total_b")
            div = cc.divide(agg, "div", "a", ["a", 1])
            mult = cc.multiply(div, "mult", "a", ["a", 1])

            cc.collect(mult, 1)

            # return root nodes
            return {in_1, in_2, in_3}

        actual = protocol()
        self.check_workflow(actual, 'agg_pushdown')

    def test_aspirin_no_slicing(self):
        @scotch
        @mpc
        def protocol():
            pid_col_meds = "0"
            med_col_meds = "4"
            date_col_meds = "7"

            pid_col_diags = "8"
            diag_col_diags = "16"
            date_col_diags = "18"

            num_med_cols = 8
            num_diag_cols = 13

            left_medication_cols = [defCol(str(i), "INTEGER", 1) for i in range(num_med_cols)]
            # public PID column
            left_medication_cols[0] = defCol(pid_col_meds, "INTEGER", 1, 2, 3)
            left_medication = cc.create("left_medication", left_medication_cols, {1})

            left_diagnosis_cols = [defCol(str(i + num_med_cols), "INTEGER", 1) for i in range(num_diag_cols)]
            # public PID column
            left_diagnosis_cols[0] = defCol(pid_col_diags, "INTEGER", 1, 2, 3)
            left_diagnosis = cc.create("left_diagnosis", left_diagnosis_cols, {1})

            right_medication_cols = [defCol(str(i), "INTEGER", 2) for i in range(num_med_cols)]
            # public PID column
            right_medication_cols[0] = defCol(pid_col_meds, "INTEGER", 1, 2, 3)
            right_medication = cc.create("right_medication", right_medication_cols, {2})

            right_diagnosis_cols = [defCol(str(i + num_med_cols), "INTEGER", 2) for i in range(num_diag_cols)]
            # public PID column
            right_diagnosis_cols[0] = defCol(pid_col_diags, "INTEGER", 1, 2, 3)
            right_diagnosis = cc.create("right_diagnosis", right_diagnosis_cols, {2})

            medication = cc.concat([left_medication, right_medication], "medication")
            diagnosis = cc.concat([left_diagnosis, right_diagnosis], "diagnosis")

            # only keep relevant columns
            medication_proj = cc.project(medication, "medication_proj", [pid_col_meds, med_col_meds, date_col_meds])
            diagnosis_proj = cc.project(diagnosis, "diagnosis_proj", [pid_col_diags, diag_col_diags, date_col_diags])

            joined = cc.join(medication_proj, diagnosis_proj, "joined", [pid_col_meds], [pid_col_diags])

            cases = cc.cc_filter(joined, "cases", date_col_diags, "<", other_col_name=date_col_meds)
            aspirin = cc.cc_filter(cases, "aspirin", med_col_meds, "==", scalar=1)
            heart_patients = cc.cc_filter(aspirin, "heart_patients", diag_col_diags, "==", scalar=1)

            cc.collect(cc.distinct_count(heart_patients, "actual", pid_col_meds, use_sort=False), 1)

            return {left_medication, left_diagnosis, right_medication, right_diagnosis}

        actual = protocol()
        print(actual)
        self.check_workflow(actual, "aspirin_no_slicing")

    def test_comorb_full(self):
        @scotch
        @mpc
        def protocol():
            pid_col = "8"
            diagnosis_col = "16"

            cols_to_skip = 8
            num_diagnosis_cols = 13

            left_diagnosis_cols = [defCol(str(i + cols_to_skip), "INTEGER", 1) for i in range(num_diagnosis_cols)]
            left_diagnosis = cc.create("left_diagnosis", left_diagnosis_cols, {1})

            left_cohort = cc.create("left_cohort", [defCol("pid", "Integer", 1)], {1})

            left_selected = cc.filter_by(left_diagnosis, "left_selected", pid_col, left_cohort)

            right_diagnosis_cols = [defCol(str(i + cols_to_skip), "INTEGER", 2) for i in range(num_diagnosis_cols)]
            right_diagnosis = cc.create("right_diagnosis", right_diagnosis_cols, {2})

            right_cohort = cc.create("right_cohort", [defCol("pid", "Integer", 2)], {2})

            right_selected = cc.filter_by(right_diagnosis, "right_selected", pid_col, right_cohort)

            cohort = cc.concat([left_selected, right_selected], "cohort")
            counts = cc.aggregate_count(cohort, "counts", [diagnosis_col], "total")
            cc.collect(cc.sort_by(counts, "actual", "total"), 1)

            return {left_diagnosis, left_cohort, right_diagnosis, right_cohort}

        actual = protocol()
        self.check_workflow(actual, "comorb_full")

    def test_comorb(self):
        @scotch
        @mpc
        def protocol():
            diagnosis_col = "12"
            num_diagnosis_cols = 13

            left_diagnosis_cols = [defCol(str(i), "INTEGER", 1) for i in range(num_diagnosis_cols)]
            left_diagnosis = cc.create("left_diagnosis", left_diagnosis_cols, {1})

            right_diagnosis_cols = [defCol(str(i), "INTEGER", 2) for i in range(num_diagnosis_cols)]
            right_diagnosis = cc.create("right_diagnosis", right_diagnosis_cols, {2})

            cohort = cc.concat([left_diagnosis, right_diagnosis], "cohort")
            counts = cc.aggregate_count(cohort, "counts", [diagnosis_col], "total")
            cc.collect(cc.sort_by(counts, "actual", "total"), 1)

            return {left_diagnosis, right_diagnosis}

        actual = protocol()
        self.check_workflow(actual, "comorb")
