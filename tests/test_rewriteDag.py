from unittest import TestCase

import salmon.lang as sal
from salmon.comp import mpc, scotch
from salmon.utils import *

class TestConclave(TestCase):
    def test_single_concat(self):
        @scotch
        @mpc
        def protocol():

            # define inputs
            colsIn1 = [
                defCol("a", "INTEGER", [1]),
                defCol("b", "INTEGER", [1])
            ]
            in1 = sal.create("in1", colsIn1, set([1]))
            colsIn2 = [
                defCol("a", "INTEGER", [2]),
                defCol("b", "INTEGER", [2])
            ]
            in2 = sal.create("in2", colsIn2, set([2]))
            colsIn3 = [
                defCol("a", "INTEGER", [3]),
                defCol("b", "INTEGER", [3])
            ]
            in3 = sal.create("in3", colsIn3, set([3]))

            # combine parties' inputs into one relation
            rel = sal.concat([in1, in2, in3], "rel")

            sal.collect(rel, 1)

            # return root nodes
            return set([in1, in2, in3])

        expected = """CREATE RELATION in1([a {1}, b {1}]) {1} WITH COLUMNS (INTEGER, INTEGER)
CREATE RELATION in2([a {2}, b {2}]) {2} WITH COLUMNS (INTEGER, INTEGER)
CLOSEMPC in2([a {2}, b {2}]) {2} INTO in2_close([a {2}, b {2}]) {1}
CREATE RELATION in3([a {3}, b {3}]) {3} WITH COLUMNS (INTEGER, INTEGER)
CLOSEMPC in3([a {3}, b {3}]) {3} INTO in3_close([a {3}, b {3}]) {1}
CONCAT [in1([a {1}, b {1}]) {1}, in2_close([a {2}, b {2}]) {1}, in3_close([a {3}, b {3}]) {1}] AS rel([a {1,2,3}, b {1,2,3}]) {1}
"""
        actual = protocol()
        self.assertEqual(expected, actual)
        
    def test_single_aggregate(self):
        @scotch
        @mpc
        def protocol():

            # define inputs
            colsIn1 = [
                defCol("a", "INTEGER", [1]),
                defCol("b", "INTEGER", [1])
            ]
            in1 = sal.create("in1", colsIn1, set([1]))
            colsIn2 = [
                defCol("a", "INTEGER", [2]),
                defCol("b", "INTEGER", [2])
            ]
            in2 = sal.create("in2", colsIn2, set([2]))

            # combine parties' inputs into one relation
            rel = sal.concat([in1, in2], "rel")

            # specify the workflow
            agg = sal.aggregate(rel, "agg", ["a"], "b", "+", "total_b")

            sal.collect(agg, 1)

            # return root nodes
            return set([in1, in2])

        expected = """CREATE RELATION in1([a {1}, b {1}]) {1} WITH COLUMNS (INTEGER, INTEGER)
AGG [b, +] FROM (in1([a {1}, b {1}]) {1}) GROUP BY [a] AS agg_0([a {1}, total_b {1}]) {1}
CLOSEMPC agg_0([a {1}, total_b {1}]) {1} INTO agg_0_close([a {1}, total_b {1}]) {1, 2}
CREATE RELATION in2([a {2}, b {2}]) {2} WITH COLUMNS (INTEGER, INTEGER)
AGG [b, +] FROM (in2([a {2}, b {2}]) {2}) GROUP BY [a] AS agg_1([a {2}, total_b {2}]) {2}
CLOSEMPC agg_1([a {2}, total_b {2}]) {2} INTO agg_1_close([a {2}, total_b {2}]) {1, 2}
CONCATMPC [agg_0_close([a {1}, total_b {1}]) {1, 2}, agg_1_close([a {2}, total_b {2}]) {1, 2}] AS rel([a {1,2}, b {1,2}]) {1, 2}
AGGMPC [b, +] FROM (rel([a {1,2}, b {1,2}]) {1, 2}) GROUP BY [a] AS agg_obl([a {1,2}, total_b {1,2}]) {1, 2}
OPENMPC agg_obl([a {1,2}, total_b {1,2}]) {1, 2} INTO agg_obl_open([a {1,2}, total_b {1,2}]) {1}
"""
        actual = protocol()
        self.assertEqual(expected, actual)
