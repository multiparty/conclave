import salmon.lang as sal
from salmon.comp import mpc, scotch
from salmon.utils import *


def testSingleConcat():

    @scotch
    @mpc(1)
    def protocol():

        # define inputs
        colsIn1 = [
            defCol("INTEGER", [1]),
            defCol("INTEGER", [1])
        ]
        in1 = sal.create("in1", colsIn1, set([1]))
        colsIn2 = [
            defCol("INTEGER", [2]),
            defCol("INTEGER", [2])
        ]
        in2 = sal.create("in2", colsIn2, set([2]))
        colsIn3 = [
            defCol("INTEGER", [3]),
            defCol("INTEGER", [3])
        ]
        in3 = sal.create("in3", colsIn3, set([3]))

        # combine parties' inputs into one relation
        rel = sal.concat([in1, in2, in3], "rel")

        sal.collect(rel, 1)

        # return root nodes
        return set([in1, in2, in3])

    expected = """CREATE RELATION in1([in1_0 {1}, in1_1 {1}]) {1} WITH COLUMNS (INTEGER, INTEGER)
STORE in2([in2_0 {2}, in2_1 {2}]) {2} INTO in2_store([in2_store_0 {2}, in2_store_1 {2}]) {1}
STORE in3([in3_0 {3}, in3_1 {3}]) {3} INTO in3_store([in3_store_0 {3}, in3_store_1 {3}]) {1}
CONCAT [in1([in1_0 {1}, in1_1 {1}]) {1}, in2_store([in2_store_0 {2}, in2_store_1 {2}]) {1}, in3_store([in3_store_0 {3}, in3_store_1 {3}]) {1}] AS rel([rel_0 {1,2,3}, rel_1 {1,2,3}]) {1}
"""
    actual = protocol()
    assert expected == actual, actual

if __name__ == "__main__":

    testSingleConcat()
    