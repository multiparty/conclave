import salmon.lang as sal
from salmon.comp import dagonly, sharemind
from salmon.utils import *


def testStore():

    @sharemind
    @dagonly
    def protocol():

        # define inputs
        colsIn1 = [
            defCol("INTEGER", [1]),
            defCol("INTEGER", [1])
        ]
        in1 = sal.create("in1", colsIn1, set([1]))
        out = sal._store(in1, "out", set([1, 2, 3]))

        # return root nodes
        return set([in1])

    actual = protocol()
    print(actual["schemas"])
    print(actual["input"])
    print(actual["protocol"])


def testAgg():

    @sharemind
    @dagonly
    def protocol():

        # define inputs
        colsIn1 = [
            defCol("INTEGER", [1]),
            defCol("INTEGER", [1])
        ]
        in1 = sal.create("in1", colsIn1, set([1]))
        agg = sal.aggregate(in1, "agg", "in1_0", "in1_1", "+")

        # return root nodes
        return set([in1])

    actual = protocol()
    print(actual["schemas"])
    print(actual["input"])
    print(actual["protocol"])


def testConcatTwo():

    @sharemind
    @dagonly
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
        rel = sal.concat([in1, in2], "rel")

        # return root nodes
        return set([in1, in2])

    actual = protocol()
    print(actual["schemas"])
    print(actual["input"])
    print(actual["protocol"])


def testConcatMore():

    @sharemind
    @dagonly
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

        rel = sal.concat([in1, in2, in3], "rel")

        # return root nodes
        return set([in1, in2, in3])

    actual = protocol()
    print(actual["protocol"])


def testJoin():

    @sharemind
    @dagonly
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
        joined = sal.join(in1, in2, "joined", "in1_0", "in2_0")

        # return root nodes
        return set([in1, in2])

    actual = protocol()
    print(actual["protocol"])

if __name__ == "__main__":

    testStore()
    testAgg()
    testConcatTwo()
    testConcatMore()
    testJoin()
