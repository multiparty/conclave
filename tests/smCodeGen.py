import salmon.lang as sal
from salmon.comp import dagonly, sharemind
from salmon.utils import *
from salmon.codegen.sharemind import SharemindCodeGen


def testClose():

    @sharemind
    @dagonly
    def protocol():

        # define inputs
        colsIn1 = [
            defCol("a", "INTEGER", [1]),
            defCol("b", "INTEGER", [1])
        ]
        in1 = sal.create("in1", colsIn1, set([1]))
        out = sal._close(in1, "out", set([1, 2, 3]))

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
            defCol("a", "INTEGER", [1]),
            defCol("b", "INTEGER", [1])
        ]
        in1 = sal.create("in1", colsIn1, set([1]))
        agg = sal.aggregate(in1, "agg", "a", "b", "+", "total_b")

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
            defCol("a", "INTEGER", [1]),
            defCol("b", "INTEGER", [1])
        ]
        in1 = sal.create("in1", colsIn1, set([1]))
        colsIn2 = [
            defCol("a", "INTEGER", [2]),
            defCol("b", "INTEGER", [2])
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
            defCol("a", "INTEGER", [1]),
            defCol("b", "INTEGER", [1])
        ]
        in1 = sal.create("in1", colsIn1, set([1]))
        colsIn2 = [
            defCol("c", "INTEGER", [2]),
            defCol("d", "INTEGER", [2])
        ]
        in2 = sal.create("in2", colsIn2, set([2]))
        joined = sal.join(in1, in2, "joined", "a", "c")

        # return root nodes
        return set([in1, in2])

    actual = protocol()
    print(actual["protocol"])


def testProj():

    @sharemind
    @dagonly
    def protocol():

        colsIn1 = [
            defCol("a", "INTEGER", [1]),
            defCol("b", "INTEGER", [1])
        ]
        in1 = sal.create("in1", colsIn1, set([1]))
        proj = sal.project(in1, "proj", ["a", "b"])

        return set([in1])

    actual = protocol()
    print(actual["protocol"])


def testOpen():

    @sharemind
    @dagonly
    def protocol():

        colsIn1 = [
            defCol("a", "INTEGER", [1]),
            defCol("b", "INTEGER", [1])
        ]
        in1 = sal.create("in1", colsIn1, set([1]))
        opened = sal._open(in1, "opened", 1)

        return set([in1])

    actual = protocol()
    print(actual["protocol"])


def testMult():

    @sharemind
    @dagonly
    def protocol():

        colsIn1 = [
            defCol("a", "INTEGER", [1]),
            defCol("b", "INTEGER", [1])
        ]
        in1 = sal.create("in1", colsIn1, set([1]))
        mult = sal.multiply(in1, "mult", "a", ["a", "b", 1])

        return set([in1])

    actual = protocol()
    print(actual["protocol"])


def testDiv():

    @sharemind
    @dagonly
    def protocol():

        colsIn1 = [
            defCol("a", "INTEGER", [1]),
            defCol("b", "INTEGER", [1])
        ]
        in1 = sal.create("in1", colsIn1, set([1]))
        div = sal.divide(in1, "div", "a", ["a", "b", 10])

        return set([in1])

    actual = protocol()
    print(actual["protocol"])

if __name__ == "__main__":


    testClose()
    # testAgg()
    # testConcatTwo()
    # testConcatMore()
    # testJoin()
    # testProj()
    # testOpen()
    # testMult()
    # testDiv()
