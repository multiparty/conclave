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
        out = sal._store(in1, "out", set([1, 2, 3]))
        agg = sal.aggregate(out, "agg", "out_0", "out_1", "+")

        # return root nodes
        return set([in1])

    actual = protocol()
    print(actual["schemas"])
    print(actual["input"])
    print(actual["protocol"])

if __name__ == "__main__":

    testStore()
    testAgg()
    
