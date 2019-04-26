import conclave.lang as cc
from conclave.utils import defCol
from conclave import workflow


def protocol():

    cols_in_a = [
        defCol('a', 'INTEGER', [1]),
        defCol('b', 'INTEGER', [1]),
        defCol('c', 'INTEGER', [1]),
    ]
    cols_in_b = [
        defCol('a', 'INTEGER', [2]),
        defCol('b', 'INTEGER', [2]),
        defCol('c', 'INTEGER', [2]),
    ]

    in1 = cc.create("in1", cols_in_a, {1})
    in2 = cc.create("in2", cols_in_b, {2})

    cc1 = cc.concat([in1, in2], 'cc1', ['a', 'b', 'c'])

    agg1 = cc.aggregate(cc1, "agg1", ['a'], "b", "sum", "b")

    cc.collect(agg1, 1)

    return {in1, in2}


if __name__ == "__main__":

    workflow.run(protocol, mpc_framework="obliv-c")