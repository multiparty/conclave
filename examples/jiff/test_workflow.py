import conclave.lang as cc
from conclave.utils import defCol
from conclave import workflow


def protocol():
    """
    Define inputs and operations to be performed between them.
    """

    """
    Define input datasets
    """
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
    cols_in_c = [
        defCol('a', 'INTEGER', [3]),
        defCol('b', 'INTEGER', [3]),
        defCol('c', 'INTEGER', [3]),
    ]

    """
    Create input relations.
    """
    in1 = cc.create("in1", cols_in_a, {1})
    in2 = cc.create("in2", cols_in_b, {2})
    in3 = cc.create("in3", cols_in_c, {3})

    cc1 = cc.concat([in1, in2, in3], 'cc1', ['a', 'b', 'c'])

    agg1 = cc.aggregate(cc1, "agg1", ['a'], "b", "mean", "b")

    cc.collect(agg1, 1)

    return {in1, in2, in3}


if __name__ == "__main__":

    workflow.run(protocol, mpc_framework="jiff")