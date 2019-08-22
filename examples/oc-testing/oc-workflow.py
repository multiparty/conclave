import conclave.lang as cc
from conclave import workflow
from conclave.utils import defCol


def protocol():

    party_one = [
        defCol("a", "INTEGER", 1),
        defCol("b", "INTEGER", 1)
    ]

    party_two = [
        defCol("c", "INTEGER", 2),
        defCol("d", "INTEGER", 2)
    ]

    one = cc.create("one", party_one, {1})
    two = cc.create("two", party_two, {2})

    rel = cc.concat([one, two], "rel")
    agg = cc.aggregate(rel, 'agg1', ['a'], 'b', 'sum', 'c_agg')

    cc.collect(agg, 1)

    return {one, two}


if __name__ == "__main__":

    workflow.run(protocol, mpc_framework="obliv-c")

