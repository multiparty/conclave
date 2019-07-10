import conclave.lang as cc
from conclave import workflow
from conclave.utils import defCol


def protocol():
    left_one_cols = [
        defCol("a", "INTEGER", 1, 2),
        defCol("b", "INTEGER", 1)
    ]
    left_one = cc.create("left_one", left_one_cols, {1})

    right_one_cols = [
        defCol("c", "INTEGER", 1, 2),
        defCol("d", "INTEGER", 1)
    ]
    right_one = cc.create("right_one", right_one_cols, {1})

    left_two_cols = [
        defCol("a", "INTEGER", 1, 2),
        defCol("b", "INTEGER", 2)
    ]
    left_two = cc.create("left_two", left_two_cols, {2})

    right_two_cols = [
        defCol("c", "INTEGER", 1, 2),
        defCol("d", "INTEGER", 2)
    ]
    right_two = cc.create("right_two", right_two_cols, {2})

    left = cc.concat([left_one, left_two], "left")
    right = cc.concat([right_one, right_two], "right")

    joined = cc.join(left, right, "actual", ["a"], ["c"])
    cc.collect(joined, 1)

    return {left_one, left_two, right_one, right_two}


if __name__ == "__main__":

    workflow.run(protocol, mpc_framework="jiff")
