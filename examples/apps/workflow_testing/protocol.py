import conclave.lang as sal
from conclave.utils import *
from conclave import workflow


def protocol():
    cols_in_a = [
        defCol('a', 'INTEGER', [1]),
        defCol('b', 'INTEGER', [1]),
    ]
    cols_in_b = [
        defCol('a', 'INTEGER', [2]),
        defCol('b', 'INTEGER', [2]),
    ]

    in1 = sal.create("in1", cols_in_a, {1})
    in1.is_mpc = True
    in2 = sal.create("in2", cols_in_b, {2})
    in2.is_mpc = True

    rel = sal.concat([in1, in2], "rel")
    mult = sal.multiply(rel, "mult", "a", ["a", 10])

    opened = sal._open(mult, "opened", 1)

    sal.collect(opened, 1)

    # return root nodes
    return {in1, in2}


if __name__ == "__main__":

    workflow.run(protocol)