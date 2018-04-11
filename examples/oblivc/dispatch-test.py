import conclave.lang as sal
from conclave.utils import *
from conclave import workflow


def protocol():
    """
    Define inputs and operations to be performed between them.
    """

    cols_in_a = [
        defCol('a', 'INTEGER', [1]),
        defCol('b', 'INTEGER', [1]),
    ]
    cols_in_b = [
        defCol('a', 'INTEGER', [2]),
        defCol('c', 'INTEGER', [2]),
    ]

    in1 = sal.create("in1", cols_in_a, {1})
    in1.is_mpc = True

    in2 = sal.create("in2", cols_in_b, {2})
    in2.is_mpc = True

    join1 = sal.join(in1, in2, 'join1', ['a'], ['a'])

    open_join1 = sal._open(join1, 'opened1', 1)

    sal.collect(open_join1, 1)

    return {in1, in2}


if __name__ == "__main__":
    workflow.run(protocol)
