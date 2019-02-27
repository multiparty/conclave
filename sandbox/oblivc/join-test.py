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
        defCol('b', 'INTEGER', [2]),
    ]

    in1 = sal.create("in1", cols_in_a, {1})
    in2 = sal.create("in2", cols_in_b, {2})

    join1 = sal.join(in1, in2, 'join1', ['a'], ['a'])

    sal.collect(join1, 1)

    return {in1, in2}


if __name__ == "__main__":
    workflow.run(protocol)
