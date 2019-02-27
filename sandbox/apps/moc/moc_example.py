"""
Simple example workflow for MOC deployment of Conclave
"""
import conclave.lang as sal
from conclave.utils import *
from conclave import workflow


def protocol():
    """
    Define inputs and operations to be performed between them.
    """

    # define input columns
    cols_in_a = [
        defCol('a', 'INTEGER', [1]),
        defCol('b', 'INTEGER', [1]),
    ]
    cols_in_b = [
        defCol('a', 'INTEGER', [1]),
        defCol('c', 'INTEGER', [1]),
    ]

    # instantiate input columns
    # NOTE: input file names will correspond to the 0th arg of each create call ("in1", "in2", etc.)
    in1 = sal.create("in1", cols_in_a, {1})
    in2 = sal.create("in2", cols_in_b, {1})

    # operate on columns
    # join in1 & in2 over the column 'a', name output relation 'join1'
    join1 = sal.join(in1, in2, 'join1', ['a'], ['a'])

    # collect leaf node
    out = sal.collect(join1, 2)

    # return root nodes
    return {in1, in2}


if __name__ == "__main__":

    workflow.run(protocol)

