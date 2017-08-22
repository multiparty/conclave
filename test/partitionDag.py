import salmon.lang as sal
from salmon.comp import dagonly
from salmon.utils import *
import salmon.partition as part


def testPartition():

    @dagonly
    def protocol():

        colsIn1 = [
            defCol("a", "INTEGER", [1]),
            defCol("b", "INTEGER", [1])
        ]

        colsIn2 = [
            defCol("a", "INTEGER", [2]),
            defCol("b", "INTEGER", [2])
        ]

        in1 = sal.create("in1", colsIn1, set([1]))
        in2 = sal.create("in2", colsIn2, set([2]))

        mult1 = sal.multiply(in1, "mult1", "c", ["a", "b"])
        mult2 = sal.multiply(in2, "mult2", "d", ["a", "b"])

        join1 = sal.join(mult1, mult2, 'join1', 'a', 'a')
        proj1 = sal.project(join1, "projA", ["b"])

        sal.collect(proj1, 1)

        return set([in1, in2])

    dag = protocol()
    part_dag = part.partDag(dag)

    for job in part_dag:
        print(job[0].name)
        print(job[1])

if __name__ == "__main__":

    testPartition()