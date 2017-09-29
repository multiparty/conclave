import salmon.lang as sal
from salmon.comp import dagonly, scotch
from salmon.utils import *


def testHybridJoinWorkflow():

    @scotch
    @dagonly
    def protocol():
        # define inputs
        colsInA = [
            defCol("a", "INTEGER", [1]),
            defCol("b", "INTEGER", [1]),
        ]
        inA = sal.create("inA", colsInA, set([1]))

        colsInB = [
            defCol("c", "INTEGER", [1], [2]),
            defCol("d", "INTEGER", [2])
        ]
        inB = sal.create("inB", colsInB, set([2]))

        clA = sal._close(inA, "clA", set([1, 2, 3]))
        clA.isMPC = True
        clB = sal._close(inB, "clB", set([1, 2, 3]))
        clB.isMPC = True

        shuffledA = sal.shuffle(clA, "shuffledA")
        shuffledA.isMPC = True
        persistedA = sal._persist(shuffledA, "persistedA")
        persistedA.isMPC = True
        shuffledB = sal.shuffle(clB, "shuffledB")
        shuffledB.isMPC = True
        persistedB = sal._persist(shuffledB, "persistedB")

        keysAclosed = sal.project(shuffledA, "keysAclosed", ["a"])
        keysAclosed.isMPC = True
        keysBclosed = sal.project(shuffledB, "keysBclosed", ["c"])
        keysBclosed.isMPC = True

        keysA = sal._open(keysAclosed, "keysA", 1)
        keysB = sal._open(keysBclosed, "keysB", 1)

        indexedA = sal.index(keysA, "indexedA", "indexA")
        indexedB = sal.index(keysB, "indexedB", "indexB")

        joinedindeces = sal.join(
            indexedA, indexedB, "joinedindeces", ["a"], ["c"])
        indecesonly = sal.project(
            joinedindeces, "indecesonly", ["indexA", "indexB"])

        indecesclosed = sal._close(
            indecesonly, "indecesclosed", set([1, 2, 3]))
        indecesclosed.isMPC = True

        joined = sal._index_join(persistedA, persistedB, "joined", [
                                 "a"], ["c"], indecesclosed)

        # create dag
        return set([inA, inB])

    actual = protocol()
    print(actual)

if __name__ == "__main__":

    testHybridJoinWorkflow()
