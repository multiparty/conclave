from salmon.utils import *

def testDefCol():

    actual = defCol("a", "INTEGER", [1], [2], [1, 2, 3])
    expected = ('a', 'INTEGER', {frozenset({1, 2, 3}), frozenset({2}), frozenset({1})})

    assert actual == expected, actual

def testMergeCollSets():

    left = set([frozenset([1, 2]), frozenset([3, 4])])
    right = set([frozenset([5, 6]), frozenset([7])])
    
    actual = mergeCollSets(left, right)
    expected = {frozenset({1, 2, 5, 6}), frozenset({1, 2, 7}), frozenset({3, 4, 5, 6}), frozenset({3, 4, 7})}

    assert actual == expected, actual

if __name__ == "__main__":

    testMergeCollSets()
    testDefCol()
    print("All OK")
    