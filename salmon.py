import ast

def foo():
    relA = relation('relA', columns(int, int), cgs([1,2,3], [1,2,3]))
    relB = relA.aggregate(0, 1, sum)
    relB.out(1, 2, 3)