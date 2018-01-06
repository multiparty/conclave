import salmon.lang as sal
from salmon.codegen.spark import SparkCodeGen
from salmon import CodeGenConfig
from salmon.utils import *
from salmon.comp import dagonly

def setup():

    colsIn1 = [
        defCol("a", "INTEGER", [1]),
        defCol("b", "INTEGER", [1]),
        defCol("c", "INTEGER", [1]),
        defCol("d", "INTEGER", [1])
    ]

    in1 = sal.create("in1", colsIn1, set([1]))

    in2 = sal.create("in2", colsIn1, set([1]))

    return [in1, in2]


@dagonly
def agg():

    in1 = setup()[0]

    agg = sal.aggregate(in1, "agg", ["a", "b"], "c", "sum", "agg1")

    out = sal.collect(agg, 1)

    return set([in1])


@dagonly
def concat():

    inpts = setup()
    in1, in2 = inpts[0], inpts[1]

    cc = sal.concat([in1, in2], "cc")

    out = sal.collect(cc, 1)

    return set([in1, in2])


@dagonly
def distinct():

    in1 = setup()[0]

    # TODO: (ben) codegen currently assumes all cols for distinct operator
    dist = sal.distinct(in1, "dist", ["a", "b", "c", "d"])

    return set([in1])


@dagonly
def divide():

    in1 = setup()[0]

    div = sal.divide(in1, "div", "d", ["a", "b"])

    out = sal.collect(div, 1)

    return set([in1])


@dagonly
def multiply():

    in1 = setup()[0]

    mult = sal.multiply(in1, "mult", "d", ["a", "b"])

    out = sal.collect(mult, 1)

    return set([in1])


@dagonly
def project():

    in1 = setup()[0]

    proj = sal.project(in1, "proj", ["a", "b"])

    out = sal.collect(proj, 1)

    return set([in1])


@dagonly
def join():

    inpts = setup()
    in1, in2 = inpts[0], inpts[1]

    join = sal.join(in1, in2, "join", ["a", "b"], ["a", "b"])

    out = sal.collect(join, 1)

    return set([in1, in2])


if __name__ == "__main__":

    dag_agg = agg()
    cfg_agg = CodeGenConfig('agg')
    cg_agg = SparkCodeGen(cfg_agg, dag_agg)
    cg_agg.generate('agg', '/tmp')

    dag_cc = concat()
    cfg_cc = CodeGenConfig('cc')
    cg_cc = SparkCodeGen(cfg_cc, dag_cc)
    cg_cc.generate('cc', '/tmp')

    dag_distinct = distinct()
    cfg_distinct = CodeGenConfig('distinct')
    cg_distinct = SparkCodeGen(cfg_distinct, dag_distinct)
    cg_distinct.generate('distinct', '/tmp')

    dag_div = divide()
    cfg_div = CodeGenConfig('div')
    cg_div = SparkCodeGen(cfg_div, dag_div)
    cg_div.generate('div', '/tmp')

    dag_mult = multiply()
    cfg_mult = CodeGenConfig('mult')
    cg_mult = SparkCodeGen(cfg_mult, dag_mult)
    cg_mult.generate('mult', '/tmp')

    dag_proj = project()
    cfg_proj = CodeGenConfig('proj')
    cg_proj = SparkCodeGen(cfg_proj, dag_proj)
    cg_proj.generate('proj', '/tmp')

    dag_join = join()
    cfg_join = CodeGenConfig('join')
    cg_join = SparkCodeGen(cfg_join, dag_join)
    cg_join.generate('join', '/tmp')



