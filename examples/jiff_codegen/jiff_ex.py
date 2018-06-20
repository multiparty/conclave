import conclave.lang as sal
from conclave.codegen.jiff import JiffCodeGen
import conclave.config as config
from conclave.utils import *
from conclave.comp import dag_only

import sys


def generate(dag_one, name):
    """
    sys.argv[1] - file path to directory containing input file
    (full path is <path> + <input_rel_name> + '.csv')

    sys.argv[2] - path to obliv-c compiler (at /obliv-c/bin/oblivcc)
    """
    # "JIFF_PATH": self.jiff_config["jiff_path"],
    # "PARTY_COUNT": self.config["all_pids"]
    jiff_conf = config.JiffConfig({"jiff_path":"/tmp"},("127.0.0.1","8080"))

    cfg = config.CodeGenConfig(name)
    cfg.input_path = sys.argv[1]

    cfg.with_jiff_config(jiff_conf)

    cg1 = JiffCodeGen(cfg, dag_one, 1)
    cg1.generate('protocol1', '/tmp/prot/')

    cg2 = JiffCodeGen(cfg, dag_one, 2)
    cg2.generate('protocol2', '/tmp/prot/')


def setup():

    colsInA = [
        defCol('a', 'INTEGER', [1]),
        defCol('b', 'INTEGER', [1]),
        defCol('c', 'INTEGER', [1]),
        defCol('d', 'INTEGER', [1])
    ]

    colsInB = [
        defCol("a", "INTEGER", [2]),
        defCol("b", "INTEGER", [2]),
        defCol("c", "INTEGER", [2]),
        defCol("d", "INTEGER", [2])
    ]

    in1 = sal.create("in1", colsInA, set([1]))
    in2 = sal.create("in2", colsInB, set([2]))

    return [in1, in2]


def setup_three():

    colsInA = [
        defCol('a', 'INTEGER', [1]),
        defCol('b', 'INTEGER', [1]),
        defCol('c', 'INTEGER', [1]),
        defCol('d', 'INTEGER', [1])
    ]

    colsInB = [
        defCol("a", "INTEGER", [2]),
        defCol("b", "INTEGER", [2]),
        defCol("c", "INTEGER", [2]),
        defCol("d", "INTEGER", [2])
    ]

    colsInC = [
        defCol("a", "INTEGER", [2]),
        defCol("b", "INTEGER", [2]),
        defCol("c", "INTEGER", [2]),
        defCol("d", "INTEGER", [2])
    ]

    in1 = sal.create("in1", colsInA, set([1]))
    in2 = sal.create("in2", colsInB, set([2]))
    in3 = sal.create("in3", colsInC, set([2]))

    return [in1, in2, in3]

@dag_only
def project():

    in_rels = setup()
    in1 = in_rels[0]
    in2 = in_rels[1]

    cl1 = sal._close(in1, "cl1", set([1, 2]))
    cl2 = sal._close(in2, "cl2", set([1, 2]))

    rel = sal.concat([cl1, cl2], "rel")

    proj = sal.project(rel, 'proj1', ['b', 'a', 'c'])

    opened = sal._open(proj, "opened", 1)

    return set([in1, in2])


if __name__ == "__main__":

    dag = project()
    generate(dag, 'project')
