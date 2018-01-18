from unittest import TestCase
import conclave.lang as sal
from conclave.codegen.sharemind import SharemindCodeGen, SharemindCodeGenConfig
from conclave.utils import *
from conclave.comp import dag_only
from conclave import CodeGenConfig
import os


def setup():

    # define inputs
    colsIn1 = [
        defCol("a", "INTEGER", [1]),
        defCol("b", "INTEGER", [1]),
        defCol("c", "INTEGER", [1]),
        defCol("d", "INTEGER", [1])
    ]
    colsIn2 = [
        defCol("a", "INTEGER", [2]),
        defCol("b", "INTEGER", [2]),
        defCol("c", "INTEGER", [2]),
        defCol("d", "INTEGER", [2])
    ]
    colsIn3 = [
        defCol("a", "INTEGER", [3]),
        defCol("b", "INTEGER", [3]),
        defCol("c", "INTEGER", [3]),
        defCol("d", "INTEGER", [3])
    ]

    in1 = sal.create("in1", colsIn1, set([1]))
    in2 = sal.create("in2", colsIn2, set([2]))
    in3 = sal.create("in3", colsIn3, set([3]))

    cl1 = sal._close(in1, "cl1", set([1, 2, 3]))
    cl2 = sal._close(in2, "cl2", set([1, 2, 3]))
    cl3 = sal._close(in3, "cl3", set([1, 2, 3]))

    rel = sal.concat([cl1, cl2, cl3], "rel")

    return set([in1, in2, in3]), rel


class TestSharemind(TestCase):

    def check_workflow(self, dag, name):
        expected_rootdir = \
            "{}/sharemind_expected".format(os.path.dirname(os.path.realpath(__file__)))

        sm_cfg = SharemindCodeGenConfig()
        cfg = CodeGenConfig('cfg').with_sharemind_config(sm_cfg)
        cg = SharemindCodeGen(cfg, dag, 1)

        actual = cg._generate('code', '/tmp')[1]['miner']

        with open(expected_rootdir + '/{}'.format(name), 'r') as f:
            expected = f.read()

        self.assertEqual(expected, actual)

    def test_col_div(self):

        @dag_only
        def protocol():
            inputs, rel = setup()
            div = sal.divide(rel, 'div1', 'a', ['a', 'b'])

            opened = sal._open(div, "opened", 1)

            return inputs

        dag = protocol()
        self.check_workflow(dag, 'col_div')

    def test_col_mult(self):

        @dag_only
        def protocol():
            inputs, rel = setup()
            mult = sal.multiply(rel, 'mult1', 'a', ['a', 'b'])

            opened = sal._open(mult, "opened", 1)

            return inputs

        dag = protocol()
        self.check_workflow(dag, 'col_mult')

    def test_scalar_div(self):

        @dag_only
        def protocol():
            inputs, rel = setup()
            div = sal.divide(rel, 'div1', 'a', ['a', 1])

            opened = sal._open(div, "opened", 1)

            return inputs

        dag = protocol()
        self.check_workflow(dag, 'scalar_div')

    def test_scalar_mult(self):

        @dag_only
        def protocol():
            inputs, rel = setup()
            mult = sal.multiply(rel, 'mult1', 'a', ['a', 1])

            opened = sal._open(mult, "opened", 1)

            return inputs

        dag = protocol()
        self.check_workflow(dag, 'scalar_mult')

    def test_project(self):

        @dag_only
        def protocol():
            inputs, rel = setup()
            cols = [column.name for column in rel.out_rel.columns][::-1]
            proj = sal.project(rel, "proja", cols)

            opened = sal._open(proj, "opened", 1)

            return inputs

        dag = protocol()
        self.check_workflow(dag, 'project')

    def test_agg(self):

        @dag_only
        def protocol():
            inputs, rel = setup()
            agg = sal.aggregate(rel, "agg", ["a", "b"], "c", "sum", "agg_1")

            out = sal._open(agg, "opened", 1)

            return inputs

        dag = protocol()
        self.check_workflow(dag, 'agg')

    def test_shuffle(self):

        @dag_only
        def protocol():
            inputs, rel = setup()
            shuf = sal.shuffle(rel, "shuf")

            out = sal._open(shuf, "opened", 1)

            return inputs

        dag = protocol()
        self.check_workflow(dag, 'shuffle')

    def test_join(self):

        @dag_only
        def protocol():
            colsIn1 = [
                defCol("a", "INTEGER", [1]),
                defCol("b", "INTEGER", [1])
            ]
            in1 = sal.create("in1", colsIn1, set([1]))
            colsIn2 = [
                defCol("a", "INTEGER", [2]),
                defCol("b", "INTEGER", [2])
            ]
            in2 = sal.create("in2", colsIn2, set([2]))

            cl1 = sal._close(in1, "cl1", set([1, 2, 3]))
            cl2 = sal._close(in2, "cl2", set([1, 2, 3]))
            res = sal.join(cl1, cl2, "res", ["a"], ["a"])

            opened = sal._open(res, "opened", 1)

            return set([in1, in2])

        dag = protocol()
        self.check_workflow(dag, 'join')

