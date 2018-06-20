from unittest import TestCase
import conclave.lang as sal
from conclave.codegen.jiff import JiffCodeGen
from conclave.utils import *
from conclave.comp import dag_only
from conclave.config import CodeGenConfig, JiffConfig
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

    in1 = sal.create("in1", colsIn1, {1})
    in2 = sal.create("in2", colsIn2, {2})
    in3 = sal.create("in3", colsIn3, {3})

    cl1 = sal._close(in1, "cl1", {1, 2, 3})
    cl2 = sal._close(in2, "cl2", {1, 2, 3})
    cl3 = sal._close(in3, "cl3", {1, 2, 3})

    rel = sal.concat([cl1, cl2, cl3], "rel")

    return {in1, in2, in3}, rel


class TestSharemind(TestCase):

    def check_workflow(self, dag, name, use_leaky_ops=True):
        expected_rootdir = \
            "{}/sharemind_expected".format(os.path.dirname(os.path.realpath(__file__)))

        sm_cfg = JiffConfig()
        cfg = CodeGenConfig('cfg').with_sharemind_config(sm_cfg)
        cfg.use_leaky_ops = use_leaky_ops
        cg = JiffCodeGen(cfg, dag, 1)

        actual = cg._generate('code', '/tmp')[1]['miner']

        with open(expected_rootdir + '/{}'.format(name), 'r') as f_specific, open(
                expected_rootdir + '/{}'.format("base"), 'r') as f_base:
            expected_base = f_base.read()
            expected_specific = f_specific.read()
            expected = expected_base + expected_specific

        self.assertEqual(expected, actual)

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

