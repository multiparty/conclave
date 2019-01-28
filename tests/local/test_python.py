import os
from unittest import TestCase

import conclave.lang as cc
from conclave import CodeGenConfig, generate_code, dispatch_jobs
from conclave.utils import defCol


class TestPython(TestCase):

    def dispatch_and_check(self, pid: str, protocol, out_rel_name: str, expected: list):
        # define name for the workflow
        workflow_name = "python-test-" + pid
        # configure conclave
        conclave_config = CodeGenConfig(workflow_name, int(pid))
        conclave_config.all_pids = [1]
        current_dir = os.path.dirname(os.path.realpath(__file__))
        # point conclave to the directory where the generated code should be stored/ read from
        conclave_config.code_path = os.path.join(current_dir, "tmp")
        # point conclave to directory where data is to be read from...
        conclave_config.input_path = os.path.join(current_dir, "inputs")
        # and written to
        conclave_config.output_path = os.path.join(current_dir, "tmp")
        # define this party's unique ID (in this demo there is only one party)
        job_queue = generate_code(protocol, conclave_config, ["sharemind"], ["python"], apply_optimizations=False)

        # TODO this is hacky
        # create scratch directory for result relations and code
        os.makedirs(conclave_config.output_path, exist_ok=True)

        dispatch_jobs(job_queue, conclave_config)
        with open(conclave_config.output_path + "/" + out_rel_name + ".csv") as f:
            actual = set(f.read().split()[1:])
            self.assertEqual(set(expected), set(actual))

    def test_union(self):
        def protocol():
            cols_in_1 = [
                defCol("a", "INTEGER", 1),
                defCol("b", "INTEGER", 1)
            ]
            in_1 = cc.create("in_1", cols_in_1, {1})
            cols_in_2 = [
                defCol("a", "INTEGER", 1),
                defCol("b", "INTEGER", 1)
            ]
            in_2 = cc.create("in_2", cols_in_2, {1})
            rel = cc.union(in_1, in_2, "rel", "a", "a")
            cc.collect(rel, 1)
            return {in_1, in_2}

        self.dispatch_and_check("1", protocol, "rel", ["1", "3", "4", "244"])
