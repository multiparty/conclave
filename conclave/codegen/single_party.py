import os
import pystache

import conclave.dag as saldag

from conclave.codegen import CodeGen
from conclave.codegen.spark import SparkCodeGen
from conclave.codegen.python import PythonCodeGen
from conclave.job import SinglePartyJob


class SinglePartyCodegen(CodeGen):

    def __init__(self, config, dag: saldag.Dag, fmwk: str):
        super(SinglePartyCodegen, self).__init__(config, dag)
        self.fmwk = fmwk

    def generate(self, job_name: str, output_directory: str):

        if self.fmwk == "python":

            code = PythonCodeGen(self.config, self.dag)._generate(job_name, output_directory)[1]
            self._write_python_code(code, job_name)

        elif self.fmwk == "spark":

            code = SparkCodeGen(self.config, self.dag)._generate(job_name, output_directory)[1]
            self._write_spark_code(code, job_name)

        else:

            raise Exception("Unknown framework: {}".format(self.fmwk))

        job = SinglePartyJob(self.fmwk, job_name, output_directory, self.config.compute_party, self.config.all_pids)

        return job

    def _write_python_code(self, code: str, job_name: str):

        os.makedirs("{}/{}".format(self.config.code_path, job_name), exist_ok=True)

        pyfile = open(
            "{}/{}/workflow.py".format(self.config.code_path, job_name), 'w')
        pyfile.write(code)

    def _write_bash(self, job_name: str):
        """ Generate bash script that runs Spark jobs. """

        roots, leaves = [], []

        nodes = self.dag.top_sort()
        for node in nodes:
            if node.is_root():
                roots.append("{}/{}"
                             .format(self.config.input_path, node.out_rel.name))
            elif node.is_leaf():
                leaves.append("{}/{}"
                              .format(self.config.input_path, node.out_rel.name))

        template_directory = "{}/templates/spark".format(os.path.dirname(os.path.realpath(__file__)))
        template = open("{}/bash.tmpl".format(template_directory), 'r').read()

        data = {
            'INPUTS': ' '.join(roots),
            'OUTPUTS': ' '.join(leaves),
            'PATH': "{}/{}".format(self.config.code_path, job_name)
        }

        return pystache.render(template, data)

    def _write_spark_code(self, code: str, job_name: str):
        """ Write generated code to file. """

        os.makedirs("{}/{}".format(self.config.code_path, job_name), exist_ok=True)

        # write code to a file
        pyfile = open("{}/{}/workflow.py".format(self.config.code_path, job_name), 'w')
        pyfile.write(code)

        bash_code = self._write_bash(job_name)
        bash = open("{}/{}/bash.sh".format(self.config.code_path, job_name), 'w')
        bash.write(bash_code)

