import os
import sys

import pystache

from conclave.codegen import CodeGen
from conclave.dag import *


class JiffCodeGen(CodeGen):

    def __init__(self, config, dag: Dag, pid: int,
                 template_directory=
                 "{}/templates/jiff"
                 .format(os.path.dirname(os.path.realpath(__file__)))):

        self.template_directory = template_directory
        self.pid = pid

        super(JiffCodeGen, self).__init__(config, dag)

        self.server_code = ''
        self.jiff_config = config.system_configs['jiff']

    def generate(self, job_name: str, output_directory: str):
        """
        Generate code for DAG passed, along with header and controller files.
        Write results to file.
        """

        job, jiff_code = self._generate(job_name, output_directory)

        self._write_code(jiff_code, job_name)

        return job

    def _generate_job(self, job_name: str, code_directory: str, op_code: str):

        template = open(
            "{0}/top_level.tmpl".format(self.template_directory), 'r').read()

        data = {}

        op_code = pystache.render(template, data)

        # job = JiffJob(job_name, "{}/{}".format(code_directory, job_name))

        # return job, op_code

    def _generate(self, job_name: [str, None], output_directory: [str, None]):
        """ Generate code for DAG passed"""

        op_code = ""

        # topological traversal
        nodes = self.dag.top_sort()

        for node in nodes:
            if isinstance(node, Aggregate):
                op_code += self._generate_aggregate(node)
            elif isinstance(node, Concat):
                op_code += self._generate_concat(node)
            elif isinstance(node, Close):
                op_code += self._generate_close(node)
            elif isinstance(node, Create):
                self._generate_create(node)
            elif isinstance(node, Join):
                op_code += self._generate_join(node)
            elif isinstance(node, Open):
                op_code += self._generate_open(node)
            elif isinstance(node, Project):
                op_code += self._generate_project(node)
            elif isinstance(node, Multiply):
                op_code += self._generate_multiply(node)
            elif isinstance(node, Divide):
                op_code += self._generate_divide(node)
            elif isinstance(node, SortBy):
                op_code += self._generate_sort_by(node)
            elif isinstance(node, Open):
                op_code += self._generate_open(node)
            else:
                print("encountered unknown operator type", repr(node))

        # expand top-level job template and return code
        return self._generate_job(job_name, self.config.code_path, op_code)

    def _generate_create(self, create_op: Create):

        # assuming party with pid 1 will always be server party,
        # change if this isn't true
        if self.pid == 1:

            template = open(
                "{0}/server.tmpl".format(self.template_directory), 'r').read()

            data = {
                "JIFF_PATH": self.jiff_config["jiff_path"],
                "PARTY_COUNT": self.config["all_pids"]
            }

            self.server_code += pystache.render(template, data)

    def _generate_aggregate(self, agg_op: Aggregate):

        template = open(
            "{0}/aggregate.tmpl".format(self.template_directory), 'r').read()

        data = {
            # input values here
        }

        return pystache.render(template, data)

    def _generate_concat(self, concat_op: Concat):

        template = open(
            "{0}/concat.tmpl".format(self.template_directory), 'r').read()

        data = {}

        return pystache.render(template, data)

    def _generate_close(self, close_op: Close):

        template = open(
            "{0}/close.tmpl".format(self.template_directory), 'r').read()

        data = {}

        return pystache.render(template, data)

    def _generate_join(self, join_op: Join):

        template = open(
            "{0}/join.tmpl".format(self.template_directory), 'r').read()

        data = {}

        return pystache.render(template, data)

    def _generate_open(self, open_op: Open):

        template = open(
            "{0}/open.tmpl".format(self.template_directory), 'r').read()

        data = {}

        return pystache.render(template, data)

    def _generate_project(self, project_op: Project):

        template = open(
            "{0}/project.tmpl".format(self.template_directory), 'r').read()

        data = {}

        return pystache.render(template, data)

    def _generate_multiply(self, mult_op: Multiply):

        template = open(
            "{0}/multiply.tmpl".format(self.template_directory), 'r').read()

        data = {}

        return pystache.render(template, data)

    def _generate_divide(self, div_op: Divide):

        template = open(
            "{0}/divide.tmpl".format(self.template_directory), 'r').read()

        data = {}

        return pystache.render(template, data)

    def _generate_sort_by(self, sort_op: SortBy):

        template = open(
            "{0}/sort.tmpl".format(self.template_directory), 'r').read()

        data = {}

        return pystache.render(template, data)



