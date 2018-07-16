import os
import sys

import pystache

from conclave.codegen import CodeGen
from conclave.dag import *
from conclave.job import JiffJob


class JiffCodeGen(CodeGen):

    def __init__(self, config, dag: Dag, pid: int,
                 template_directory=
                 "{}/templates/jiff"
                 .format(os.path.dirname(os.path.realpath(__file__)))):

        self.template_directory = template_directory
        self.pid = pid

        super(JiffCodeGen, self).__init__(config, dag)

        self.server_code = ''
        self.party_code = ''
        self.jiff_config = config.system_configs['jiff']

    def generate_server_code(self):

        template = open(
            "{0}/server.tmpl".format(self.template_directory), 'r').read()

        data = {
            "JIFF_PATH": self.jiff_config.jiff_path,
            "PORT": self.jiff_config.server_port
        }

        self.server_code += pystache.render(template, data)

        return self

    def generate_party_code(self):

        template = open(
            "{0}/party.tmpl".format(self.template_directory), 'r').read()

        data = {
            "SERVER_IP_PORT": "{0}:{1}".format(self.jiff_config.server_ip, self.jiff_config.server_port)
        }

        self.party_code += pystache.render(template, data)

        return self

    def generate(self, job_name: str, output_directory: str):
        """
        Generate code for DAG passed, along with header and controller files.
        Write results to file.
        """

        if self.pid == self.jiff_config.server_pid:
            self.generate_server_code()
        self.generate_party_code()

        job, op_code = self._generate(job_name, output_directory)

        self._write_code(op_code, job_name)

        return job

    def _generate_job(self, job_name: str, op_code: str):

        template = open(
            "{0}/top_level.tmpl".format(self.template_directory), 'r').read()

        data = {
            "OP_CODE": op_code
        }

        op_code = pystache.render(template, data)
        job = JiffJob(job_name, "{}/{}".format(self.config.code_path, job_name))

        self._write_code(op_code, job_name)

        return job, op_code

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
                op_code += ''
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

        return self._generate_job(job_name, op_code)

    def _generate_create(self, create_op: Create):

        # check that the input data belongs to exactly one party
        assert(len(create_op.out_rel.stored_with) == 1)

        copied_set = copy.deepcopy(create_op.out_rel.stored_with)
        data_holder = copied_set.pop()

        template = open(
            "{0}/create.tmpl".format(self.template_directory), 'r').read()

        data = {
            "OUTREL": create_op.out_rel.name,
            "ID": data_holder
        }

        return pystache.render(template, data)

    def _generate_aggregate(self, agg_op: Aggregate):
        # TODO: implement in codegen

        template = open(
            "{0}/aggregate.tmpl".format(self.template_directory), 'r').read()

        data = {
            # input values here
        }

        #return pystache.render(template, data)
        return ''

    def _generate_concat(self, concat_op: Concat):

        # in_rel_str = " + ".join([in_rel.name for in_rel in concat_op.get_in_rels()])

        template = open(
            "{0}/concat.tmpl".format(self.template_directory), 'r').read()

        data = {
            "OUTREL": concat_op.out_rel.name,
            "INRELS": ", ".join(in_rel.name for in_rel in concat_op.get_in_rels())
        }

        return pystache.render(template, data)

    def _generate_join(self, join_op: Join):

        template = open(
            "{0}/join.tmpl".format(self.template_directory), 'r').read()

        data = {
            "OUTREL": join_op.out_rel.name,
            "LEFTREL": join_op.get_left_in_rel().name,
            "RIGHTREL": join_op.get_right_in_rel().name,
            "LEFT_JOINCOL": join_op.left_join_cols[0].idx,
            "RIGHT_JOINCOL": join_op.right_join_cols[0].idx
        }

        return pystache.render(template, data)

    def _generate_open(self, open_op: Open):
        # TODO: implement in codegen

        template = open(
            "{0}/open.tmpl".format(self.template_directory), 'r').read()

        data = {}

        return pystache.render(template, data)

    def _generate_project(self, project_op: Project):

        template = open(
            "{0}/project.tmpl".format(self.template_directory), 'r').read()

        data = {
            "INREL": project_op.get_in_rel().name,
            "OUTREL": project_op.out_rel.name,
            "PROJCOLS": '[' + ','.join(str(c.idx) for c in project_op.selected_cols) + ']',
        }

        return pystache.render(template, data)

    def _generate_multiply(self, mult_op: Multiply):

        template = open(
            "{0}/multiply.tmpl".format(self.template_directory), 'r').read()

        op_cols = mult_op.operands
        target_col = mult_op.target_col.idx
        scalar = 1
        operands = []

        for op_col in op_cols:
            if hasattr(op_col, 'idx'):
                operands.append(str(op_col.idx))
            else:
                scalar = op_col

        new_col = 0
        if str(target_col) != operands[0]:
            new_col = 1

        data = {
            "OUTREL": mult_op.out_rel.name,
            "INREL": mult_op.get_in_rel().name,
            "NEWCOL": new_col,
            "TARGETCOL": target_col,
            "OPERANDS": '[' + ','.join(str(c.idx) for c in op_cols) + ']',
            "SCALAR": scalar
        }

        return pystache.render(template, data)

    def _generate_divide(self, div_op: Divide):

        template = open(
            "{0}/divide.tmpl".format(self.template_directory), 'r').read()

        op_cols = div_op.operands
        target_col = div_op.target_col.idx
        scalar = 1
        operands = []

        for op_col in op_cols:
            if hasattr(op_col, 'idx'):
                operands.append(str(op_col.idx))
            else:
                scalar = op_col

        new_col = 0
        if str(target_col) != operands[0]:
            new_col = 1

        data = {
            "OUTREL": div_op.out_rel.name,
            "INREL": div_op.get_in_rel().name,
            "NEWCOL": new_col,
            "TARGETCOL": target_col,
            "OPERANDS": '[' + ','.join(str(c.idx) for c in op_cols) + ']',
            "SCALAR": scalar
        }

        return pystache.render(template, data)

    def _generate_sort_by(self, sort_op: SortBy):
        # TODO: implement in codegen

        template = open(
            "{0}/sort.tmpl".format(self.template_directory), 'r').read()

        data = {}

        return pystache.render(template, data)

    def _generate_bash(self):
        # TODO: need clear way to run jiff server/party/mpc files outside of jiff repository

        bash_code = ''

        if self.pid == self.jiff_config.server_pid:
            bash_code += 'node server.js'

        return bash_code

    def _write_code(self, code, job_name):

        os.makedirs("{}/{}".format(self.config.code_path, job_name), exist_ok=True)

        if self.pid == self.jiff_config.server_pid:
            server_file = open("{}/{}/server.js".format(self.config.code_path, job_name), 'w')
            server_file.write(self.server_code)

        party_file = open("{}/{}/party.js".format(self.config.code_path, job_name), 'w')
        party_file.write(self.party_code)

        protocol_file = open("{}/{}/mpc.js".format(self.config.code_path, job_name), 'w')
        protocol_file.write(code)

        bash_code = self._generate_bash()
        bash_file = open("{}/{}/run.sh".format(self.config.code_path, job_name), 'w')
        bash_file.write(bash_code)




