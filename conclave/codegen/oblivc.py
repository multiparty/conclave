import os
import sys
import csv

import pystache

from conclave.codegen import CodeGen
from conclave.dag import *
from conclave.job import OblivCJob


class OblivcCodeGen(CodeGen):
    """
    Codegen subclass for generating OblivC code.
    """

    def __init__(self, config, dag: Dag, pid: int,
                 template_directory=
                 "{}/templates/oblivc"
                 .format(os.path.dirname(os.path.realpath(__file__)))):

        if not "oblivc" in config.system_configs:
            print("Missing OblivC configuration in CodeGenConfig.\n")
            sys.exit(1)

        self.oc_config = config.system_configs['oblivc']

        self.controller = ''
        self.header = ''

        super(OblivcCodeGen, self).__init__(config, dag)

        self.template_directory = template_directory
        self.pid = pid

    def generate(self, job_name: str, output_directory: str):
        """
        Generate code for DAG passed, along with header and controller files.
        Write results to file.
        """

        job, oc_code = self._generate(job_name, output_directory)

        self._write_code(oc_code, job_name)

        return job

    def _generate_job(self, job_name: str, code_directory: str, op_code: str):
        """
        Returns generated Spark code and Job object.
        """

        template = open(
            "{}/top_level.tmpl".format(self.template_directory), 'r').read()

        data = {
            'OP_CODE': op_code
        }

        op_code = pystache.render(template, data)

        job = OblivCJob(job_name, "{}/{}".format(code_directory, job_name))

        return job, op_code

    def count_rows(self, rel_name: str):
        """
        Count number of rows in input CSV file
        """

        with open(self.config.input_path + rel_name + '.csv', 'r') as in_file:
            file_obj = csv.reader(in_file, delimiter=self.config.delimiter)
            rows = sum(1 for row in file_obj)

        return rows

    def _generate_create(self, create_op: Create):
        """
        Generate controller file that dispatches MPC computation.
        Generate header and file that stores necessary structs and IO information.
        """

        if self.pid in create_op.out_rel.stored_with:

            self.num_cols = len(create_op.out_rel.columns)
            self.num_rows = self.count_rows(create_op.out_rel.name)

            self.input_rel_name = create_op.out_rel.name

        return ''

    def _generate_close(self, close_op: Close):
        """
        Generate code to close input data for MPC computation.
        """

        template = open(
            "{0}/close.tmpl".format(self.template_directory), 'r').read()

        data = {
            "RELNAME": close_op.out_rel.name,
            "PID": self.pid
        }

        return pystache.render(template, data)

    # TODO: generalize, oc code is limited to 2 input relations
    def _generate_concat(self, concat_op: Concat):
        """
        Generate code for Concat operations.
        """

        in_rels = concat_op.get_in_rels()
        assert len(in_rels) == 2

        template = open(
            "{0}/concat.tmpl".format(self.template_directory), 'r').read()

        data = {
            "INREL_LEFT": in_rels[0].name,
            "INREL_RIGHT": in_rels[1].name,
            "OUTREL": concat_op.out_rel.name
        }

        return pystache.render(template, data)

    def _generate_join(self, join_op: Join):
        """
        Generate code for Join operations.
        """

        template = open(
            "{0}/join.tmpl".format(self.template_directory), 'r').read()

        data = {
            "JOINCOL_ONE": join_op.left_join_cols[0],
            "JOINCOL_TWO": join_op.right_join_cols[0],
            "LEFT": join_op.get_left_in_rel().name,
            "RIGHT": join_op.get_right_in_rel().name,
            "OUTREL": join_op.out_rel.name
        }

        return pystache.render(template, data)

    # TODO: opens to both parties for now, make configurable
    def _generate_open(self, open_op: Open):
        """
        Generate code to reveal output data.
        """

        template = open(
            "{0}/reveal.tmpl".format(self.template_directory), 'r').read()

        data = {
            "IN_REL": open_op.get_in_rel().name,
        }

        return pystache.render(template, data)

    def _generate_project(self, project_op: Project):
        """
        Generate code for Project operations.
        """

        template = open(
            "{0}/project.tmpl".format(self.template_directory), 'r').read()

        selected_cols = project_op.selected_cols

        data = {
            "IN_REL": project_op.get_in_rel().name,
            "OUT_REL": project_op.out_rel.name,
            "PROJ_COLS": ','.join(c.idx for c in selected_cols),
            "NUM_COLS": len(selected_cols)
        }

        return pystache.render(template, data)

    def _generate_multiply(self, mult_op: Multiply):
        """
        Generate code for Multiply operations.
        """

        template = open(
            "{0}/multiply.tmpl".format(self.template_directory), 'r').read()

        op_cols = mult_op.operands
        target_col = mult_op.target_col.idx
        scalar = 1
        operands = []

        for op_col in op_cols:
            if hasattr(op_col, 'idx'):
                operands.append(op_col.idx)
            else:
                scalar = op_col

        new_col = 0
        if target_col not in set(operands):
            new_col = 1

        data = {
            "IN_REL": mult_op.get_in_rel().name,
            "OUT_REL": mult_op.out_rel.name,
            "OPERANDS": ','.join(i for i in operands),
            "SCALAR": scalar,
            "NUM_OPS": len(operands),
            "OP_COL_IDX": target_col,
            "NEW_COL": new_col
        }

        return pystache.render(template, data)

    def _write_bash(self):
        """
        Generate bash script that runs Obliv-c jobs.
        """

        template = open("{}/bash.tmpl"
                        .format(self.template_directory), 'r').read()

        data = {
            "OC_COMP_PATH": self.oc_config.oc_path
        }

        return pystache.render(template, data)

    def _generate_header(self):

        template = open(
            "{0}/protocol_io.tmpl".format(self.template_directory), 'r').read()

        data = {
            "NUM_ROWS": self.num_rows,
            "NUM_COLS": self.num_cols
        }

        return pystache.render(template, data)

    def _generate_controller(self):

        template = open(
            "{0}/c_controller.tmpl".format(self.template_directory), 'r').read()

        data = {
            "PID": self.pid,
            "IP_AND_PORT": self.oc_config.ip_and_port,
            "INPUT_PATH": self.config.input_path + '/' + self.input_rel_name + '.csv'
        }

        return pystache.render(template, data)

    def _write_code(self, code: str, job_name: str):
        """
        Write generated code to file.
        """

        os.makedirs("{}/{}".format(self.config.code_path, job_name), exist_ok=True)

        oc_file = open("{}/{}/workflow.oc".format(self.config.code_path, job_name), 'w')
        oc_file.write(code)

        header_code = self._generate_header()
        header = open("{}/{}/workflow.h".format(self.config.code_path, job_name), 'w')
        header.write(header_code)

        controller_code = self._generate_controller()
        controller = open("{}/{}/workflow.c".format(self.config.code_path, job_name), 'w')
        controller.write(controller_code)

        bash_code = self._write_bash()
        bash = open("{}/{}/bash.sh".format(self.config.code_path, job_name), 'w')
        bash.write(bash_code)



