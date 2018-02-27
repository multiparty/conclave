import os
import sys

import pystache

from conclave.codegen import CodeGen
from conclave.dag import *
from conclave.rel import *


class OblivcCodeGen(CodeGen):
    """ Codegen subclass for generating OblivC code. """

    def __init__(self, config, dag: Dag, pid: int,
                 template_directory=
                 "{}/templates/oblivc"
                 .format(os.path.dirname(os.path.realpath(__file__)))):

        if not "oblivc" in config.system_configs:
            print("Missing OblivC configuration in CodeGenConfig!")
            sys.exit(1)
        self.oc_config = config.system_configs['oblivc']

        super(OblivcCodeGen, self).__init__(config, dag)

        self.template_directory = template_directory
        self.pid = pid

    def _generate_protocol_io(self):
        """
        Generate .h file that stores necessary structs and IO information.
        Currently takes input row/col numbers from config, but might need
        to generate that from the c_controller.c file instead in the future.
        """

        template = open(
            "{0}/protocol_io.tmpl".format(self.template_directory), 'r').read()

        data = {
            "NUM_ROWS": self.oc_config['num rows'],
            "NUM_COLS": self.oc_config['num cols']
        }

        return pystache.render(template, data)

    def _generate_controller(self):

        template = open(
            "{0}/c_controller.tmpl".format(self.template_directory), 'r').read()

        data = {
            "PID": self.pid,
            "IP_AND_PORT": self.oc_config["ip/port"],
            "INPUT_PATH": self.oc_config["input path"]
        }

        return pystache.render(template, data)

    def _generate_close(self, close_op: Close):
        """ Generate code for Create operations. """

        template = open(
            "{0}/close.tmpl".format(self.template_directory), 'r').read()

        data = {
            "RELNAME": close_op.out_rel.name,
            "PID": self.pid
        }

        return pystache.render(template, data)

    # TODO: more than two inrels, oc code is limited to 2
    def _generate_concat(self, concat_op: Concat):
        """ Generate code for Concat operations. """

        in_rels = concat_op.get_in_rels()
        assert len(in_rels) == 2

        template = open(
            "{0}/concat.tmpl".format(self.template_directory), 'r').read()

        data = {
            "INREL_LEFT": in_rels[0],
            "INREL_RIGHT": in_rels[1],
            "OUTREL": concat_op.out_rel.name
        }

        return pystache.render(template, data)

    def _generate_join(self, join_op: Join):
        """ Generate code for Join operations. """

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
        """ Generate code for Open operations. """

        template = open(
            "{0}/reveal.tmpl".format(self.template_directory), 'r').read()

        data = {
            "IN_REL": open_op.get_in_rel().name,
        }

        return pystache.render(template, data)

    def _generate_project(self, project_op: Project):
        """ Generate code for Project operations. """

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
        """ Generate code for Multiply operations. """

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


