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
        self.create_params = {}

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
                self._set_create_params(node)
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

    def _set_create_params(self, create_op: Create):
        """
        For each Create node, store its name and number of columns.
        """

        self.create_params[create_op.out_rel.name] = {}
        self.create_params[create_op.out_rel.name]["COLS"] = len(create_op.out_rel.columns)

        return self

    def _generate_close(self, close_op: Close):
        """
        Generate code to close input data for MPC computation.
        """

        stored_with_set = copy.deepcopy(close_op.get_in_rel().stored_with)

        assert(len(stored_with_set) > 0)

        # name of struct that will be read from
        parent_name = close_op.get_in_rel().name

        template = open(
             "{0}/close.tmpl".format(self.template_directory), 'r').read()

        data = {
            "RELNAME": close_op.out_rel.name,
            "STORED_WITH": stored_with_set.pop(),
            "PARENT": parent_name
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
            "JOINCOL_ONE": join_op.left_join_cols[0].idx,
            "JOINCOL_TWO": join_op.right_join_cols[0].idx,
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
            "PARTY": 0
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
            "PROJ_COLS": ','.join(str(c.idx) for c in selected_cols),
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
                operands.append(str(op_col.idx))
            else:
                scalar = op_col

        new_col = 0
        if str(target_col) != operands[0]:
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

    def _generate_divide(self, div_op: Divide):
        """
        Generate code for Multiply operations.
        """

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
            "IN_REL": div_op.get_in_rel().name,
            "OUT_REL": div_op.out_rel.name,
            "OPERANDS": ','.join(i for i in operands),
            "SCALAR": scalar,
            "NUM_OPS": len(operands),
            "OP_COL_IDX": target_col,
            "NEW_COL": new_col
        }

        return pystache.render(template, data)

    def _generate_sort_by(self, sort_op: SortBy):
        """
        Generate code for SortBy operations.
        """

        template = open(
            "{}/sort_by.tmpl".format(self.template_directory), 'r').read()

        data = {
            "IN_REL": sort_op.get_in_rel().name,
            "OUT_REL": sort_op.out_rel.name,
            "KEY_COL": sort_op.sort_by_col.idx
        }

        return pystache.render(template, data)

    def _generate_aggregate(self, agg_op: Aggregate):
        """
        Generate code for Aggregate operations.
        """

        # TODO: codegen assumes '+' aggregator, generalize

        template = open(
            "{}/agg.tmpl".format(self.template_directory), 'r').read()

        # TODO: generalize codegen to handle multiple group_cols
        assert(len(agg_op.group_cols) == 1)

        data = {
            "IN_REL": agg_op.get_in_rel().name,
            "OUT_REL": agg_op.out_rel.name,
            "KEY_COL": agg_op.group_cols[0].idx,
            "AGG_COL": agg_op.agg_col.idx
        }

        return pystache.render(template, data)

    def _write_bash(self, job_name):
        """
        Generate bash script that runs Obliv-c jobs.
        """

        template = open("{}/bash.tmpl"
                        .format(self.template_directory), 'r').read()

        data = {
            "OC_COMP_PATH": self.oc_config.oc_path,
            "IP_AND_PORT": self.oc_config.ip_and_port,
            "PATH": self.config.code_path + job_name
        }

        return pystache.render(template, data)

    def _generate_header(self):
        """
        Generate header file that stores struct data.
        """

        template = open(
            "{0}/protocol_io.tmpl".format(self.template_directory), 'r').read()

        structs_code = ''

        for in_rel in self.create_params.keys():
            structs_code += "\tIo {};\n".format(in_rel)

        data = {
            "STRUCTS": structs_code
        }

        return pystache.render(template, data)

    def _add_to_struct_code(self, node):
        """
        Generates code to pass locally held data to MPC computation.
        """

        node_name = node.out_rel.name

        path_str = "\tio.{0}.src = \"{1}/{2}.csv\";\n".format(node_name, self.config.input_path, node_name)
        cols_str = "\tio.{0}.cols = {1};\n".format(node_name, len(node.out_rel.columns))
        rows_str = "\tio.{0}.rows = countRows(&io.{1});\n".format(node_name, node_name)
        cols = "\tint COLS = io.{0}.cols;\n".format(node_name)
        rows = "\tint ROWS = io.{0}.rows;\n".format(node_name)
        load_str = "\tloadData(&io.{0});\n\n".format(node_name)

        return "".join([path_str, cols_str, rows_str, cols, rows, load_str])

    def _generate_controller(self):
        """
        Populates controller file that loads data and dispatches computation.
        """

        nodes = self.dag.top_sort()

        out_path = ''
        struct_code = ''
        write_str = ''
        mock_str = ''

        for node in nodes:
            if isinstance(node, Create):
                if int(self.pid) in node.out_rel.stored_with:
                    struct_code += self._add_to_struct_code(node)
                else:
                    # if data isn't held locally, must populate struct field with mock data.
                    # Doesn't affect computation but OC requires that something be there.
                    mock_str += "\tloadMockData(&io.{0}, ROWS, COLS);\n\n".format(node.out_rel.name)

            if isinstance(node, Open):
                out_path = node.out_rel.name
                if int(self.pid) in node.out_rel.stored_with:
                    write_str += 'writeData(&io);'

        struct_code += mock_str

        template = open(
            "{0}/c_controller.tmpl".format(self.template_directory), 'r').read()

        data = {
            "PID": self.pid,
            "OUTPUT_PATH": "{0}/{1}_{2}.csv".format(self.config.input_path, out_path, str(self.pid)),
            "WRITE_CODE": write_str,
            "STRUCT_CODE": struct_code
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

        bash_code = self._write_bash(job_name)
        bash = open("{}/{}/bash.sh".format(self.config.code_path, job_name), 'w')
        bash.write(bash_code)



