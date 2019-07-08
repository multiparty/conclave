import os

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

        if "oblivc" not in config.system_configs:
            raise Exception("Missing OblivC configuration in CodeGenConfig.\n")

        self.oc_config = config.system_configs['oblivc']
        self.create_params = {}

        super(OblivcCodeGen, self).__init__(config, dag)

        self.template_directory = template_directory
        self.pid = pid
        self.in_path = None

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
            elif isinstance(node, DistinctCount):
                op_code += self._generate_distinct_count(node)
            elif isinstance(node, Filter):
                op_code += self._generate_filter(node)
            elif isinstance(node, ConcatCols):
                op_code += self._generate_concat_cols(node)
            elif isinstance(node, Limit):
                op_code += self._generate_limit(node)
            else:
                print("encountered unknown operator type", repr(node))

        # expand top-level job template and return code
        return self._generate_job(job_name, self.config.code_path, op_code)

    def _generate_job(self, job_name: str, code_directory: str, op_code: str):
        """
        Returns generated Spark code and Job object.
        """

        if self.config.use_floats:
            template = open(
                "{}/top_level_float.tmpl".format(self.template_directory), 'r').read()
        else:
            template = open(
                "{}/top_level_int.tmpl".format(self.template_directory), 'r').read()

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

        template = open(
             "{0}/close.tmpl".format(self.template_directory), 'r').read()

        data = {
            "RELNAME": close_op.out_rel.name,
            "STORED_WITH": stored_with_set.pop()
         }

        return pystache.render(template, data)

    def _generate_concat_cols(self, concat_cols_op: ConcatCols):

        if len(concat_cols_op.get_in_rels()) != 2:
            raise NotImplementedError("Only support concat cols of two relations")

        if concat_cols_op.use_mult:

            template = open(
                "{0}/matrix_mult.tmpl".format(self.template_directory), 'r').read()

            data = {
                "LEFT_REL": concat_cols_op.get_in_rels()[0].name,
                'RIGHT_REL': concat_cols_op.get_in_rels()[1].name,
                "OUT_REL": concat_cols_op.out_rel.name
            }

            return pystache.render(template, data)

        else:

            template = open(
                "{0}/concat_cols.tmpl".format(self.template_directory), 'r').read()

            data = {
                "LEFT_REL": concat_cols_op.get_in_rels()[0].name,
                'RIGHT_REL': concat_cols_op.get_in_rels()[1].name,
                "OUT_REL": concat_cols_op.out_rel.name
            }

            return pystache.render(template, data)

    def _generate_limit(self, limit_op: Limit):
        """
        Generate code for limit operation.
        """

        if self.config.use_leaky_ops:
            template = open(
                "{0}/limit_leaky.tmpl".format(self.template_directory), 'r').read()
        else:
            template = open(
                "{0}/limit.tmpl".format(self.template_directory), 'r').read()

        data = {
            "IN_REL": limit_op.get_in_rel().name,
            "OUT_REL": limit_op.out_rel.name,
            "NUM": limit_op.num
        }

        return pystache.render(template, data)

    def _generate_filter(self, filter_op: Filter):
        """
        Generate code for different filter operations.
        """

        if filter_op.is_scalar:

            template = open(
                "{0}/filter_eq_by_constant.tmpl".format(self.template_directory), 'r').read()

            data = {
                "IN_REL": filter_op.get_in_rel().name,
                "OUT_REL": filter_op.out_rel.name,
                "KEY_COL": filter_op.filter_col.idx,
                "CONSTANT": filter_op.scalar
            }

            return pystache.render(template, data)

        else:

            template = open(
                "{0}/filter_lt_by_column.tmpl".format(self.template_directory), 'r').read()

            data = {
                "IN_REL": filter_op.get_in_rel().name,
                "OUT_REL": filter_op.out_rel.name,
                "KEY_COL": filter_op.filter_col.idx,
                "COMP_COL": filter_op.other_col.idx
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

        if not self.config.use_leaky_ops:
            template = open(
                "{0}/join.tmpl".format(self.template_directory), 'r').read()
        else:
            template = open(
                "{0}/join_leaky.tmpl".format(self.template_directory), 'r').read()

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

        if agg_op.aggregator == 'sum':
            template = open(
                "{}/agg_sum.tmpl".format(self.template_directory), 'r').read()
        elif agg_op.aggregator == "count":
            template = open(
                "{}/agg_count.tmpl".format(self.template_directory), 'r').read()
        elif agg_op.aggregator == 'mean':
            template = open(
                "{}/agg_mean_with_count_col.tmpl".format(self.template_directory), 'r').read()
        elif agg_op.aggregator == "std_dev":
            template = open(
                "{}/std_dev.tmpl".format(self.template_directory), 'r').read()
        else:
            raise Exception("Unknown aggregator encountered: {}".format(agg_op.aggregator))

        # TODO: generalize codegen to handle multiple group_cols
        assert(len(agg_op.group_cols) == 1)

        if self.config.use_leaky_ops:
            leaky = 1
        else:
            leaky = 0

        data = {
            "IN_REL": agg_op.get_in_rel().name,
            "OUT_REL": agg_op.out_rel.name,
            "KEY_COL": agg_op.group_cols[0].idx,
            "AGG_COL": agg_op.agg_col.idx,
            "USE_LEAKY": leaky,
            "COUNT_COL": 2,
            "LEAKY": "Leaky" if leaky else ""
        }

        return pystache.render(template, data)

    def _generate_distinct_count(self, distinct_count_op: DistinctCount):
        """
        Generate code for DistinctCount operations.
        """

        if distinct_count_op.use_sort:
            template = open(
                "{}/distinct_count.tmpl".format(self.template_directory), 'r').read()
        else:
            template = open(
                "{}/distinct_count_presorted.tmpl".format(self.template_directory), 'r').read()

        data = {
            "IN_REL": distinct_count_op.get_in_rel().name,
            "OUT_REL": distinct_count_op.out_rel.name,
            "KEY_COL": distinct_count_op.selected_col.idx
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
            "PATH": "{0}/{1}".format(self.config.code_path, job_name)
        }

        return pystache.render(template, data)

    def _generate_header_json(self):
        """
        Generate header file that stores struct data.
        """

        nodes = self.dag.top_sort()

        in_path = ''

        for node in nodes:
            if isinstance(node, Create):
                if int(self.pid) in node.out_rel.stored_with:
                    in_path = "{0}/{1}.csv".format(self.config.input_path, node.out_rel.name)

        template = open(
            "{0}/protocol_io.tmpl".format(self.template_directory), 'r').read()

        data = {
            "IN_PATH": in_path,
            "TYPE": 'float' if self.config.use_floats else 'int'
        }

        return pystache.render(template, data)

    def _generate_controller(self):
        """
        Populates controller file that loads data and dispatches computation.
        """

        nodes = self.dag.top_sort()

        out_path = ''
        in_path = ''
        write_str = ''

        for node in nodes:
            if isinstance(node, Create):
                if int(self.pid) in node.out_rel.stored_with:
                    in_path = "{0}/{1}.csv".format(self.config.input_path, node.out_rel.name)
                    self.in_path = in_path

            if isinstance(node, Open):
                if int(self.pid) in node.out_rel.stored_with:
                    out_path = "{0}/{1}.csv".format(self.config.input_path, node.out_rel.name)
                    write_str += 'writeData(&io);'

        template = open(
            "{0}/c_controller.tmpl".format(self.template_directory), 'r').read()

        data = {
            "PID": self.pid,
            "OUTPUT_PATH": out_path,
            "INPUT_PATH": in_path,
            "WRITE_CODE": write_str,
            "TYPE": 'g' if self.config.use_floats else 'i',
            "TYPE_CONV_STR": 'atof' if self.config.use_floats else 'atoi',
            "NUM_TYPE": 'float' if self.config.use_floats else 'int'
        }

        return pystache.render(template, data)

    def _write_code(self, code: str, job_name: str):
        """
        Write generated code to file.
        """

        os.makedirs("{}/{}".format(self.config.code_path, job_name), exist_ok=True)

        oc_file = open("{}/{}/workflow.oc".format(self.config.code_path, job_name), 'w')
        oc_file.write(code)

        header_code = self._generate_header_json()
        header = open("{}/{}/header_params.json".format(self.config.code_path, job_name), 'w')
        header.write(header_code)

        controller_code = self._generate_controller()
        controller = open("{}/{}/workflow.c".format(self.config.code_path, job_name), 'w')
        controller.write(controller_code)

        bash_code = self._write_bash(job_name)
        bash = open("{}/{}/bash.sh".format(self.config.code_path, job_name), 'w')
        bash.write(bash_code)



