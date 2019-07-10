import os

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

        out_node = ''

        nodes = self.dag.top_sort()
        for node in nodes:
            if isinstance(node, Open):
                if self.pid in node.out_rel.stored_with:
                    out_node = node.out_rel.name
                    break

        write = "false"
        if out_node != '':
            write = "true"

        data = {
            "WRITE": write,
            "SERVER_IP_PORT": "{0}:{1}".format(self.jiff_config.server_ip, self.jiff_config.server_port),
            "OUTPUT_FILE": "{0}/{1}.csv".format(self.config.input_path, out_node)
        }

        self.party_code += pystache.render(template, data)

        return self

    def generate(self, job_name: str, output_directory: str):
        """
        Generate code for DAG passed, along with header and controller files.
        Write results to file.
        """

        self.generate_server_code()
        self.generate_party_code()

        job, op_code = self._generate(job_name, output_directory)

        self._write_code(op_code, job_name)

        return job

    def _generate_job(self, job_name: str, op_code: str):

        template = open(
            "{0}/mpc_top_level.tmpl".format(self.template_directory), 'r').read()

        data = {
            "OP_CODE": op_code,
            "JIFF_PATH": self.jiff_config.jiff_path
        }

        op_code = pystache.render(template, data)
        job = JiffJob(job_name, "{}/{}".format(self.config.code_path, job_name))

        self._write_code(op_code, job_name)

        return job, op_code

    def _generate(self, job_name: [str, None], output_directory: str):
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
                pass
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
            elif isinstance(node, ConcatCols):
                op_code += self._generate_concat_cols(node)
            elif isinstance(node, Open):
                op_code += self._generate_open(node)
            else:
                print("encountered unknown operator type", repr(node))

        return self._generate_job(job_name, op_code)

    def _generate_close(self, close_op: Close):

        # node.parent.out_rel.stored_with
        copied_set = copy.deepcopy(close_op.parent.out_rel.stored_with)
        data_holder = copied_set.pop()

        template = open(
            "{0}/create.tmpl".format(self.template_directory), 'r').read()

        data = {
            "OUTREL": close_op.out_rel.name,
            "ID": data_holder
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
                "OUTREL": concat_cols_op.out_rel.name
            }

            return pystache.render(template, data)

        else:
            # TODO: implement this
            return ""

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

        if agg_op.aggregator == 'sum':
            template = open(
                "{}/agg_sum.tmpl".format(self.template_directory), 'r').read()
        elif agg_op.aggregator == 'mean':
            template = open(
                "{}/agg_mean_with_count_col.tmpl".format(self.template_directory), 'r').read()

            data = {
                "INREL": agg_op.get_in_rel().name,
                "OUTREL": agg_op.out_rel.name,
                "KEY_COL": agg_op.group_cols[0].idx,
                "AGG_COL": agg_op.agg_col.idx,
                "COUNT_COL": 2
            }

            return pystache.render(template, data)

        elif agg_op.aggregator == 'std_dev':
            template = open(
                "{}/agg_std_dev.tmpl".format(self.template_directory), 'r').read()
        else:
            raise Exception("Unknown aggregator encountered: {}\n".format(agg_op.aggregator))

        if len(agg_op.group_cols) != 1:
            raise Exception("JIFF aggregation only supports a single Key Column.")

        data = {
            "INREL": agg_op.get_in_rel().name,
            "OUTREL": agg_op.out_rel.name,
            "KEY_COL": agg_op.group_cols[0].idx,
            "AGG_COL": agg_op.agg_col.idx
        }

        return pystache.render(template, data)

    def _generate_concat(self, concat_op: Concat):

        in_rels = [in_rel.name for in_rel in concat_op.get_in_rels()]

        template = open(
            "{0}/concat.tmpl".format(self.template_directory), 'r').read()

        data = {
            "OUTREL": concat_op.out_rel.name,
            "INRELS": ','.join(in_rels),
            "INRELS_KEEPROWS_STR": ','.join(i + 'KeepRows' for i in in_rels)
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

        template = open(
            "{0}/open.tmpl".format(self.template_directory), 'r').read()

        data = {
            "INREL": open_op.get_in_rel().name,
            "OUTREL": open_op.out_rel.name
        }

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
        """
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
            "OUTREL": mult_op.out_rel.name,
            "INREL": mult_op.get_in_rel().name,
            "NEWCOL": new_col,
            "TARGETCOL": target_col,
            "OPERANDS": '[' + ','.join(c for c in operands[1:]) + ']',
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
            "OPERANDS": '[' + ','.join(c for c in operands[1:]) + ']',
            "SCALAR": scalar
        }

        return pystache.render(template, data)

    def _generate_sort_by(self, sort_op: SortBy):

        template = open(
            "{0}/bubbleSort.tmpl".format(self.template_directory), 'r').read()

        data = {
            "INREL": sort_op.get_in_rel().name,
            "OUTREL": sort_op.out_rel.name,
            "KEY_COL": sort_op.sort_by_col.idx
        }

        return pystache.render(template, data)

    def _gen_input_string(self):

        nodes = self.dag.top_sort()

        for node in nodes:
            if isinstance(node, Create):
                if int(self.pid) in node.out_rel.stored_with:

                    node_name = node.out_rel.name
                    input_path = self.config.input_path

                    return "{0}/{1}.csv".format(input_path, node_name)

    def _write_bash(self, job_name):

        template = open("{}/bash.tmpl"
                        .format(self.template_directory), 'r').read()

        data = {
            "JIFF_PATH": self.jiff_config.jiff_path,
            "INPUT_PATH": self._gen_input_string(),
            "PARTY_COUNT": len(self.config.all_pids),
            "PARTY_ID": self.pid,
            "CODE_PATH": "{0}/{1}".format(self.config.code_path, job_name)
        }

        return pystache.render(template, data)

    def _write_server_bash(self, job_name):

        template = open("{}/server_bash.tmpl"
                        .format(self.template_directory), 'r').read()

        data = {
            "JIFF_PATH": self.jiff_config.jiff_path,
            "CODE_PATH": "{0}/{1}".format(self.config.code_path, job_name)
        }

        return pystache.render(template, data)

    def _write_code(self, code, job_name):

        os.makedirs("{}/{}".format(self.config.code_path, job_name), exist_ok=True)

        server_file = open("{}/{}/server.js".format(self.config.code_path, job_name), 'w')
        server_file.write(self.server_code)

        party_file = open("{}/{}/party.js".format(self.config.code_path, job_name), 'w')
        party_file.write(self.party_code)

        protocol_file = open("{}/{}/mpc.js".format(self.config.code_path, job_name), 'w')
        protocol_file.write(code)

        bash_code = self._write_bash(job_name)
        bash_file = open("{}/{}/run.sh".format(self.config.code_path, job_name), 'w')
        bash_file.write(bash_code)

        if self.pid == self.jiff_config.server_pid:

            server_bash_code = self._write_server_bash(job_name)
            server_bash_file = open("{}/{}/run_server.sh".format(self.config.code_path, job_name), 'w')
            server_bash_file.write(server_bash_code)




