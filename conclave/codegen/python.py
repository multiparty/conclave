import os

import pystache

from conclave.codegen import CodeGen
from conclave.job import PythonJob
import conclave.dag as saldag


class PythonCodeGen(CodeGen):
    """ Codegen subclass for generating Python code. """

    def __init__(self, config, dag: saldag.Dag, space="    ",
                 template_directory="{}/templates/python".format(os.path.dirname(os.path.realpath(__file__)))):
        """ Initialize PythonCodeGen object. """
        super(PythonCodeGen, self).__init__(config, dag)
        self.template_directory = template_directory
        # this belongs inside config
        self.space = space

    def _generate_outputs(self, op_code: str):
        """ Generate code to save outputs to file. """
        leaf_nodes = [node for node in self.dag.top_sort() if node.is_leaf()]
        for leaf in leaf_nodes:
            op_code += self._generate_output(leaf)
        return op_code

    def _generate_job(self, job_name: str, code_directory: str, op_code: str):
        """ Top level code generation function. """
        op_code = self._generate_outputs(op_code)
        template = open("{}/top_level.tmpl"
                        .format(self.template_directory), 'r').read()
        data = {
            'OP_CODE': op_code
        }

        op_code = pystache.render(template, data)
        job = PythonJob(job_name, "{}/{}".format(code_directory, job_name))
        return job, op_code

    def _generate_concat(self, concat_op: saldag.Concat):
        """ Generate code for Concat operations. """
        in_rel_str = " + ".join([in_rel.name for in_rel in concat_op.get_in_rels()])
        return "{}{} = {}\n".format(
            self.space,
            concat_op.out_rel.name,
            in_rel_str
        )

    def _generate_aggregate(self, agg_op: saldag.Aggregate):
        """ Generate code for Aggregate operations. """
        # TODO handle multi-column case
        return "{}{} = aggregate({}, {}, {}, '{}')\n".format(
            self.space,
            agg_op.out_rel.name,
            agg_op.get_in_rel().name,
            agg_op.group_cols[0].idx,
            agg_op.agg_col.idx,
            agg_op.aggregator
        )

    def _generate_multiply(self, mult_op: saldag.Multiply):
        """ Generate code for Multiply operations. """
        operands = [col.idx for col in mult_op.operands]
        lambda_expr = "lambda row : " + " * ".join(["row[{}]".format(idx) for idx in operands])
        return "{}{} = arithmetic_project({}, {}, {})\n".format(
            self.space,
            mult_op.out_rel.name,
            mult_op.get_in_rel().name,
            mult_op.target_col.idx,
            lambda_expr
        )

    def _generate_output(self, leaf: saldag.OpNode):
        """ Generate code for storing a single output. """
        schema_header = ",".join(['"' + col.name + '"' for col in leaf.out_rel.columns])
        return "{}write_rel('{}', '{}.csv', {}, '{}')\n".format(
            self.space,
            self.config.output_path,
            leaf.out_rel.name,
            leaf.out_rel.name,
            schema_header
        )

    def _generate_create(self, create_op: saldag.Create):
        """ Generate code for loading input data. """
        return "{}{} = read_rel('{}')\n".format(
            self.space,
            create_op.out_rel.name,
            self.config.input_path + "/" + create_op.out_rel.name + ".csv"
        )

    def _generate_join_flags(self, join_flags_op: saldag.JoinFlags):
        """ Generate code for JoinFlags operations. """
        return "{}{}  = join_flags({}, {}, {}, {})\n".format(
            self.space,
            join_flags_op.out_rel.name,
            join_flags_op.get_left_in_rel().name,
            join_flags_op.get_right_in_rel().name,
            join_flags_op.left_join_cols[0].idx,
            join_flags_op.right_join_cols[0].idx
        )

    def _generate_join(self, join_op: saldag.Join):
        """ Generate code for Join operations. """
        return "{}{}  = join({}, {}, {}, {})\n".format(
            self.space,
            join_op.out_rel.name,
            join_op.get_left_in_rel().name,
            join_op.get_right_in_rel().name,
            join_op.left_join_cols[0].idx,
            join_op.right_join_cols[0].idx
        )

    def _generate_project(self, project_op: saldag.Project):
        """ Generate code for Project operations. """
        selected_cols = [col.idx for col in project_op.selected_cols]
        return "{}{} = project({}, {})\n".format(
            self.space,
            project_op.out_rel.name,
            project_op.get_in_rel().name,
            selected_cols
        )

    def _generate_distinct(self, distinct_op: saldag.Distinct):
        """ Generate code for Distinct operations. """
        selected_cols = [col.idx for col in distinct_op.selected_cols]
        return "{}{} = distinct({}, {})\n".format(
            self.space,
            distinct_op.out_rel.name,
            distinct_op.get_in_rel().name,
            selected_cols
        )

    def _generate_sort_by(self, sort_by_op: saldag.SortBy):
        """ Generate code for SortBy operations. """
        return "{}{} = sort_by({}, {})\n".format(
            self.space,
            sort_by_op.out_rel.name,
            sort_by_op.get_in_rel().name,
            sort_by_op.sort_by_col.idx
        )

    def _generate_comp_neighs(self, comp_neighs_op: saldag.CompNeighs):
        """ Generate code for CompNeighs operations. """
        return "{}{} = comp_neighs({}, {})\n".format(
            self.space,
            comp_neighs_op.out_rel.name,
            comp_neighs_op.get_in_rel().name,
            comp_neighs_op.comp_col.idx
        )

    def _generate_index(self, index_op: saldag.Index):
        """ Generate code for Index operations. """
        return "{}{} = project_indeces({})\n".format(
            self.space,
            index_op.out_rel.name,
            index_op.get_in_rel().name
        )

    def _generate_index_aggregate(self, index_agg_op: saldag.IndexAggregate):
        # TODO: generalize
        return "{}{} = index_agg({}, {}, {}, {}, lambda x, y: x {} y)\n".format(
            self.space,
            index_agg_op.out_rel.name,
            index_agg_op.get_in_rel().name,
            index_agg_op.agg_col.idx,
            index_agg_op.sorted_keys_op.out_rel.name,
            index_agg_op.eq_flag_op.out_rel.name,
            index_agg_op.aggregator
        )

    def _write_code(self, code: str, job_name: str):
        os.makedirs("{}/{}".format(self.config.code_path,
                                   job_name), exist_ok=True)
        # write code to a file
        pyfile = open(
            "{}/{}/workflow.py".format(self.config.code_path, job_name), 'w')
        pyfile.write(code)
