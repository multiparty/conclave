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
                 "{}/templates/sharemind"
                 .format(os.path.dirname(os.path.realpath(__file__)))):

        if not "oblivc" in config.system_configs:
            print("Missing OblivC configuration in CodeGenConfig!")
            sys.exit(1)
        self.sm_config = config.system_configs['oblivc']

        super(OblivcCodeGen, self).__init__(config, dag)

        self.template_directory = template_directory
        self.pid = pid

    def _generate_aggregate(self, agg_op: Aggregate):
        """ Generate code for Aggregate operations. """

        template = open(
            "{0}/aggregate_sum.tmpl".format(self.template_directory), 'r').read()

        data = {
            "OUT_REL_NAME": agg_op.out_rel.name,
            "IN_REL_NAME": agg_op.get_in_rel().name,
            "KEY_COL_IDX": agg_op.group_cols[0].idx,
            "AGG_COL_IDX": agg_op.agg_col.idx
        }

        return pystache.render(template, data)

    def _generate_index_aggregate(self, idx_agg_op: IndexAggregate):
        """ Generate code for Index Aggregate operations. """

        template = open(
            "{0}/index_aggregate_sum.tmpl".format(self.template_directory), 'r').read()

        data = {
            "OUT_REL_NAME": idx_agg_op.out_rel.name,
            "IN_REL_NAME": idx_agg_op.get_in_rel().name,
            "GROUP_COL_IDX": idx_agg_op.group_cols[0].idx,
            "AGG_COL_IDX": idx_agg_op.agg_col.idx,
            "EQ_FLAG_REL": idx_agg_op.eq_flag_op.out_rel.name,
            "SORTED_KEYS_REL": idx_agg_op.sorted_keys_op.out_rel.name
        }

        return pystache.render(template, data)

    def _generate_concat(self, concat_op: Concat):
        """ Generate code for Concat operations. """

        in_rels = concat_op.get_in_rels()
        assert len(in_rels) > 1

        return ''

    def _generate_create(self, create_op: Create):
        """ Generate code for Create operations. """

        template = open(
            "{0}/read_from_csv.tmpl".format(self.template_directory), 'r').read()
        data = {
            "NAME": create_op.out_rel.name
        }

        return pystache.render(template, data)

    def _generate_divide(self, divide_op: Divide):
        """ Generate code for Divide operations. """

        template = open(
            "{0}/divide.tmpl".format(self.template_directory), 'r').read()

        operands = [op.idx if isinstance(
            op, Column) else op for op in divide_op.operands]
        operands_str = ",".join(str(op) for op in operands)
        scalar_flags = [0 if isinstance(
            op, Column) else 1 for op in divide_op.operands]
        scalar_flags_str = ",".join(str(op) for op in scalar_flags)

        data = {
            "OUT_REL": divide_op.out_rel.name,
            "IN_REL": divide_op.get_in_rel().name,
            "TARGET_COL": divide_op.target_col.idx,
            "SCALAR_FLAGS": "{" + scalar_flags_str + "}"
        }

        return pystache.render(template, data)

    def _generate_index_join(self, index_join_op: IndexJoin):
        """ Generate code for Index Join operations. """

        template = open(
            "{0}/index_join.tmpl".format(self.template_directory), 'r').read()

        index_rel = index_join_op.index_rel.out_rel

        data = {
            "TYPE": "uint32",
            "OUT_REL": index_join_op.out_rel.name,
            "LEFT_IN_REL": index_join_op.get_left_in_rel().name,
            "LEFT_KEY_COLS": str(index_join_op.left_join_cols[0].idx),
            "RIGHT_IN_REL": index_join_op.get_right_in_rel().name,
            "RIGHT_KEY_COLS": str(index_join_op.right_join_cols[0].idx),
            "INDEX_REL": index_rel.name
        }

        return pystache.render(template, data)

    def _generate_join(self, join_op: Join):
        """ Generate code for Join operations. """

        return ''

    def _generate_open(self, open_op: Open):
        """ Generate code for Open operations. """

        template = open(
            "{0}/reveal.tmpl".format(self.template_directory), 'r').read()

        data = {
            "OUT_REL": open_op.out_rel.name,
            "IN_REL": open_op.get_in_rel().name,
        }
        return pystache.render(template, data)

    def _generate_project(self, project_op: Project):
        """ Generate code for Project operations. """

        template = open(
            "{0}/project.tmpl".format(self.template_directory), 'r').read()
        selected_cols = project_op.selected_cols

        return ''

    def _generate_multiply(self, multiply_op: Multiply):
        """ Generate code for Multiply operations. """

        template = open(
            "{0}/multiply.tmpl".format(self.template_directory), 'r').read()

        operands = [op.idx if isinstance(
            op, Column) else op for op in multiply_op.operands]
        operands_str = ",".join(str(op) for op in operands)
        scalar_flags = [0 if isinstance(
            op, Column) else 1 for op in multiply_op.operands]
        scalar_flags_str = ",".join(str(op) for op in scalar_flags)

        data = {
            "OUT_REL": multiply_op.out_rel.name,
            "IN_REL": multiply_op.get_in_rel().name,
            "TARGET_COL": multiply_op.target_col.idx,
        }
        return pystache.render(template, data)


