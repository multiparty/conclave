from salmon.codegen import CodeGen
from salmon.dag import *
import os, pystache

class BeerCodeGen(CodeGen):
    def __init__(self, config, dag):
        super(BeerCodeGen, self).__init__(config, dag)

    def _generateJob(self, job_name, op_code):

        # very simple in this case: the BEER query is just the concatenation
        # of the operators' code, which we already have
        return op_code

    def _generateAggregate(self, agg_op):

        return "AGG{} [{}, {}] FROM ({}) GROUP BY [{}] AS {}\n".format(
                "MPC" if agg_op.is_mpc else "",
                agg_op.agg_col.getName(),
                agg_op.aggregator,
                agg_op.get_in_rel().name,
                [group_col.getName() for group_col in agg_op.group_cols],
                agg_op.out_rel.name
            )

    def _generateConcat(self, concat_op):

        inRelStr = ", ".join([in_rel.name for in_rel in concat_op.get_in_rels()])

        return "CONCAT{} [{}] AS {}\n".format(
                "MPC" if self.is_mpc else "",
                inRelStr,
                concat_op.out_rel.name
            )

    def _generateCreate(self, create_op):

        colTypeStr = ", ".join([col.type_str for col in create_op.out_rel.columns])

        return "CREATE RELATION {} WITH COLUMNS ({})\n".format(
                create_op.out_rel.name,
                colTypeStr
            )

    def _generateJoin(self, join_op):

        return "({}) JOIN{} ({}) ON {} AND {} AS {}\n".format(
                join_op.get_left_in_rel().name,
                "MPC" if join_op.is_mpc else "",
                join_op.get_right_in_rel().name,
                str(join_op.left_join_col),
                str(join_op.right_join_col),
                join_op.out_rel.name
            )

    def _generateProject(self, project_op):

        selectedColsStr = ", ".join([str(col) for col in project_op.selected_cols])

        return "PROJECT{} [{}] FROM ({}) AS {}\n".format(
                "MPC" if project_op.is_mpc else "",
                selectedColsStr,
                project_op.get_in_rel().name,
                project_op.out_rel.name
            )

    def _generateStore(self, store_op):

        return "STORE RELATION {} INTO {} AS {}\n".format(
                store_op.get_in_rel().name,
                store_op.out_rel.getCombinedCollusionSet(),
                store_op.out_rel.name
            )

    def _writeCode(self, code, job_name):
        # write code to a file
        outfile = open("{}/{}.rap".format(self.config.code_path, job_name), 'w')
        outfile.write(code)
