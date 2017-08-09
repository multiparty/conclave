from salmon.codegen import CodeGen
from salmon.dag import *
import os, pystache

class BeerCodeGen(CodeGen):
    def __init__(self, dag):
        super(BeerCodeGen, self).__init__(dag)

    def _generateJob(self, job_name, op_code):

        # very simple in this case: the BEER query is just the concatenation
        # of the operators' code, which we already have
        return op_code

    def _generateAggregate(self, agg_op):

        return "AGG{} [{}, {}] FROM ({}) GROUP BY [{}] AS {}\n".format(
                "MPC" if agg_op.isMPC else "",
                agg_op.aggCol.getName(),
                agg_op.aggregator,
                agg_op.getInRel().name,
                [groupCol.getName() for groupCol in agg_op.groupCols],
                agg_op.outRel.name
            )

    def _generateConcat(self, concat_op):

        inRelStr = ", ".join([inRel.name for inRel in concat_op.getInRels()])

        return "CONCAT{} [{}] AS {}\n".format(
                "MPC" if self.isMPC else "",
                inRelStr,
                concat_op.outRel.name
            )

    def _generateCreate(self, create_op):

        colTypeStr = ", ".join([col.typeStr for col in create_op.outRel.columns])

        return "CREATE RELATION {} WITH COLUMNS ({})\n".format(
                create_op.outRel.name,
                colTypeStr
            )

    def _generateJoin(self, join_op):

        return "({}) JOIN{} ({}) ON {} AND {} AS {}\n".format(
                join_op.getLeftInRel().name,
                "MPC" if join_op.isMPC else "",
                join_op.getRightInRel().name,
                str(join_op.leftJoinCol),
                str(join_op.rightJoinCol),
                join_op.outRel.name
            )

    def _generateProject(self, project_op):

        selectedColsStr = ", ".join([str(col) for col in project_op.selectedCols])

        return "PROJECT{} [{}] FROM ({}) AS {}\n".format(
                "MPC" if project_op.isMPC else "",
                selectedColsStr,
                project_op.getInRel().name,
                project_op.outRel.name
            )

    def _generateStore(self, store_op):

        return "STORE RELATION {} INTO {} AS {}\n".format(
                store_op.getInRel().name,
                store_op.outRel.getCombinedCollusionSet(),
                store_op.outRel.name
            )

    def _writeCode(self, code, output_directory, job_name):
        # write code to a file
        outfile = open("{}/{}.rap".format(output_directory, job_name), 'w')
        outfile.write(code)
