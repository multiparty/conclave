from salmon.codegen import CodeGen
from salmon.dag import *

# Basically BEER but with additional debugging information (scotch tape?)
class ScotchCodeGen(CodeGen):
    
    def __init__(self, dag):
        super(ScotchCodeGen, self).__init__(dag)

    def _generateJob(self, job_name, op_code):

        return op_code

    def _generateAggregate(self, agg_op):

        return "AGG{} [{}, {}] FROM ({} {}) GROUP BY [{}] AS {} {}\n".format(
                "MPC" if agg_op.isMPC else "",
                agg_op.aggCol.getName(),
                agg_op.aggregator,
                agg_op.getInRel().name,
                agg_op.getInRel().storedWith,
                agg_op.keyCol.getName(),
                agg_op.outRel.name,
                agg_op.outRel.storedWith
            )

    def _generateConcat(self, concat_op):

        inRelStr = ", ".join([" ".join([inRel.name, str(inRel.storedWith)]) for inRel in concat_op.getInRels()])

        return "CONCAT{} [{}] AS {} {}\n".format(
                "MPC" if concat_op.isMPC else "",
                inRelStr,
                concat_op.outRel.name,
                concat_op.outRel.storedWith
            )

    def _generateCreate(self, create_op):

        colTypeStr = ", ".join([col.typeStr for col in create_op.outRel.columns])

        return "CREATE RELATION {} {} WITH COLUMNS ({})\n".format(
                create_op.outRel.name,
                create_op.outRel.storedWith,
                colTypeStr
            )

    def _generateJoin(self, join_op):

        return "({} {}) JOIN{} ({} {}) ON {} AND {} AS {} {}\n".format(
                join_op.getLeftInRel().name,
                join_op.getLeftInRel().storedWith,
                "MPC" if join_op.isMPC else "",
                join_op.getRightInRel().name,
                join_op.getRightInRel().storedWith,
                str(join_op.leftJoinCol),
                str(join_op.rightJoinCol),
                join_op.outRel.name,
                join_op.outRel.storedWith
            )

    def _generateProject(self, project_op):

        selectedColsStr = ", ".join([str(col) for col in project_op.selectedCols])

        return "PROJECT{} [{}] FROM ({} {}) AS {} {}\n".format(
                "MPC" if project_op.isMPC else "",
                selectedColsStr,
                project_op.getInRel().name,
                project_op.getInRel().storedWith,
                project_op.outRel.name,
                project_op.outRel.storedWith
            )

    def _generateStore(self, store_op):

        return "STORE RELATION {} {} INTO {} AS {}\n".format(
                store_op.getInRel().name,
                store_op.getInRel().storedWith,
                store_op.outRel.getCombinedCollusionSet(),
                store_op.outRel.name
            )

    def _writeCode(self, code, output_directory, job_name):
        # print out code
        print(code)
