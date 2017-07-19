from salmon.codegen import CodeGen
from salmon.dag import *


class ScotchCodeGen(CodeGen):
    # Basically BEER but with additional debugging information (scotch tape?)

    def __init__(self, dag):

        super(ScotchCodeGen, self).__init__(dag)

    def _generateJob(self, job_name, op_code):

        return op_code

    def _generateAggregate(self, agg_op):

        return "AGG{} [{}, {}] FROM ({}) GROUP BY [{}] AS {}\n".format(
            "MPC" if agg_op.isMPC else "",
            agg_op.aggCol.getName(),
            agg_op.aggregator,
            agg_op.getInRel().dbgStr(),
            agg_op.keyCol.getName(),
            agg_op.outRel.dbgStr()
        )

    def _generateConcat(self, concat_op):

        inRelStr = ", ".join([inRel.dbgStr()
                              for inRel in concat_op.getInRels()])
        return "CONCAT{} [{}] AS {}\n".format(
            "MPC" if concat_op.isMPC else "",
            inRelStr,
            concat_op.outRel.dbgStr()
        )

    def _generateCreate(self, create_op):

        colTypeStr = ", ".join(
            [col.typeStr for col in create_op.outRel.columns])
        return "CREATE RELATION {} WITH COLUMNS ({})\n".format(
            create_op.outRel.dbgStr(),
            colTypeStr
        )

    def _generateDivide(self, divide_op):

        operandColStr = " / ".join([str(col) for col in divide_op.operands])
        return "DIVIDE{} [{} -> {}] FROM ({}) AS {}\n".format(
            "MPC" if divide_op.isMPC else "",
            str(divide_op.targetCol),
            operandColStr,
            divide_op.getInRel().dbgStr(),
            divide_op.outRel.dbgStr()
        )

    def _generateJoin(self, join_op):

        return "({}) JOIN{} ({}) ON {} AND {} AS {}\n".format(
            join_op.getLeftInRel().dbgStr(),
            "MPC" if join_op.isMPC else "",
            join_op.getRightInRel().dbgStr(),
            str(join_op.leftJoinCol),
            str(join_op.rightJoinCol),
            join_op.outRel.dbgStr()
        )

    def _generateRevealJoin(self, reveal_join_op):

        return "({}) REVEALJOIN ({}) ON {} AND {} AS {}\n".format(
            reveal_join_op.getLeftInRel().dbgStr(),
            reveal_join_op.getRightInRel().dbgStr(),
            str(reveal_join_op.leftJoinCol),
            str(reveal_join_op.rightJoinCol),
            reveal_join_op.outRel.dbgStr()
        )

    def _generateHybridJoin(self, hybrid_join_op):

        return "({}) HYBRIDJOIN ({}) ON {} AND {} AS {}\n".format(
            hybrid_join_op.getLeftInRel().dbgStr(),
            hybrid_join_op.getRightInRel().dbgStr(),
            str(hybrid_join_op.leftJoinCol),
            str(hybrid_join_op.rightJoinCol),
            hybrid_join_op.outRel.dbgStr()
        )

    def _generateProject(self, project_op):

        selectedColsStr = ", ".join([str(col)
                                     for col in project_op.selectedCols])
        return "PROJECT{} [{}] FROM ({}) AS {}\n".format(
            "MPC" if project_op.isMPC else "",
            selectedColsStr,
            project_op.getInRel().dbgStr(),
            project_op.outRel.dbgStr()
        )

    def _generateMultiply(self, multiply_op):

        operandColStr = " * ".join([str(col) for col in multiply_op.operands])
        return "MULTIPLY{} [{} -> {}] FROM ({}) AS {}\n".format(
            "MPC" if multiply_op.isMPC else "",
            str(multiply_op.targetCol),
            operandColStr,
            multiply_op.getInRel().dbgStr(),
            multiply_op.outRel.dbgStr()
        )

    def _generateClose(self, close_op):

        return "CLOSE {} INTO {}\n".format(
            close_op.getInRel().dbgStr(),
            close_op.outRel.dbgStr()
        )

    def _generateOpen(self, open_op):

        return "OPEN {} INTO {}\n".format(
            open_op.getInRel().dbgStr(),
            open_op.outRel.dbgStr()
        )
