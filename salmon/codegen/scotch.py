from salmon.codegen import CodeGen
from salmon.dag import *


class ScotchCodeGen(CodeGen):
    # Basically BEER but with additional debugging information (scotch tape?)

    def __init__(self, config, dag):

        super(ScotchCodeGen, self).__init__(config, dag)

    def _generateJob(self, job_name, output_directory, op_code):

        return op_code

    def _generateIndexAggregate(self, idx_agg_op):

        return "IDXAGG{} [{}, {}] FROM ({}) GROUP BY [{}] AS {}\n".format(
            "MPC" if idx_agg_op.isMPC else "",
            idx_agg_op.aggCol.get_name(),
            idx_agg_op.aggregator,
            idx_agg_op.get_in_rel().dbg_str(),
            ",".join([groupCol.get_name() for groupCol in idx_agg_op.groupCols]),
            idx_agg_op.out_rel.dbg_str()
        )

    def _generateAggregate(self, agg_op):

        return "AGG{} [{}, {}] FROM ({}) GROUP BY [{}] AS {}\n".format(
            "MPC" if agg_op.isMPC else "",
            agg_op.aggCol.get_name(),
            agg_op.aggregator,
            agg_op.get_in_rel().dbg_str(),
            ",".join([groupCol.get_name() for groupCol in agg_op.groupCols]),
            agg_op.out_rel.dbg_str()
        )

    def _generateConcat(self, concat_op):

        inRelStr = ", ".join([inRel.dbg_str()
                              for inRel in concat_op.get_in_rels()])
        return "CONCAT{} [{}] AS {}\n".format(
            "MPC" if concat_op.isMPC else "",
            inRelStr,
            concat_op.out_rel.dbg_str()
        )

    def _generateCreate(self, create_op):

        colTypeStr = ", ".join(
            [col.typeStr for col in create_op.out_rel.columns])
        return "CREATE RELATION {} WITH COLUMNS ({})\n".format(
            create_op.out_rel.dbg_str(),
            colTypeStr
        )

    def _generateDivide(self, divide_op):

        operandColStr = " / ".join([str(col) for col in divide_op.operands])
        return "DIVIDE{} [{} -> {}] FROM ({}) AS {}\n".format(
            "MPC" if divide_op.isMPC else "",
            str(divide_op.targetCol),
            operandColStr,
            divide_op.get_in_rel().dbg_str(),
            divide_op.out_rel.dbg_str()
        )
        
    def _generateMultiply(self, multiply_op):

        operandColStr = " * ".join([str(col) for col in multiply_op.operands])
        return "MULTIPLY{} [{} -> {}] FROM ({}) AS {}\n".format(
            "MPC" if multiply_op.isMPC else "",
            str(multiply_op.targetCol),
            operandColStr,
            multiply_op.get_in_rel().dbg_str(),
            multiply_op.out_rel.dbg_str()
        )

    def _generateJoin(self, join_op):

        return "({}) JOIN{} ({}) ON [{}] AND [{}] AS {}\n".format(
            join_op.get_left_in_rel().dbg_str(),
            "MPC" if join_op.isMPC else "",
            join_op.get_right_in_rel().dbg_str(),
            ",".join([c.name for c in join_op.leftJoinCols]),
            ",".join([c.name for c in join_op.rightJoinCols]),
            join_op.out_rel.dbg_str()
        )

    def _generateIndexJoin(self, index_join_op):

        return "({}) IDXJOIN{} ({}) WITH INDECES ({}) ON [{}] AND [{}] AS {}\n".format(
            index_join_op.get_left_in_rel().dbg_str(),
            "MPC" if index_join_op.isMPC else "",
            index_join_op.get_right_in_rel().dbg_str(),
            index_join_op.indexRel.out_rel.dbg_str(),
            ",".join([c.name for c in index_join_op.leftJoinCols]),
            ",".join([c.name for c in index_join_op.rightJoinCols]),
            index_join_op.out_rel.dbg_str()
        )

    def _generateRevealJoin(self, reveal_join_op):

        return "({}) REVEALJOIN ({}) ON {} AND {} AS {}\n".format(
            reveal_join_op.get_left_in_rel().dbg_str(),
            reveal_join_op.get_right_in_rel().dbg_str(),
            ",".join([c.name for c in reveal_join_op.leftJoinCols]),
            ",".join([c.name for c in reveal_join_op.rightJoinCols]),
            reveal_join_op.out_rel.dbg_str()
        )

    def _generateHybridJoin(self, hybrid_join_op):

        return "({}) HYBRIDJOIN ({}) ON {} AND {} AS {}\n".format(
            hybrid_join_op.get_left_in_rel().dbg_str(),
            hybrid_join_op.get_right_in_rel().dbg_str(),
            ",".join([c.name for c in hybrid_join_op.leftJoinCols]),
            ",".join([c.name for c in hybrid_join_op.rightJoinCols]),
            hybrid_join_op.out_rel.dbg_str()
        )

    def _generateProject(self, project_op):

        selectedColsStr = ", ".join([str(col)
                                     for col in project_op.selectedCols])
        return "PROJECT{} [{}] FROM ({}) AS {}\n".format(
            "MPC" if project_op.isMPC else "",
            selectedColsStr,
            project_op.get_in_rel().dbg_str(),
            project_op.out_rel.dbg_str()
        )

    def _generateDistinct(self, distinct_op):

        selectedColsStr = ", ".join([str(col)
                                     for col in distinct_op.selectedCols])
        return "DISTINCT{} [{}] FROM ({}) AS {}\n".format(
            "MPC" if distinct_op.isMPC else "",
            selectedColsStr,
            distinct_op.get_in_rel().dbg_str(),
            distinct_op.out_rel.dbg_str()
        )

    def _generateSortBy(self, sort_by_op):

        return "SORTBY{} {} FROM ({}) AS {}\n".format(
            "MPC" if sort_by_op.isMPC else "",
            sort_by_op.sortByCol.name,
            sort_by_op.get_in_rel().dbg_str(),
            sort_by_op.out_rel.dbg_str()
        )

    def _generateCompNeighs(self, comp_neighs_op):

        return "COMPNEIGHS{} {} FROM ({}) AS {}\n".format(
            "MPC" if comp_neighs_op.isMPC else "",
            comp_neighs_op.compCol.name,
            comp_neighs_op.get_in_rel().dbg_str(),
            comp_neighs_op.out_rel.dbg_str()
        )

    def _generateFilter(self, filter_op):

        filterStr = "{} {} {}".format(filter_op.targetCol,
                filter_op.operator, filter_op.filterExpr)
        return "FILTER{} [{}] FROM ({}) AS {}\n".format(
            "MPC" if filter_op.isMPC else "",
            filterStr,
            filter_op.get_in_rel().dbg_str(),
            filter_op.out_rel.dbg_str()
        )

    def _generateShuffle(self, shuffle_op):

        return "SHUFFLE{} ({}) AS {}\n".format(
            "MPC" if shuffle_op.isMPC else "",
            shuffle_op.get_in_rel().dbg_str(),
            shuffle_op.out_rel.dbg_str()
        )

    def _generateIndex(self, index_op):

        return "INDEX{} ({}) AS {}\n".format(
            "MPC" if index_op.isMPC else "",
            index_op.get_in_rel().dbg_str(),
            index_op.out_rel.dbg_str()
        )

    def _generateClose(self, close_op):

        return "CLOSE{} {} INTO {}\n".format(
            "MPC" if close_op.isMPC else "",
            close_op.get_in_rel().dbg_str(),
            close_op.out_rel.dbg_str()
        )

    def _generateOpen(self, open_op):

        return "OPEN{} {} INTO {}\n".format(
            "MPC" if open_op.isMPC else "",
            open_op.get_in_rel().dbg_str(),
            open_op.out_rel.dbg_str()
        )

    def _generatePersist(self, persist_op):

        return "PERSIST{} {} INTO {}\n".format(
            "MPC" if persist_op.isMPC else "",
            persist_op.get_in_rel().dbg_str(),
            persist_op.out_rel.dbg_str()
        )
