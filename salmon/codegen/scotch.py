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
            "MPC" if idx_agg_op.is_mpc else "",
            idx_agg_op.agg_col.getName(),
            idx_agg_op.aggregator,
            idx_agg_op.get_in_rel().dbgStr(),
            ",".join([group_col.getName() for group_col in idx_agg_op.group_cols]),
            idx_agg_op.out_rel.dbgStr()
        )

    def _generateAggregate(self, agg_op):

        return "AGG{} [{}, {}] FROM ({}) GROUP BY [{}] AS {}\n".format(
            "MPC" if agg_op.is_mpc else "",
            agg_op.agg_col.getName(),
            agg_op.aggregator,
            agg_op.get_in_rel().dbgStr(),
            ",".join([group_col.getName() for group_col in agg_op.group_cols]),
            agg_op.out_rel.dbgStr()
        )

    def _generateConcat(self, concat_op):

        inRelStr = ", ".join([in_rel.dbgStr()
                              for in_rel in concat_op.get_in_rels()])
        return "CONCAT{} [{}] AS {}\n".format(
            "MPC" if concat_op.is_mpc else "",
            inRelStr,
            concat_op.out_rel.dbgStr()
        )

    def _generateCreate(self, create_op):

        colTypeStr = ", ".join(
            [col.type_str for col in create_op.out_rel.columns])
        return "CREATE RELATION {} WITH COLUMNS ({})\n".format(
            create_op.out_rel.dbgStr(),
            colTypeStr
        )

    def _generateDivide(self, divide_op):

        operandColStr = " / ".join([str(col) for col in divide_op.operands])
        return "DIVIDE{} [{} -> {}] FROM ({}) AS {}\n".format(
            "MPC" if divide_op.is_mpc else "",
            str(divide_op.target_col),
            operandColStr,
            divide_op.get_in_rel().dbgStr(),
            divide_op.out_rel.dbgStr()
        )
        
    def _generateMultiply(self, multiply_op):

        operandColStr = " * ".join([str(col) for col in multiply_op.operands])
        return "MULTIPLY{} [{} -> {}] FROM ({}) AS {}\n".format(
            "MPC" if multiply_op.is_mpc else "",
            str(multiply_op.target_col),
            operandColStr,
            multiply_op.get_in_rel().dbgStr(),
            multiply_op.out_rel.dbgStr()
        )

    def _generateJoin(self, join_op):

        return "({}) JOIN{} ({}) ON [{}] AND [{}] AS {}\n".format(
            join_op.get_left_in_rel().dbgStr(),
            "MPC" if join_op.is_mpc else "",
            join_op.get_right_in_rel().dbgStr(),
            ",".join([c.name for c in join_op.left_join_cols]),
            ",".join([c.name for c in join_op.right_join_cols]),
            join_op.out_rel.dbgStr()
        )

    def _generateIndexJoin(self, index_join_op):

        return "({}) IDXJOIN{} ({}) WITH INDECES ({}) ON [{}] AND [{}] AS {}\n".format(
            index_join_op.get_left_in_rel().dbgStr(),
            "MPC" if index_join_op.is_mpc else "",
            index_join_op.get_right_in_rel().dbgStr(),
            index_join_op.index_rel.out_rel.dbgStr(),
            ",".join([c.name for c in index_join_op.left_join_cols]),
            ",".join([c.name for c in index_join_op.right_join_cols]),
            index_join_op.out_rel.dbgStr()
        )

    def _generateRevealJoin(self, reveal_join_op):

        return "({}) REVEALJOIN ({}) ON {} AND {} AS {}\n".format(
            reveal_join_op.get_left_in_rel().dbgStr(),
            reveal_join_op.get_right_in_rel().dbgStr(),
            ",".join([c.name for c in reveal_join_op.left_join_cols]),
            ",".join([c.name for c in reveal_join_op.right_join_cols]),
            reveal_join_op.out_rel.dbgStr()
        )

    def _generateHybridJoin(self, hybrid_join_op):

        return "({}) HYBRIDJOIN ({}) ON {} AND {} AS {}\n".format(
            hybrid_join_op.get_left_in_rel().dbgStr(),
            hybrid_join_op.get_right_in_rel().dbgStr(),
            ",".join([c.name for c in hybrid_join_op.left_join_cols]),
            ",".join([c.name for c in hybrid_join_op.right_join_cols]),
            hybrid_join_op.out_rel.dbgStr()
        )

    def _generateProject(self, project_op):

        selectedColsStr = ", ".join([str(col)
                                     for col in project_op.selected_cols])
        return "PROJECT{} [{}] FROM ({}) AS {}\n".format(
            "MPC" if project_op.is_mpc else "",
            selectedColsStr,
            project_op.get_in_rel().dbgStr(),
            project_op.out_rel.dbgStr()
        )

    def _generateDistinct(self, distinct_op):

        selectedColsStr = ", ".join([str(col)
                                     for col in distinct_op.selected_cols])
        return "DISTINCT{} [{}] FROM ({}) AS {}\n".format(
            "MPC" if distinct_op.is_mpc else "",
            selectedColsStr,
            distinct_op.get_in_rel().dbgStr(),
            distinct_op.out_rel.dbgStr()
        )

    def _generateSortBy(self, sort_by_op):

        return "SORTBY{} {} FROM ({}) AS {}\n".format(
            "MPC" if sort_by_op.is_mpc else "",
            sort_by_op.sort_by_col.name,
            sort_by_op.get_in_rel().dbgStr(),
            sort_by_op.out_rel.dbgStr()
        )

    def _generateCompNeighs(self, comp_neighs_op):

        return "COMPNEIGHS{} {} FROM ({}) AS {}\n".format(
            "MPC" if comp_neighs_op.is_mpc else "",
            comp_neighs_op.compCol.name,
            comp_neighs_op.get_in_rel().dbgStr(),
            comp_neighs_op.out_rel.dbgStr()
        )

    def _generateFilter(self, filter_op):

        filterStr = "{} {} {}".format(filter_op.target_col,
                filter_op.operator, filter_op.filter_expr)
        return "FILTER{} [{}] FROM ({}) AS {}\n".format(
            "MPC" if filter_op.is_mpc else "",
            filterStr,
            filter_op.get_in_rel().dbgStr(),
            filter_op.out_rel.dbgStr()
        )

    def _generateShuffle(self, shuffle_op):

        return "SHUFFLE{} ({}) AS {}\n".format(
            "MPC" if shuffle_op.is_mpc else "",
            shuffle_op.get_in_rel().dbgStr(),
            shuffle_op.out_rel.dbgStr()
        )

    def _generateIndex(self, index_op):

        return "INDEX{} ({}) AS {}\n".format(
            "MPC" if index_op.is_mpc else "",
            index_op.get_in_rel().dbgStr(),
            index_op.out_rel.dbgStr()
        )

    def _generateClose(self, close_op):

        return "CLOSE{} {} INTO {}\n".format(
            "MPC" if close_op.is_mpc else "",
            close_op.get_in_rel().dbgStr(),
            close_op.out_rel.dbgStr()
        )

    def _generateOpen(self, open_op):

        return "OPEN{} {} INTO {}\n".format(
            "MPC" if open_op.is_mpc else "",
            open_op.get_in_rel().dbgStr(),
            open_op.out_rel.dbgStr()
        )

    def _generatePersist(self, persist_op):

        return "PERSIST{} {} INTO {}\n".format(
            "MPC" if persist_op.is_mpc else "",
            persist_op.get_in_rel().dbgStr(),
            persist_op.out_rel.dbgStr()
        )
