from conclave.codegen import CodeGen
import conclave.dag as saldag


class ScotchCodeGen(CodeGen):
    """ Codegen subclass for generating relational debugging language. """

    def __init__(self, config, dag: saldag.Dag):

        super(ScotchCodeGen, self).__init__(config, dag)

    def _generate_job(self, job_name, output_directory, op_code: str):
        """ Top level job function. """

        return op_code

    def _generate_join_flags(self, join_flags_op: saldag.JoinFlags):
        """ Generate code for JoinFlags operations. """

        return "({}) JOINFLAGS{} ({}) ON [{}] AND [{}] AS {}\n".format(
            join_flags_op.get_left_in_rel().dbg_str(),
            "MPC" if join_flags_op.is_mpc else "",
            join_flags_op.get_right_in_rel().dbg_str(),
            ",".join([c.name for c in join_flags_op.left_join_cols]),
            ",".join([c.name for c in join_flags_op.right_join_cols]),
            join_flags_op.out_rel.dbg_str()
        )

    def _generate_flag_join(self, flag_join_op: saldag.FlagJoin):
        """ Generate code for FlagJoin operations. """

        return "({}) FLAGJOIN{} ({}) WITH FLAGS ({}) ON [{}] AND [{}] AS {}\n".format(
            flag_join_op.get_left_in_rel().dbg_str(),
            "MPC" if flag_join_op.is_mpc else "",
            flag_join_op.get_right_in_rel().dbg_str(),
            flag_join_op.join_flag_op.out_rel.dbg_str(),
            ",".join([c.name for c in flag_join_op.left_join_cols]),
            ",".join([c.name for c in flag_join_op.right_join_cols]),
            flag_join_op.out_rel.dbg_str()
        )

    def _generate_index_aggregate(self, idx_agg_op: saldag.IndexAggregate):
        """ Generate code for IndexAggregate operations. """

        return "IDXAGG{} [{}, {}] FROM ({}) GROUP BY [{}] AS {}\n".format(
            "MPC" if idx_agg_op.is_mpc else "",
            idx_agg_op.agg_col.get_name(),
            idx_agg_op.aggregator,
            idx_agg_op.get_in_rel().dbg_str(),
            ",".join([group_col.get_name() for group_col in idx_agg_op.group_cols]),
            idx_agg_op.out_rel.dbg_str()
        )

    def _generate_aggregate(self, agg_op: saldag.Aggregate):
        """ Generate code for Aggregate operations. """

        return "AGG{} [{}, {}] FROM ({}) GROUP BY [{}] AS {}\n".format(
            "MPC" if agg_op.is_mpc else "",
            agg_op.agg_col.get_name(),
            agg_op.aggregator,
            agg_op.get_in_rel().dbg_str(),
            ",".join([group_col.get_name() for group_col in agg_op.group_cols]),
            agg_op.out_rel.dbg_str()
        )

    def _generate_concat(self, concat_op: saldag.Concat):
        """ Generate code for Concat operations. """

        in_rel_str = ", ".join([in_rel.dbg_str() for in_rel in concat_op.get_in_rels()])
        return "CONCAT{} [{}] AS {}\n".format(
            "MPC" if concat_op.is_mpc else "",
            in_rel_str,
            concat_op.out_rel.dbg_str()
        )

    def _generate_create(self, create_op: saldag.Create):
        """ Generate code to Create relations. """

        col_type_str = ", ".join(
            [col.type_str for col in create_op.out_rel.columns])
        return "CREATE RELATION {} WITH COLUMNS ({})\n".format(
            create_op.out_rel.dbg_str(),
            col_type_str
        )

    def _generate_divide(self, divide_op: saldag.Divide):
        """ Generate code for Divide operations. """

        operand_col_str = " / ".join([str(col) for col in divide_op.operands])
        return "DIVIDE{} [{} -> {}] FROM ({}) AS {}\n".format(
            "MPC" if divide_op.is_mpc else "",
            str(divide_op.target_col),
            operand_col_str,
            divide_op.get_in_rel().dbg_str(),
            divide_op.out_rel.dbg_str()
        )
        
    def _generate_multiply(self, multiply_op: saldag.Multiply):
        """ Generate code for Multiply operations. """

        operand_col_str = " * ".join([str(col) for col in multiply_op.operands])
        return "MULTIPLY{} [{} -> {}] FROM ({}) AS {}\n".format(
            "MPC" if multiply_op.is_mpc else "",
            str(multiply_op.target_col),
            operand_col_str,
            multiply_op.get_in_rel().dbg_str(),
            multiply_op.out_rel.dbg_str()
        )

    def _generate_join(self, join_op: saldag.Join):
        """ Generate code for Join operations. """

        return "({}) JOIN{} ({}) ON [{}] AND [{}] AS {}\n".format(
            join_op.get_left_in_rel().dbg_str(),
            "MPC" if join_op.is_mpc else "",
            join_op.get_right_in_rel().dbg_str(),
            ",".join([c.name for c in join_op.left_join_cols]),
            ",".join([c.name for c in join_op.right_join_cols]),
            join_op.out_rel.dbg_str()
        )

    def _generate_index_join(self, index_join_op: saldag.IndexJoin):
        """ Generate code for IndexJoin operations. """

        return "({}) IDXJOIN{} ({}) WITH INDECES ({}) ON [{}] AND [{}] AS {}\n".format(
            index_join_op.get_left_in_rel().dbg_str(),
            "MPC" if index_join_op.is_mpc else "",
            index_join_op.get_right_in_rel().dbg_str(),
            index_join_op.index_rel.out_rel.dbg_str(),
            ",".join([c.name for c in index_join_op.left_join_cols]),
            ",".join([c.name for c in index_join_op.right_join_cols]),
            index_join_op.out_rel.dbg_str()
        )

    def _generate_reveal_join(self, reveal_join_op: saldag.RevealJoin):
        """ Generate code for RevealJoin operations. """

        return "({}) REVEALJOIN ({}) ON {} AND {} AS {}\n".format(
            reveal_join_op.get_left_in_rel().dbg_str(),
            reveal_join_op.get_right_in_rel().dbg_str(),
            ",".join([c.name for c in reveal_join_op.left_join_cols]),
            ",".join([c.name for c in reveal_join_op.right_join_cols]),
            reveal_join_op.out_rel.dbg_str()
        )

    def _generate_hybrid_join(self, hybrid_join_op: saldag.HybridJoin):
        """ Generate code for HybridJoin operations. """

        return "({}) HYBRIDJOIN ({}) ON {} AND {} AS {}\n".format(
            hybrid_join_op.get_left_in_rel().dbg_str(),
            hybrid_join_op.get_right_in_rel().dbg_str(),
            ",".join([c.name for c in hybrid_join_op.left_join_cols]),
            ",".join([c.name for c in hybrid_join_op.right_join_cols]),
            hybrid_join_op.out_rel.dbg_str()
        )

    def _generate_project(self, project_op: saldag.Project):
        """ Generate code for Project operations. """

        selected_cols_str = ", ".join([str(col) for col in project_op.selected_cols])
        return "PROJECT{} [{}] FROM ({}) AS {}\n".format(
            "MPC" if project_op.is_mpc else "",
            selected_cols_str,
            project_op.get_in_rel().dbg_str(),
            project_op.out_rel.dbg_str()
        )

    def _generate_distinct(self, distinct_op: saldag.Distinct):
        """ Generate code for Distinct operations. """

        selected_cols_str = ", ".join([str(col) for col in distinct_op.selected_cols])
        return "DISTINCT{} [{}] FROM ({}) AS {}\n".format(
            "MPC" if distinct_op.is_mpc else "",
            selected_cols_str,
            distinct_op.get_in_rel().dbg_str(),
            distinct_op.out_rel.dbg_str()
        )

    def _generate_sort_by(self, sort_by_op: saldag.SortBy):
        """ Generate code for Sort By operations. """

        return "SORTBY{} {} FROM ({}) AS {}\n".format(
            "MPC" if sort_by_op.is_mpc else "",
            sort_by_op.sort_by_col.name,
            sort_by_op.get_in_rel().dbg_str(),
            sort_by_op.out_rel.dbg_str()
        )

    def _generate_comp_neighs(self, comp_neighs_op: saldag.CompNeighs):
        """ Generate code for Comp Cols operations. """

        return "COMPNEIGHS{} {} FROM ({}) AS {}\n".format(
            "MPC" if comp_neighs_op.is_mpc else "",
            comp_neighs_op.comp_col.name,
            comp_neighs_op.get_in_rel().dbg_str(),
            comp_neighs_op.out_rel.dbg_str()
        )

    def _generate_filter(self, filter_op: saldag.Filter):
        """ Generate code for Filer operations. """

        filterStr = "{} {} {}".format(filter_op.target_col,
                filter_op.operator, filter_op.filter_expr)
        return "FILTER{} [{}] FROM ({}) AS {}\n".format(
            "MPC" if filter_op.is_mpc else "",
            filterStr,
            filter_op.get_in_rel().dbg_str(),
            filter_op.out_rel.dbg_str()
        )

    def _generate_shuffle(self, shuffle_op: saldag.Shuffle):
        """ Generate code for Shuffle operations. """

        return "SHUFFLE{} ({}) AS {}\n".format(
            "MPC" if shuffle_op.is_mpc else "",
            shuffle_op.get_in_rel().dbg_str(),
            shuffle_op.out_rel.dbg_str()
        )

    def _generate_index(self, index_op: saldag.Index):
        """ Generate code for Index operations. """

        return "INDEX{} ({}) AS {}\n".format(
            "MPC" if index_op.is_mpc else "",
            index_op.get_in_rel().dbg_str(),
            index_op.out_rel.dbg_str()
        )

    def _generate_close(self, close_op: saldag.Close):
        """ Generate code for Close operations. """

        return "CLOSE{} {} INTO {}\n".format(
            "MPC" if close_op.is_mpc else "",
            close_op.get_in_rel().dbg_str(),
            close_op.out_rel.dbg_str()
        )

    def _generate_open(self, open_op: saldag.Open):
        """ Generate code for Open operations. """

        return "OPEN{} {} INTO {}\n".format(
            "MPC" if open_op.is_mpc else "",
            open_op.get_in_rel().dbg_str(),
            open_op.out_rel.dbg_str()
        )

    def _generate_persist(self, persist_op: saldag.Persist):
        """ Generate code for Persist operations. """

        return "PERSIST{} {} INTO {}\n".format(
            "MPC" if persist_op.is_mpc else "",
            persist_op.get_in_rel().dbg_str(),
            persist_op.out_rel.dbg_str()
        )
