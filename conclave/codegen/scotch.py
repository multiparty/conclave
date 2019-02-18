import conclave.dag as ccdag
from conclave.codegen import CodeGen


class ScotchCodeGen(CodeGen):
    """ Codegen subclass for generating relational debugging language. """

    def __init__(self, config, dag: ccdag.Dag):
        super(ScotchCodeGen, self).__init__(config, dag)

    @staticmethod
    def _generate_job(job_name, output_directory, op_code: str):
        """ Top level job function. """

        return op_code

    @staticmethod
    def _generate_join_flags(join_flags_op: ccdag.JoinFlags):
        """ Generate code for JoinFlags operations. """

        return "({}) JOINFLAGS{} ({}) ON [{}] AND [{}] AS {}\n".format(
            join_flags_op.get_left_in_rel().dbg_str(),
            "MPC" if join_flags_op.is_mpc else "",
            join_flags_op.get_right_in_rel().dbg_str(),
            ",".join([c.name for c in join_flags_op.left_join_cols]),
            ",".join([c.name for c in join_flags_op.right_join_cols]),
            join_flags_op.out_rel.dbg_str()
        )

    @staticmethod
    def _generate_pub_join(pub_join_op: ccdag.PubJoin):
        """ Generate code for PubJoin operations. """

        return "PUBJOIN{}{} ({}) {} ON [{}] AS {}\n".format(
            "MPC" if pub_join_op.is_mpc else "",
            "Server" if pub_join_op.is_server else "Client",
            pub_join_op.get_left_in_rel().dbg_str(),
            pub_join_op.get_right_in_rel().dbg_str() if pub_join_op.right_parent else "",
            pub_join_op.key_col.name,
            pub_join_op.out_rel.dbg_str()
        )

    @staticmethod
    def _generate_pub_intersect(pub_intersect_op: ccdag.PubIntersect):
        """ Generate code for PubIntersect operations. """

        return "PUBINTERSECT{}{} {} ON [{}] AS {}\n".format(
            "MPC" if pub_intersect_op.is_mpc else "",
            "Server" if pub_intersect_op.is_server else "Client",
            pub_intersect_op.get_in_rel().dbg_str(),
            pub_intersect_op.col.name,
            pub_intersect_op.out_rel.dbg_str()
        )

    @staticmethod
    def _generate_flag_join(flag_join_op: ccdag.FlagJoin):
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

    @staticmethod
    def _generate_leaky_hybrid_aggregate(lky_idx_agg_op: ccdag.LeakyIndexAggregate):
        """ Generate code for LeakyIndexAggregate operations. """

        return "LKYIDXAGG{} [{}, {}] FROM ({}) GROUP BY [{}] WITH KEYS {} AND MAP {} AS {}\n".format(
            "MPC" if lky_idx_agg_op.is_mpc else "",
            lky_idx_agg_op.agg_col.get_name(),
            lky_idx_agg_op.aggregator,
            lky_idx_agg_op.get_in_rel().dbg_str(),
            ",".join([group_col.get_name() for group_col in lky_idx_agg_op.group_cols]),
            lky_idx_agg_op.dist_keys.out_rel.name,
            lky_idx_agg_op.keys_to_idx_map.out_rel.name,
            lky_idx_agg_op.out_rel.dbg_str()
        )

    @staticmethod
    def _generate_index_aggregate(idx_agg_op: ccdag.IndexAggregate):
        """ Generate code for IndexAggregate operations. """

        return "IDXAGG{} [{}, {}] FROM ({}) GROUP BY [{}] AS {}\n".format(
            "MPC" if idx_agg_op.is_mpc else "",
            idx_agg_op.agg_col.get_name(),
            idx_agg_op.aggregator,
            idx_agg_op.get_in_rel().dbg_str(),
            ",".join([group_col.get_name() for group_col in idx_agg_op.group_cols]),
            idx_agg_op.out_rel.dbg_str()
        )

    @staticmethod
    def _generate_hybrid_aggregate(hybrid_agg_op: ccdag.HybridAggregate):
        """ Generate code for Aggregate operations. """

        return "HYBRIDAGG{} [{}, {}] FROM ({}) GROUP BY [{}] AS {} WITH STP {}\n".format(
            "MPC" if hybrid_agg_op.is_mpc else "",
            hybrid_agg_op.agg_col.get_name(),
            hybrid_agg_op.aggregator,
            hybrid_agg_op.get_in_rel().dbg_str(),
            ",".join([group_col.get_name() for group_col in hybrid_agg_op.group_cols]),
            hybrid_agg_op.out_rel.dbg_str(),
            hybrid_agg_op.trusted_party
        )

    @staticmethod
    def _generate_aggregate(agg_op: ccdag.Aggregate):
        """ Generate code for Aggregate operations. """

        return "AGG{} [{}, {}] FROM ({}) GROUP BY [{}] AS {}\n".format(
            "MPC" if agg_op.is_mpc else "",
            agg_op.agg_col.get_name() if agg_op.agg_col else "",
            agg_op.aggregator,
            agg_op.get_in_rel().dbg_str(),
            ",".join([group_col.get_name() for group_col in agg_op.group_cols]),
            agg_op.out_rel.dbg_str()
        )

    @staticmethod
    def _generate_concat(concat_op: ccdag.Concat):
        """ Generate code for Concat operations. """

        in_rel_str = ", ".join([in_rel.dbg_str() for in_rel in concat_op.get_in_rels()])
        return "CONCAT{} [{}] AS {}\n".format(
            "MPC" if concat_op.is_mpc else "",
            in_rel_str,
            concat_op.out_rel.dbg_str()
        )

    @staticmethod
    def _generate_concat_cols(concat_cols_op: ccdag.ConcatCols):
        """ Generate code for ConcatCols operations. """

        in_rel_str = ", ".join([in_rel.dbg_str() for in_rel in concat_cols_op.get_in_rels()])
        return "CONCATCOLS{} [{}] AS {}\n".format(
            "MPC" if concat_cols_op.is_mpc else "",
            in_rel_str,
            concat_cols_op.out_rel.dbg_str()
        )

    @staticmethod
    def _generate_create(create_op: ccdag.Create):
        """ Generate code to Create relations. """

        col_type_str = ", ".join(
            [col.type_str for col in create_op.out_rel.columns])
        return "CREATE RELATION {} WITH COLUMNS ({})\n".format(
            create_op.out_rel.dbg_str(),
            col_type_str
        )

    @staticmethod
    def _generate_divide(divide_op: ccdag.Divide):
        """ Generate code for Divide operations. """

        operand_col_str = " / ".join([str(col) for col in divide_op.operands])
        return "DIVIDE{} [{} -> {}] FROM ({}) AS {}\n".format(
            "MPC" if divide_op.is_mpc else "",
            str(divide_op.target_col),
            operand_col_str,
            divide_op.get_in_rel().dbg_str(),
            divide_op.out_rel.dbg_str()
        )

    @staticmethod
    def _generate_multiply(multiply_op: ccdag.Multiply):
        """ Generate code for Multiply operations. """

        operand_col_str = " * ".join([str(col) for col in multiply_op.operands])
        return "MULTIPLY{} [{} -> {}] FROM ({}) AS {}\n".format(
            "MPC" if multiply_op.is_mpc else "",
            str(multiply_op.target_col),
            operand_col_str,
            multiply_op.get_in_rel().dbg_str(),
            multiply_op.out_rel.dbg_str()
        )

    @staticmethod
    def _generate_join(join_op: ccdag.Join):
        """ Generate code for Join operations. """

        return "({}) JOIN{} ({}) ON [{}] AND [{}] AS {}\n".format(
            join_op.get_left_in_rel().dbg_str(),
            "MPC" if join_op.is_mpc else "",
            join_op.get_right_in_rel().dbg_str(),
            ",".join([c.name for c in join_op.left_join_cols]),
            ",".join([c.name for c in join_op.right_join_cols]),
            join_op.out_rel.dbg_str()
        )

    @staticmethod
    def _generate_index_join(index_join_op: ccdag.IndexJoin):
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

    @staticmethod
    def _generate_public_join(reveal_join_op: ccdag.PublicJoin):
        """ Generate code for PublicJoin operations. """

        return "({}) PUBLICJOIN ({}) ON {} AND {} AS {}\n".format(
            reveal_join_op.get_left_in_rel().dbg_str(),
            reveal_join_op.get_right_in_rel().dbg_str(),
            ",".join([c.name for c in reveal_join_op.left_join_cols]),
            ",".join([c.name for c in reveal_join_op.right_join_cols]),
            reveal_join_op.out_rel.dbg_str()
        )

    @staticmethod
    def _generate_hybrid_join(hybrid_join_op: ccdag.HybridJoin):
        """ Generate code for HybridJoin operations. """

        return "({}) HYBRIDJOIN ({}) ON {} AND {} AS {}\n".format(
            hybrid_join_op.get_left_in_rel().dbg_str(),
            hybrid_join_op.get_right_in_rel().dbg_str(),
            ",".join([c.name for c in hybrid_join_op.left_join_cols]),
            ",".join([c.name for c in hybrid_join_op.right_join_cols]),
            hybrid_join_op.out_rel.dbg_str()
        )

    @staticmethod
    def _generate_project(project_op: ccdag.Project):
        """ Generate code for Project operations. """

        selected_cols_str = ", ".join([str(col) for col in project_op.selected_cols])
        return "PROJECT{} [{}] FROM ({}) AS {}\n".format(
            "MPC" if project_op.is_mpc else "",
            selected_cols_str,
            project_op.get_in_rel().dbg_str(),
            project_op.out_rel.dbg_str()
        )

    @staticmethod
    def _generate_distinct(distinct_op: ccdag.Distinct):
        """ Generate code for Distinct operations. """

        selected_cols_str = ", ".join([str(col) for col in distinct_op.selected_cols])
        return "DISTINCT{} [{}] FROM ({}) AS {}\n".format(
            "MPC" if distinct_op.is_mpc else "",
            selected_cols_str,
            distinct_op.get_in_rel().dbg_str(),
            distinct_op.out_rel.dbg_str()
        )

    @staticmethod
    def _generate_distinct_count(distinct_count_op: ccdag.DistinctCount):
        """ Generate code for Distinct Count operations. """

        selected_col_str = str(distinct_count_op.selected_col)
        return "DISTINCT_COUNT{}{} [{}] FROM ({}) AS {}\n".format(
            "MPC" if distinct_count_op.is_mpc else "",
            "NO_SORT" if not distinct_count_op.use_sort else "",
            selected_col_str,
            distinct_count_op.get_in_rel().dbg_str(),
            distinct_count_op.out_rel.dbg_str()
        )

    @staticmethod
    def _generate_sort_by(sort_by_op: ccdag.SortBy):
        """ Generate code for Sort By operations. """

        return "SORTBY{} {} FROM ({}) AS {}\n".format(
            "MPC" if sort_by_op.is_mpc else "",
            sort_by_op.sort_by_col.name,
            sort_by_op.get_in_rel().dbg_str(),
            sort_by_op.out_rel.dbg_str()
        )

    @staticmethod
    def _generate_comp_neighs(comp_neighs_op: ccdag.CompNeighs):
        """ Generate code for Comp Cols operations. """

        return "COMPNEIGHS{} {} FROM ({}) AS {}\n".format(
            "MPC" if comp_neighs_op.is_mpc else "",
            comp_neighs_op.comp_col.name,
            comp_neighs_op.get_in_rel().dbg_str(),
            comp_neighs_op.out_rel.dbg_str()
        )

    @staticmethod
    def _generate_filter(filter_op: ccdag.Filter):
        """ Generate code for Filer operations. """
        filter_str = "{} {} {}".format(filter_op.filter_col.dbg_str(),
                                       filter_op.operator,
                                       filter_op.scalar if filter_op.is_scalar else filter_op.other_col.dbg_str())
        return "FILTER{} [{}] FROM ({}) AS {}\n".format(
            "MPC" if filter_op.is_mpc else "",
            filter_str,
            filter_op.get_in_rel().dbg_str(),
            filter_op.out_rel.dbg_str()
        )

    @staticmethod
    def _generate_filter_by(filter_by_op: ccdag.FilterBy):
        """ Generate code for FilterBy operations. """

        return "FILTER_BY{} [{}] FROM ({}){}IN {} AS {}\n".format(
            "MPC" if filter_by_op.is_mpc else "",
            filter_by_op.filter_col.name,
            filter_by_op.get_left_in_rel().dbg_str(),
            " NOT " if filter_by_op.use_not_in else " ",
            filter_by_op.get_right_in_rel().dbg_str(),
            filter_by_op.out_rel.dbg_str()
        )

    @staticmethod
    def _generate_indexes_to_flags(indexes_to_flags_op: ccdag.IndexesToFlags):
        """ Generate code for IndexesToFlags operations. """

        return "IDX_TO_FLAGS{} {} {} AS {}\n".format(
            "MPC" if indexes_to_flags_op.is_mpc else "",
            indexes_to_flags_op.get_left_in_rel().dbg_str(),
            indexes_to_flags_op.get_right_in_rel().dbg_str(),
            indexes_to_flags_op.out_rel.dbg_str()
        )

    @staticmethod
    def _generate_num_rows(num_rows_op: ccdag.NumRows):
        """ Generate code for NumRows operations. """

        return "NUM_ROWS{} {} AS {}\n".format(
            "MPC" if num_rows_op.is_mpc else "",
            num_rows_op.get_in_rel().dbg_str(),
            num_rows_op.out_rel.dbg_str()
        )

    @staticmethod
    def _generate_union(union_op: ccdag.Union):
        """ Generate code for Union operations. """

        return "{} UNION{} {} ON {} {} AS {}\n".format(
            union_op.get_left_in_rel().dbg_str(),
            "MPC" if union_op.is_mpc else "",
            union_op.get_right_in_rel().dbg_str(),
            union_op.left_col.name,
            union_op.right_col.name,
            union_op.out_rel.dbg_str()
        )

    @staticmethod
    def _generate_shuffle(shuffle_op: ccdag.Shuffle):
        """ Generate code for Shuffle operations. """

        return "SHUFFLE{} ({}) AS {}\n".format(
            "MPC" if shuffle_op.is_mpc else "",
            shuffle_op.get_in_rel().dbg_str(),
            shuffle_op.out_rel.dbg_str()
        )

    @staticmethod
    def _generate_index(index_op: ccdag.Index):
        """ Generate code for Index operations. """

        return "INDEX{} ({}) AS {}\n".format(
            "MPC" if index_op.is_mpc else "",
            index_op.get_in_rel().dbg_str(),
            index_op.out_rel.dbg_str()
        )

    @staticmethod
    def _generate_close(close_op: ccdag.Close):
        """ Generate code for Close operations. """

        return "CLOSE{} {} INTO {}\n".format(
            "MPC" if close_op.is_mpc else "",
            close_op.get_in_rel().dbg_str(),
            close_op.out_rel.dbg_str()
        )

    @staticmethod
    def _generate_open(open_op: ccdag.Open):
        """ Generate code for Open operations. """

        return "OPEN{} {} INTO {}\n".format(
            "MPC" if open_op.is_mpc else "",
            open_op.get_in_rel().dbg_str(),
            open_op.out_rel.dbg_str()
        )

    @staticmethod
    def _generate_persist(persist_op: ccdag.Persist):
        """ Generate code for Persist operations. """

        return "PERSIST{} {} INTO {}\n".format(
            "MPC" if persist_op.is_mpc else "",
            persist_op.get_in_rel().dbg_str(),
            persist_op.out_rel.dbg_str()
        )

    @staticmethod
    def _generate_blackbox(blackbox_op: ccdag.Blackbox):
        """ Generate code for Blackbox operations. """

        return "BLACKBOX{}[{}] {}\n".format(
            "MPC" if blackbox_op.is_mpc else "",
            blackbox_op.backend,
            blackbox_op.out_rel.dbg_str()
        )
