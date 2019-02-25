import os

import pystache

import conclave.dag as ccdag
from conclave.codegen import CodeGen
from conclave.job import PythonJob
from conclave.rel import Column


class PythonCodeGen(CodeGen):
    """ Codegen subclass for generating Python code. """

    def __init__(self, config, dag: ccdag.Dag, space="    ",
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

    def _generate_concat(self, concat_op: ccdag.Concat):
        """ Generate code for Concat operations. """
        in_rel_str = " + ".join([in_rel.name for in_rel in concat_op.get_in_rels()])
        return "{}{} = {}\n".format(
            self.space,
            concat_op.out_rel.name,
            in_rel_str
        )

    def _generate_aggregate(self, agg_op: ccdag.Aggregate):
        """ Generate code for Aggregate operations. """
        # TODO handle multi-column case
        if agg_op.aggregator == "sum":
            return "{}{} = aggregate({}, {}, {}, '{}')\n".format(
                self.space,
                agg_op.out_rel.name,
                agg_op.get_in_rel().name,
                agg_op.group_cols[0].idx,
                agg_op.agg_col.idx,
                agg_op.aggregator
            )
        elif agg_op.aggregator == "count":
            return "{}{} = aggregate_count({}, {})\n".format(
                self.space,
                agg_op.out_rel.name,
                agg_op.get_in_rel().name,
                agg_op.group_cols[0].idx
            )
        else:
            raise Exception("Unknown aggregator {}".format(agg_op.aggregator))

    @staticmethod
    def _col_or_scalar(col):
        return "row[{}]".format(col.idx) if isinstance(col, Column) else str(col)

    def _generate_multiply(self, mult_op: ccdag.Multiply):
        """ Generate code for Multiply operations. """
        operands = [self._col_or_scalar(col) for col in mult_op.operands]
        lambda_expr = "lambda row : " + " * ".join([op for op in operands])
        return "{}{} = arithmetic_project({}, {}, {})\n".format(
            self.space,
            mult_op.out_rel.name,
            mult_op.get_in_rel().name,
            mult_op.target_col.idx,
            lambda_expr
        )

    def _generate_divide(self, div_op: ccdag.Divide):
        """
        Generate code for Divide operations.
        >>> from conclave.utils import defCol
        >>> import conclave.lang as cc
        >>> from conclave.dag import Dag
        >>> cols_in_left = [defCol("a", "INTEGER", 1), defCol("b", "INTEGER", 1)]
        >>> left = cc.create("left", cols_in_left, {1})
        >>> div = cc.divide(left, "div", "a", ["a", 1, "b"])
        >>> PythonCodeGen(None, Dag({}), "")._generate_divide(div)
        'div = arithmetic_project(left, 0, lambda row : int(row[0] / 1 / row[1]))\\n'
        """
        operands = [self._col_or_scalar(col) for col in div_op.operands]
        lambda_expr = "lambda row : int({})".format(" / ".join([op for op in operands]))
        return "{}{} = arithmetic_project({}, {}, {})\n".format(
            self.space,
            div_op.out_rel.name,
            div_op.get_in_rel().name,
            div_op.target_col.idx,
            lambda_expr
        )

    def _generate_output(self, leaf: ccdag.OpNode):
        """ Generate code for storing a single output. """
        schema_header = ",".join(['"' + col.name + '"' for col in leaf.out_rel.columns])
        return "{}write_rel('{}', '{}.csv', {}, '{}')\n".format(
            self.space,
            self.config.output_path,
            leaf.out_rel.name,
            leaf.out_rel.name,
            schema_header
        )

    def _generate_persist(self, leaf: ccdag.Persist):
        """ Generate code for storing a single output via a Persist op. """
        schema_header = ",".join(['"' + col.name + '"' for col in leaf.out_rel.columns])
        return "{}write_rel('{}', '{}.csv', {}, '{}')\n".format(
            self.space,
            self.config.output_path,
            leaf.out_rel.name,
            leaf.out_rel.name,
            schema_header
        )

    def _generate_create(self, create_op: ccdag.Create):
        """ Generate code for loading input data. """
        return "{}{} = read_rel('{}')\n".format(
            self.space,
            create_op.out_rel.name,
            self.config.input_path + "/" + create_op.out_rel.name + ".csv"
        )

    def _generate_join_flags(self, join_flags_op: ccdag.JoinFlags):
        """ Generate code for JoinFlags operations. """
        return "{}{}  = join_flags({}, {}, {}, {})\n".format(
            self.space,
            join_flags_op.out_rel.name,
            join_flags_op.get_left_in_rel().name,
            join_flags_op.get_right_in_rel().name,
            join_flags_op.left_join_cols[0].idx,
            join_flags_op.right_join_cols[0].idx
        )

    def _generate_join(self, join_op: ccdag.Join):
        """ Generate code for Join operations. """
        return "{}{}  = join({}, {}, {}, {})\n".format(
            self.space,
            join_op.out_rel.name,
            join_op.get_left_in_rel().name,
            join_op.get_right_in_rel().name,
            join_op.left_join_cols[0].idx,
            join_op.right_join_cols[0].idx
        )

    def _generate_project(self, project_op: ccdag.Project):
        """ Generate code for Project operations. """
        selected_cols = [col.idx for col in project_op.selected_cols]
        return "{}{} = project({}, {})\n".format(
            self.space,
            project_op.out_rel.name,
            project_op.get_in_rel().name,
            selected_cols
        )

    def _generate_filter(self, filter_op: ccdag.Filter):
        """ Generate code for Filter operations. """
        cond_lambda = "lambda row : row[{}] {} {}".format(
            filter_op.filter_col.idx,
            filter_op.operator,
            filter_op.scalar if filter_op.is_scalar else "row[{}]".format(filter_op.other_col.idx)
        )
        return "{}{} = cc_filter({}, {})\n".format(
            self.space,
            filter_op.out_rel.name,
            cond_lambda,
            filter_op.get_in_rel().name
        )

    def _generate_distinct(self, distinct_op: ccdag.Distinct):
        """ Generate code for Distinct operations. """
        selected_cols = [col.idx for col in distinct_op.selected_cols]
        return "{}{} = distinct({}, {})\n".format(
            self.space,
            distinct_op.out_rel.name,
            distinct_op.get_in_rel().name,
            selected_cols
        )

    def _generate_distinct_count(self, distinct_count_op: ccdag.DistinctCount):
        """ Generate code for Distinct Count operations. """
        return "{}{} = distinct_count({}, {})\n".format(
            self.space,
            distinct_count_op.out_rel.name,
            distinct_count_op.get_in_rel().name,
            distinct_count_op.selected_col.idx
        )

    def _generate_pub_join(self, pub_join_op: ccdag.PubJoin):
        """ Generate code for Pub Join operations. """
        if pub_join_op.right_parent is None:
            return "{}{} = pub_join(\"{}\", {}, {}, {}, {})\n".format(
                self.space,
                pub_join_op.out_rel.name,
                pub_join_op.host,
                pub_join_op.port,
                "True" if pub_join_op.is_server else "False",
                pub_join_op.get_left_in_rel().name,
                pub_join_op.key_col.idx
            )
        else:
            return "{}{} = pub_join_part(\"{}\", {}, {}, {}, {}, {}, {}, {})\n".format(
                self.space,
                pub_join_op.out_rel.name,
                pub_join_op.host,
                pub_join_op.port,
                "True" if pub_join_op.is_server else "False",
                pub_join_op.get_left_in_rel().name,
                pub_join_op.get_right_in_rel().name,
                pub_join_op.key_col.idx,
                len(pub_join_op.get_left_in_rel().columns),
                len(pub_join_op.get_right_in_rel().columns)
            )

    def _generate_pub_intersect(self, pub_intersect_op: ccdag.PubIntersect):
        """ Generate code for PubIntersect operations. """
        return "{}{} = pub_intersect_{}(\"{}\", {}, {}, {})\n".format(
            self.space,
            pub_intersect_op.out_rel.name,
            "as_server" if pub_intersect_op.is_server else "as_client",
            pub_intersect_op.host,
            pub_intersect_op.port,
            pub_intersect_op.get_in_rel().name,
            pub_intersect_op.col.idx
        )

    def _generate_sort_by(self, sort_by_op: ccdag.SortBy):
        """ Generate code for SortBy operations. """
        return "{}{} = sort_by({}, {})\n".format(
            self.space,
            sort_by_op.out_rel.name,
            sort_by_op.get_in_rel().name,
            sort_by_op.sort_by_col.idx
        )

    def _generate_filter_by(self, filter_by_op: ccdag.FilterBy):
        """ Generate code for FilterBy operations. """
        return "{}{} = filter_by({}, {}, {}, {})\n".format(
            self.space,
            filter_by_op.out_rel.name,
            filter_by_op.get_left_in_rel().name,
            filter_by_op.get_right_in_rel().name,
            filter_by_op.filter_col.idx,
            "True" if filter_by_op.use_not_in else "False",
        )

    def _generate_indexes_to_flags(self, indexes_to_flags_op: ccdag.IndexesToFlags):
        """ Generate code for IndexesToFlags operations. """
        stage = indexes_to_flags_op.stage
        if stage == 0:
            return "{}{} = indexes_to_flags({}, len({}))\n".format(
                self.space,
                indexes_to_flags_op.out_rel.name,
                indexes_to_flags_op.get_right_in_rel().name,
                indexes_to_flags_op.get_left_in_rel().name
            )
        elif stage == 1:
            return "{}{} = arrange_by_flags({}, {})\n".format(
                self.space,
                indexes_to_flags_op.out_rel.name,
                indexes_to_flags_op.get_left_in_rel().name,
                indexes_to_flags_op.get_right_in_rel().name
            )
        else:
            raise Exception("Unknown stage " + stage)

    def _generate_num_rows(self, num_rows_op: ccdag.NumRows):
        """ Generate code for NumRows operations. """
        return "{}{} = [[len({})]]\n".format(
            self.space,
            num_rows_op.out_rel.name,
            num_rows_op.get_in_rel().name
        )

    def _generate_union(self, union_op: ccdag.Union):
        """ Generate code for FilterBy operations. """
        return "{}{} = key_union_as_rel({}, {}, {}, {})\n".format(
            self.space,
            union_op.out_rel.name,
            union_op.get_left_in_rel().name,
            union_op.get_right_in_rel().name,
            union_op.left_col.idx,
            union_op.right_col.idx
        )

    def _generate_comp_neighs(self, comp_neighs_op: ccdag.CompNeighs):
        """ Generate code for CompNeighs operations. """
        return "{}{} = comp_neighs({}, {})\n".format(
            self.space,
            comp_neighs_op.out_rel.name,
            comp_neighs_op.get_in_rel().name,
            comp_neighs_op.comp_col.idx
        )

    def _generate_index(self, index_op: ccdag.Index):
        """ Generate code for Index operations. """
        return "{}{} = project_indeces({})\n".format(
            self.space,
            index_op.out_rel.name,
            index_op.get_in_rel().name
        )

    def _generate_index_aggregate(self, index_agg_op: ccdag.IndexAggregate):
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

    def _generate_blackbox(self, blackbox_op: ccdag.Blackbox):
        raise Exception("Blackbox ops not supported")

    def _write_code(self, code: str, job_name: str):
        os.makedirs("{}/{}".format(self.config.code_path,
                                   job_name), exist_ok=True)
        # write code to a file
        pyfile = open(
            "{}/{}/workflow.py".format(self.config.code_path, job_name), 'w')
        pyfile.write(code)
