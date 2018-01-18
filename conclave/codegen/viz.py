from conclave.codegen import CodeGen
import conclave.dag as saldag
import os


# helper to extract column list
def _column_list(op: saldag.OpNode):
    """ Returns string that lists column names in output relation. """

    return ", <BR/>".join([col.name for col in op.out_rel.columns])


def _node_description(op: saldag.OpNode, kind: str, inner):
    """ Returns description of node properties. """

    if inner:
        return "{{ {{ <I>{}</I> | <B>{}</B> }} | {} | {} }}".format(
                op.out_rel.name,
                kind,
                inner,
                _column_list(op))
    else:
        return "{{ {{ <I>{}</I> | <B>{}</B> }} | {} }}".format(
                op.out_rel.name,
                kind,
                _column_list(op))


class VizCodeGen(CodeGen):
    """ Codegen subclass for generating code used to visualize Conclave workflows. """

    def __init__(self, config, dag: saldag.Dag,
            template_directory="{}/templates/viz".format(os.path.dirname(os.path.realpath(__file__)))):
        """ Initialize VizCodeGen object. """

        super(VizCodeGen, self).__init__(config, dag)
        self.template_directory = template_directory

    def _generate_edges(self):
        """ Generate code for DAG passed. """

        edges_code = ""

        nodes = self.dag.top_sort()
        for node in nodes:
            for c in node.children:
                edges_code += "{} -> {}\n".format(node.out_rel.name, c.out_rel.name)
        return edges_code

    def _generate_node(self, op: saldag.OpNode, descr: str):

        if op.is_mpc:
            c = 1
        else:
            c = 2
        return "{} [style=\"filled\", fillcolor=\"/set312/{}\", label=<{}>]\n"\
            .format(op.out_rel.name, c, descr)

    def _generate_job(self, job_name, output_directory, op_code: str):
        """ No job object here, just generates graph. """

        edges = self._generate_edges()

        return None, "digraph {{\n" \
                     "node [shape=record, fontsize=10]\n\n" \
                     "{}\n" \
                     "{}\n" \
                     "}}".format(op_code, edges)

    def _generate_aggregate(self, agg_op: saldag.Aggregate):
        """ Generate code for Aggregate operations. """

        return self._generate_node(
                agg_op,
                _node_description(
                    agg_op, "AGG", "{}: {}({})".format(
                                      agg_op.out_rel.columns[-1].name,
                                      agg_op.aggregator,
                                      agg_op.agg_col)
                                  )
        )

    def _generate_concat(self, concat_op: saldag.Concat):
        """ Generate code for Concat operations. """

        return self._generate_node(
                concat_op,
                _node_description(concat_op, "CONCAT", "")
            )

    def _generate_create(self, create_op: saldag.Create):
        """ Generate code for Create operations. """

        return self._generate_node(
                create_op,
                _node_description(create_op, "CREATE", "")
            )

    def _generate_close(self, close_op: saldag.Close):
        """ Generate code for Close operations. """

        return self._generate_node(
                close_op,
                _node_description(close_op, "CLOSE", "")
            )

    def _generate_distinct(self, distinct_op: saldag.Distinct):
        """ Generate code for Distinct operations. """

        return self._generate_node(
                distinct_op,
                _node_description(distinct_op, "DISTINCT", "")
            )

    def _generate_divide(self, div_op: saldag.Divide):
        """ Generate code for Divide operations. """

        return self._generate_node(
                div_op,
                _node_description(
                    div_op,
                    "DIV", "{}: {}".format(
                        div_op.target_col.name,
                        " / ".join([str(o) for o in div_op.operands]),
                    )
                )
            )

    def _generate_join(self, join_op: saldag.Join):
        """ Generate code for Join operations. """

        return self._generate_node(
                join_op,
                _node_description(
                    join_op, "JOIN", "{} ⋈ {} <br />on: {} ⋈ {}" .format(
                        join_op.get_left_in_rel().name,
                        join_op.get_right_in_rel().name,
                        [c.name for c in join_op.left_join_cols],
                        [c.name for c in join_op.right_join_cols])
                                  )
            )

    def _generate_index(self, index_op: saldag.Index):
        """ Generate code for Index operations. """

        return self._generate_node(
                index_op,
                _node_description(index_op, "INDEX", "")
            )

    def _generate_index_aggregate(self, agg_op: saldag.IndexAggregate):
        """ Generate code for Index Aggregate operations."""

        return self._generate_node(
                agg_op,
                _node_description(
                    agg_op, "INDEX AGG", "{}: {}({})".format(
                        agg_op.out_rel.columns[-1].name,
                        agg_op.aggregator,
                        agg_op.agg_col)
                                  )
            )

    def _generate_index_join(self, join_op: saldag.IndexJoin):
        """ Generate code for Index Join operations. """

        return self._generate_node(
                join_op,
                _node_description(
                    join_op, "INDEX JOIN", "{} ⋈ {} <br />on: {} ⋈ {}" .format(
                        join_op.get_left_in_rel().name,
                        join_op.get_right_in_rel().name,
                        [c.name for c in join_op.left_join_cols],
                        [c.name for c in join_op.right_join_cols])
                                  )
            )

    def _generate_multiply(self, mul_op: saldag.Multiply):
        """ Generate code for Multiply operations. """

        return self._generate_node(
                mul_op,
                _node_description(
                    mul_op, "MUL", "{}: {}".format(
                        mul_op.target_col.name, " * ".join([str(o) for o in mul_op.operands]),
                    )
                )
            )

    def _generateOpen(self, open_op: saldag.Open):
        """ Generate code for Open operations. """

        return self._generate_node(
                open_op,
                _node_description(open_op, "OPEN", "")
            )

    def _generate_persist(self, persist_op: saldag.Persist):
        """ Generate code for Persist operations. """

        return self._generate_node(
                persist_op,
                _node_description(persist_op, "PERSIST", "")
            )

    def _generate_project(self, project_op: saldag.Project):
        """ Generate code for Project operations. """

        return self._generate_node(
                project_op,
                _node_description(project_op, "PROJECT", "")
            )

    def _generate_shuffle(self, shuffle_op: saldag.Shuffle):
        """ Generate code for Shuffle operations. """

        return self._generate_node(
                shuffle_op,
                _node_description(shuffle_op, "SHUFFLE", "")
            )

    def _generate_store(self, store_op: saldag.Store):
        """ Generate code for Store operations. """

        return self._generate_node(
                store_op,
                _node_description(store_op, "STORE", "")
            )

    def _write_code(self, code: str, job_name: str):
        """ Write code to file. """

        os.makedirs(self.config.code_path, exist_ok=True)
        outfile = open("{}/{}.gv".format(self.config.code_path, job_name), 'w')
        outfile.write(code)
