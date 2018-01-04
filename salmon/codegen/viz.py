from salmon.codegen import CodeGen
from salmon.dag import *
import os, pystache

# helper to extract column list
def _columnList(op):

    return ", <BR/>".join([col.name for col in op.out_rel.columns])


def _nodeDescription(op, kind, inner):

    if inner:
        return "{{ {{ <I>{}</I> | <B>{}</B> }} | {} | {} }}".format(
                op.out_rel.name,
                kind,
                inner,
                _columnList(op))
    else:
        return "{{ {{ <I>{}</I> | <B>{}</B> }} | {} }}".format(
                op.out_rel.name,
                kind,
                _columnList(op))


class VizCodeGen(CodeGen):
    def __init__(self, config, dag,
            template_directory="{}/templates/viz".format(os.path.dirname(os.path.realpath(__file__)))):
        super(VizCodeGen, self).__init__(config, dag)
        self.template_directory = template_directory

    # generate code for the DAG stored
    def _generateEdges(self):
        edges_code = ""

        nodes = self.dag.top_sort()
        for node in nodes:
            for c in node.children:
                edges_code += "{} -> {}\n".format(node.out_rel.name, c.out_rel.name)
        return edges_code

    def _generateNode(self, op, descr):

        if op.is_mpc:
            c = 1
        else:
            c = 2
        return "{} [style=\"filled\", fillcolor=\"/set312/{}\",  \
                label=<{}>]\n".format(op.out_rel.name, c, descr)

    def _generateJob(self, job_name, output_directory, op_code):

        edges = self._generateEdges()
        # no job object here
        return None, "digraph {{\n" \
                "node [shape=record, fontsize=10]\n\n" \
                "{}\n" \
                "{}\n" \
                "}}".format(op_code, edges)

    def _generateAggregate(self, agg_op):

        return self._generateNode(
                agg_op,
                _nodeDescription(agg_op, "AGG",
                    "{}: {}({})".format(
                        agg_op.out_rel.columns[-1].name,
                        agg_op.aggregator,
                        agg_op.agg_col)
                )
            )

    def _generateConcat(self, concat_op):

        inRelStr = ", ".join([in_rel.name for in_rel in concat_op.get_in_rels()])

        return self._generateNode(
                concat_op,
                _nodeDescription(concat_op, "CONCAT", "")
            )

    def _generateCreate(self, create_op):

        colTypeStr = ", ".join([col.type_str for col in create_op.out_rel.columns])

        return self._generateNode(
                create_op,
                _nodeDescription(create_op, "CREATE", "")
            )

    def _generateClose(self, close_op):

        colTypeStr = ", ".join([col.type_str for col in close_op.out_rel.columns])

        return self._generateNode(
                close_op,
                _nodeDescription(close_op, "CLOSE", "")
            )

    def _generateDistinct(self, distinct_op):

        colTypeStr = ", ".join([col.type_str for col in distinct_op.out_rel.columns])

        return self._generateNode(
                distinct_op,
                _nodeDescription(distinct_op, "DISTINCT", "")
            )

    def _generateDivide(self, div_op):

        return self._generateNode(
                div_op,
                _nodeDescription(div_op, "DIV", "{}: {}".format(
                    div_op.target_col.name,
                    " / ".join([str(o) for o in div_op.operands]),
                    ))
            )

    def _generateJoin(self, join_op):

        return self._generateNode(
                join_op,
                _nodeDescription(join_op, "JOIN",
                    "{} ⋈ {} <br />on: {} ⋈ {}" .format(
                        join_op.get_left_in_rel().name,
                        join_op.get_right_in_rel().name,
                        [c.name for c in join_op.left_join_cols],
                        [c.name for c in join_op.right_join_cols])
                )
            )

    def _generateIndex(self, index_op):

        return self._generateNode(
                index_op,
                _nodeDescription(index_op, "INDEX", "")
            )

    def _generateIndexAggregate(self, agg_op):

        return self._generateNode(
                agg_op,
                _nodeDescription(agg_op, "INDEX AGG",
                    "{}: {}({})".format(
                        agg_op.out_rel.columns[-1].name,
                        agg_op.aggregator,
                        agg_op.agg_col)
                )
            )

    def _generateIndexJoin(self, join_op):

        return self._generateNode(
                join_op,
                _nodeDescription(join_op, "INDEX JOIN",
                    "{} ⋈ {} <br />on: {} ⋈ {}" .format(
                        join_op.get_left_in_rel().name,
                        join_op.get_right_in_rel().name,
                        [c.name for c in join_op.left_join_cols],
                        [c.name for c in join_op.right_join_cols])
                )
            )

    def _generateMultiply(self, mul_op):

        return self._generateNode(
                mul_op,
                _nodeDescription(mul_op, "MUL", "{}: {}".format(
                    mul_op.target_col.name,
                    " * ".join([str(o) for o in mul_op.operands]),
                    ))
            )

    def _generateOpen(self, open_op):

        colTypeStr = ", ".join([col.type_str for col in open_op.out_rel.columns])

        return self._generateNode(
                open_op,
                _nodeDescription(open_op, "OPEN", "")
            )

    def _generatePersist(self, persist_op):

        colTypeStr = ", ".join([col.type_str for col in persist_op.out_rel.columns])

        return self._generateNode(
                persist_op,
                _nodeDescription(persist_op, "PERSIST", "")
            )

    def _generateProject(self, project_op):

        selectedColsStr = ", ".join([str(col) for col in project_op.selected_cols])

        return self._generateNode(
                project_op,
                _nodeDescription(project_op, "PROJECT", "")
            )

    def _generateShuffle(self, shuffle_op):

        colTypeStr = ", ".join([col.type_str for col in shuffle_op.out_rel.columns])

        return self._generateNode(
                shuffle_op,
                _nodeDescription(shuffle_op, "SHUFFLE", "")
            )

    def _generateStore(self, store_op):

        return self._generateNode(
                store_op,
                _nodeDescription(store_op, "STORE", "")
            )

    def _writeCode(self, code, job_name):
        # write code to a file
        os.makedirs(self.config.code_path, exist_ok=True)
        outfile = open("{}/{}.gv".format(self.config.code_path, job_name), 'w')
        outfile.write(code)
