from salmon.codegen import CodeGen
from salmon.dag import *
import os, pystache

# helper to extract column list
def _columnList(op):

    return ", <BR/>".join([col.name for col in op.outRel.columns])


def _nodeDescription(op, kind, inner):

    if inner:
        return "{{ {{ <I>{}</I> | <B>{}</B> }} | {} | {} }}".format(
                op.outRel.name,
                kind,
                inner,
                _columnList(op))
    else:
        return "{{ {{ <I>{}</I> | <B>{}</B> }} | {} }}".format(
                op.outRel.name,
                kind,
                _columnList(op))


class VizCodeGen(CodeGen):
    def __init__(self, dag, template_directory="{}/templates/viz".format(os.path.dirname(os.path.realpath(__file__)))):
        super(VizCodeGen, self).__init__(dag)
        self.template_directory = template_directory

    # generate code for the DAG stored
    def _generateEdges(self):
        edges_code = ""

        nodes = self.dag.topSort()
        for node in nodes:
            for c in node.children:
                edges_code += "{} -> {}\n".format(node.outRel.name, c.outRel.name)
        return edges_code

    def _generateNode(self, op, descr):

        if op.isMPC:
            c = 1
        else:
            c = 2
        return "{} [style=\"filled\", fillcolor=\"/set312/{}\",  \
                label=<{}>]\n".format(op.outRel.name, c, descr)

    def _generateJob(self, job_name, op_code):

        edges = self._generateEdges()
        return "digraph {{\n" \
                "node [shape=record, fontsize=10]\n\n" \
                "{}\n" \
                "{}\n" \
                "}}".format(op_code, edges)

    def _generateAggregate(self, agg_op):

        return self._generateNode(
                agg_op,
                _nodeDescription(agg_op, "AGG",
                    "{}: {}({})".format(
                        agg_op.outRel.columns[-1].name,
                        agg_op.aggregator,
                        agg_op.aggCol)
                )
            )

    def _generateConcat(self, concat_op):

        inRelStr = ", ".join([inRel.name for inRel in concat_op.getInRels()])

        return self.generateNode(
                concat_op,
                _nodeDescription(concat_op, "CONCAT", "")
            )

    def _generateCreate(self, create_op):

        colTypeStr = ", ".join([col.typeStr for col in create_op.outRel.columns])

        return self._generateNode(
                create_op,
                _nodeDescription(create_op, "CREATE", "")
            )

    def _generateDivide(self, div_op):

        return self._generateNode(
                div_op,
                _nodeDescription(div_op, "DIV", "{}: {}".format(
                    div_op.targetCol.name,
                    " / ".join([str(o) for o in div_op.operands]),
                    ))
            )

    def _generateJoin(self, join_op):

        return self._generateNode(
                join_op,
                _nodeDescription(join_op, "JOIN",
                    "{} ⋈ {} <br />on: {} ⋈ {}" .format(
                        join_op.getLeftInRel().name,
                        join_op.getRightInRel().name,
                        [c.name for c in join_op.leftJoinCols],
                        [c.name for c in join_op.rightJoinCols])
                )
            )

    def _generateMultiply(self, mul_op):

        return self._generateNode(
                mul_op,
                _nodeDescription(mul_op, "MUL", "{}: {}".format(
                    mul_op.targetCol.name,
                    " * ".join([str(o) for o in mul_op.operands]),
                    ))
            )

    def _generateProject(self, project_op):

        selectedColsStr = ", ".join([str(col) for col in project_op.selectedCols])

        return self._generateNode(
                project_op,
                _nodeDescription(project_op, "PROJECT", "")
            )

    def _generateStore(self, store_op):

        return self._generateNode(
                store_op,
                _nodeDescription(store_op, "STORE", "")
            )

    def _writeCode(self, code, output_directory, job_name):
        # write code to a file
        outfile = open("{}/{}.gv".format(output_directory, job_name), 'w')
        outfile.write(code)
