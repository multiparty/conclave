from salmon.codegen import CodeGen
from salmon.dag import *
import os, pystache

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

    def _generateNode(self, name, descr):
        return "{} [style=\"filled\", label=\"{}\"]\n".format(name, descr)

    def _generateJob(self, job_name, op_code):

        edges = self._generateEdges()
        return "digraph {{\n" \
                "node [shape=record, fontsize=10]\n\n" \
                "{}\n" \
                "{}\n" \
                "}}".format(op_code, edges)

    def _generateAggregate(self, agg_op):

        return self._generateNode(
                agg_op.outRel.name,
                "{{ AGG{} | {} }}".format(
                "MPC" if agg_op.isMPC else "",
                agg_op.outRel.name)
            )

    def _generateConcat(self, concat_op):

        inRelStr = ", ".join([inRel.name for inRel in concat_op.getInRels()])

        return self.generateNode(
                concat_op.outRel.name,
                "{{ CONCAT{} | {} }}".format(
                "MPC" if self.isMPC else "",
                concat_op.outRel.name)
            )

    def _generateCreate(self, create_op):

        colTypeStr = ", ".join([col.typeStr for col in create_op.outRel.columns])

        return self._generateNode(
                create_op.outRel.name,
                "{{ CREATE | {} }}".format(
                create_op.outRel.name)
            )

    def _generateDivide(self, div_op):

        return self._generateNode(
                div_op.outRel.name,
                "{{ DIV | {} }}".format(
                div_op.outRel.name)
            )

    def _generateJoin(self, join_op):

        return self._generateNode(
                join_op.outRel.name,
                "{{ JOIN{} | {} ⋈ {} | {} }}".format(
                "MPC" if join_op.isMPC else "",
                join_op.getLeftInRel().name,
                join_op.getRightInRel().name,
                join_op.outRel.name)
            )

    def _generateMultiply(self, mul_op):

        return self._generateNode(
                mul_op.outRel.name,
                "{{ MUL | {} }}".format(
                mul_op.outRel.name)
            )

    def _generateProject(self, project_op):

        selectedColsStr = ", ".join([str(col) for col in project_op.selectedCols])

        return self._generateNode(
                project_op.outRel.name,
                "{{ PROJECT{} | {} }}".format(
                "MPC" if project_op.isMPC else "",
                project_op.outRel.name)
            )

    def _generateStore(self, store_op):

        return self._generateNode(
                store_op.outRel.name,
                "{{ STORE | {} }}".format(
                store_op.outRel.name)
            )

    def _writeCode(self, code, output_directory, job_name):
        # write code to a file
        outfile = open("{}/{}.gv".format(output_directory, job_name), 'w')
        outfile.write(code)
