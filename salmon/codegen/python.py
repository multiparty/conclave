import os

import pystache

from salmon.codegen import CodeGen
from salmon.job import PythonJob


class PythonCodeGen(CodeGen):
    # Simple Python

    def __init__(self, config, dag, space="    ",
                 template_directory="{}/templates/python".format(os.path.dirname(os.path.realpath(__file__)))):
        super(PythonCodeGen, self).__init__(config, dag)
        self.template_directory = template_directory
        # this belongs inside config
        self.space = space

    def _generateOutputs(self, op_code):
        leaf_nodes = [node for node in self.dag.topSort() if node.isLeaf()]
        for leaf in leaf_nodes:
            op_code += self._generateOutput(leaf)
        return op_code

    def _generateJob(self, job_name, code_directory, op_code):
        op_code = self._generateOutputs(op_code)
        template = open("{}/topLevel.tmpl"
                        .format(self.template_directory), 'r').read()
        data = {
            'OP_CODE': op_code
        }

        op_code = pystache.render(template, data)
        job = PythonJob(job_name, "{}/{}".format(code_directory, job_name))
        return job, op_code

    def _generateAggregate(self, agg_op):
        # TODO handle multi-column case
        return "{}{} = aggregate({}, {}, {}, '{}')\n".format(
            self.space,
            agg_op.outRel.name,
            agg_op.getInRel().name,
            agg_op.groupCols[0].idx,
            agg_op.aggCol.idx,
            agg_op.aggregator
        )

    def _generateMultiply(self, mult_op):
        operands = [col.idx for col in mult_op.operands]
        lambda_expr = "lambda row : " + " * ".join(["row[{}]".format(idx) for idx in operands])
        return "{}{} = arithmetic_project({}, {}, {})\n".format(
            self.space,
            mult_op.outRel.name,
            mult_op.getInRel().name,
            mult_op.targetCol.idx,
            lambda_expr
        )

    def _generateOutput(self, leaf):
        schema_header = ",".join(['"' + col.name + '"' for col in leaf.outRel.columns])
        return "{}write_rel('{}', '{}.csv', {}, '{}')\n".format(
            self.space,
            self.config.output_path,
            leaf.outRel.name,
            leaf.outRel.name,
            schema_header
        )

    def _generateCreate(self, create_op):
        return "{}{} = read_rel('{}')\n".format(
            self.space,
            create_op.outRel.name,
            self.config.input_path + "/" + create_op.outRel.name + ".csv"
        )

    def _generateJoin(self, join_op):
        return "{}{}  = join({}, {}, {}, {})\n".format(
            self.space,
            join_op.outRel.name,
            join_op.getLeftInRel().name,
            join_op.getRightInRel().name,
            join_op.leftJoinCols[0].idx,
            join_op.rightJoinCols[0].idx
        )

    def _generateProject(self, project_op):
        selected_cols = [col.idx for col in project_op.selectedCols]
        return "{}{} = project({}, {})\n".format(
            self.space,
            project_op.outRel.name,
            project_op.getInRel().name,
            selected_cols
        )

    def _generateDistinct(self, distinct_op):
        selected_cols = [col.idx for col in distinct_op.selectedCols]
        return "{}{} = distinct({}, {})\n".format(
            self.space,
            distinct_op.outRel.name,
            distinct_op.getInRel().name,
            selected_cols
        )

    def _generateSortBy(self, sort_by_op):
        return "{}{} = sort_by({}, {})\n".format(
            self.space,
            sort_by_op.outRel.name,
            sort_by_op.getInRel().name,
            sort_by_op.sortByCol.idx
        )

    def _generateCompNeighs(self, comp_neighs_op):
        return "{}{} = comp_neighs({}, {})\n".format(
            self.space,
            comp_neighs_op.outRel.name,
            comp_neighs_op.getInRel().name,
            comp_neighs_op.compCol.idx
        )

    def _generateIndex(self, index_op):
        return "{}{} = project_indeces({})\n".format(
            self.space,
            index_op.outRel.name,
            index_op.getInRel().name
        )

    def _generateIndexAggregate(self, index_agg_op):
        # TODO: generalize
        return "{}{} = index_agg({}, {}, {}, {}, lambda x, y: x {} y)\n".format(
            self.space,
            index_agg_op.outRel.name,
            index_agg_op.getInRel().name,
            index_agg_op.aggCol.idx,
            index_agg_op.distKeysOp.outRel.name,
            index_agg_op.indexOp.outRel.name,
            index_agg_op.aggregator
        )

    def _writeCode(self, code, job_name):
        os.makedirs("{}/{}".format(self.config.code_path,
                                   job_name), exist_ok=True)
        # write code to a file
        pyfile = open(
            "{}/{}/workflow.py".format(self.config.code_path, job_name), 'w')
        pyfile.write(code)
