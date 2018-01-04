from salmon.codegen import CodeGen
from salmon.dag import *
from salmon.job import PythonJob
import os
import pystache


class PythonCodeGen(CodeGen):
    # Simple Python

    def __init__(self, config, dag, space="    ",
                 template_directory="{}/templates/python".format(os.path.dirname(os.path.realpath(__file__)))):

        super(PythonCodeGen, self).__init__(config, dag)
        self.template_directory = template_directory
        # this belongs inside config
        self.space = space

    def _generateOutputs(self, op_code):

        leaf_nodes = [node for node in self.dag.top_sort() if node.is_leaf()]
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

    def _generateOutput(self, leaf):

        schema_header = ",".join(['"' + col.name + '"' for col in leaf.out_rel.columns])
        return "{}write_rel('{}', '{}.csv', {}, '{}')\n".format(
            self.space,
            self.config.output_path,
            leaf.out_rel.name,
            leaf.out_rel.name,
            schema_header
        )

    def _generateCreate(self, create_op):

        return "{}{} = read_rel('{}')\n".format(
            self.space,
            create_op.out_rel.name,
            self.config.input_path + "/" + create_op.out_rel.name + ".csv"
        )

    def _generateJoin(self, join_op):

        return "{}{}  = join({}, {}, {}, {})\n".format(
            self.space,
            join_op.out_rel.name,
            join_op.get_left_in_rel().name,
            join_op.get_right_in_rel().name,
            join_op.left_join_cols[0].idx,
            join_op.right_join_cols[0].idx
        )

    def _generateProject(self, project_op):

        selected_cols = [col.idx for col in project_op.selected_cols]
        return "{}{} = project({}, {})\n".format(
            self.space,
            project_op.out_rel.name,
            project_op.get_in_rel().name,
            selected_cols
        )

    def _generateDistinct(self, distinct_op):

        selected_cols = [col.idx for col in distinct_op.selected_cols]
        return "{}{} = distinct({}, {})\n".format(
            self.space,
            distinct_op.out_rel.name,
            distinct_op.get_in_rel().name,
            selected_cols
        )

    def _generateSortBy(self, sort_by_op):

        return "{}{} = sort_by({}, {})\n".format(
            self.space,
            sort_by_op.out_rel.name,
            sort_by_op.get_in_rel().name,
            sort_by_op.sort_by_col.idx
        )

    def _generateCompNeighs(self, comp_neighs_op):

        return "{}{} = comp_neighs({}, {})\n".format(
            self.space,
            comp_neighs_op.out_rel.name,
            comp_neighs_op.get_in_rel().name,
            comp_neighs_op.compCol.idx
        )

    def _generateIndex(self, index_op):

        return "{}{} = project_indeces({})\n".format(
            self.space,
            index_op.out_rel.name,
            index_op.get_in_rel().name
        )

    def _generateIndexAggregate(self, index_agg_op):

        # TODO: generalize
        return "{}{} = index_agg({}, {}, {}, {}, lambda x, y: x {} y)\n".format(
            self.space,
            index_agg_op.out_rel.name,
            index_agg_op.get_in_rel().name,
            index_agg_op.agg_col.idx,
            index_agg_op.distKeysOp.out_rel.name,
            index_agg_op.indexOp.out_rel.name,
            index_agg_op.aggregator
        )        

    def _writeCode(self, code, job_name):

        os.makedirs("{}/{}".format(self.config.code_path,
                                   job_name), exist_ok=True)
        # write code to a file
        pyfile = open(
            "{}/{}/workflow.py".format(self.config.code_path, job_name), 'w')
        pyfile.write(code)
