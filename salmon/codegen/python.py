from salmon.codegen import CodeGen
from salmon.dag import *
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

    def _generateJob(self, job_name, code_directory, op_code):

        template = open("{}/topLevel.tmpl"
                        .format(self.template_directory), 'r').read()
        data = {
            'OP_CODE': op_code
        }

        op_code = pystache.render(template, data)
        return None, op_code

    def _generateCreate(self, create_op):

        return "{}{} = read_rel('{}')\n".format(
            self.space,
            create_op.outRel.name,
            self.config.input_path + "/" + create_op.outRel.name
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

    def _generateIndex(self, index_op):

        return "{}{} = project_indeces({})\n".format(
            self.space,
            index_op.outRel.name,
            index_op.getInRel().name
        )
