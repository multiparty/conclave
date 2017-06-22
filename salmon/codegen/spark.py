from salmon.codegen import CodeGen
from salmon.dag import *
import os, pystache


def op_to_sum(op):
    if op == "+":
        return "sum"


def split_datatypes(cols):
    integers = []
    strings = []
    floats = []

    for i in range(len(cols)):
        if cols[i].typeStr == "INTEGER":
            integers.append(i)
        elif cols[i].typeStr == "STRING":
            strings.append(i)
        elif cols[i].typeStr == "FLOAT":
            floats.append(i)
        else:
            print("Unknown datatype: {0}".format(cols[i].typeStr))

    return [integers, strings, floats]


class SparkCodeGen(CodeGen):
    def __init__(self, dag, template_directory="{}/templates/spark".format(os.path.dirname(os.path.realpath(__file__)))):
        super(SparkCodeGen, self).__init__(dag)
        self.template_directory = template_directory

    def _generateJob(self, job_name, op_code):

        template = open("{}/job.tmpl".format(self.template_directory), 'r').read()
        data = { 'JOB_NAME': job_name,
                 'SPARK_MASTER': 'local',  # XXX(malte): make configurable
                 'OP_CODE': op_code }

        return pystache.render(template, data)

    def _generateAggregate(self, agg_op):

        keyCol, aggCol, aggregator = \
            agg_op.keyCol, agg_op.aggCol, agg_op.aggregator

        agg_type = 'agg_' + op_to_sum(aggregator)

        template = open("{0}/{1}.tmpl".format(self.template_directory, agg_type), 'r').read()

        data = {
            'KEYCOL_ID': keyCol.idx,
            'AGGCOL_ID': aggCol.idx,
            'INREL': agg_op.getInRel().name,
            'OUTREL': agg_op.outRel.name

        }

        return pystache.render(template, data)

    def _generateConcat(self, concat_op):

        in_rels = concat_op.getInRels()

        template = open("{0}/{1}.tmpl".format(self.template_directory, 'concat'), 'r').read()

        data = {
            'INRELS': [rel.name for rel in in_rels],
            'OUTREL': concat_op.outRel.name
        }

        return pystache.render(template, data)

    def _generateCreate(self, create_op):

        template = open("{}/create.tmpl".format(self.template_directory), 'r').read()

        cols = create_op.outRel.columns

        data = {
                'RELATION_NAME': create_op.outRel.name,
                'INPUT_PATH': "/tmp",  # XXX(malte): make configurable
               }

        return pystache.render(template, data)

    def _generateJoin(self, join_op):

        leftInRel = join_op.leftParent
        rightInRel = join_op.rightParent

        leftJoinCol, rightJoinCol = join_op.leftJoinCol, join_op.rightJoinCol

        template = open("{0}/{1}.tmpl".format(self.template_directory, 'join'), 'r').read()

        data = {
            'LEFT_PARENT': leftInRel.name,
            'RIGHT_PARENT': rightInRel.name,
            'LEFT_COL': leftJoinCol,
            'RIGHT_COL': rightJoinCol,
            'OUTREL': join_op.outRel.name
        }

        return pystache.render(template, data)

    def _generateProject(self, project_op):

        inRel = project_op.getInRel()
        cols = inRel.columns

        template = open("{0}/{1}.tmpl".format(self.template_directory, 'project'), 'r').read()

        data = {
            'COL_IDS': [c.idx for c in cols],
            'INREL': project_op.getInRel().name,
            'OUTREL': project_op.outRel.name
        }
        return pystache.render(template, data)

    def _generateMultiply(self, mult_op):

        op_cols = mult_op.operands

        template = open("{0}/{1}.tmpl".format(self.template_directory, 'multiply'), 'r').read()

        data = {
            'OPERAND_IDS': [c.idx for c in op_cols],
            'TARGET_ID': mult_op.targetCol.idx,
            'INREL': mult_op.getInRel().name,
            'OUTREL': mult_op.outRel.name
        }

        return pystache.render(template, data)

    def _generateStore(self, store_op):

        template = open("{}/store.tmpl".format(self.template_directory), 'r').read()
        data = {
                'RELATION_NAME': store_op.outRel.name,
                'OUTPUT_PATH': "/tmp"  # XXX(malte): make configurable
               }

        return pystache.render(template, data)

    def _writeCode(self, code, output_directory, job_name):
        # write code to a file
        outfile = open("{}/{}.py".format(output_directory, job_name), 'w')
        outfile.write(code)
