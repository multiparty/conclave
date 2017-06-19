from salmon.codegen import CodeGen
from salmon.dag import *
import os, pystache


def op_to_sum(op):
    if op == "+":
        return "sum"

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
        cols = [inrel.columns for inrel in in_rels]

        template = open("{0}/{1}.tmpl".format(self.template_directory, 'concat'), 'r').read()

        data = {
            'COL_IDS': [c.idx for c in cols],
            'INREL': concat_op.getInRel().name,
            'OUTREL': concat_op.outRel.name
        }

        return pystache.render(template, data)

    def _generateCreate(self, create_op):

        template = open("{}/create.tmpl".format(self.template_directory), 'r').read()

        data = {
                'RELATION_NAME': create_op.outRel.name,
                'INPUT_PATH': "/tmp"  # XXX(malte): make configurable
               }

        return pystache.render(template, data)

    def _generateJoin(self, join_op):
        return ""

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
