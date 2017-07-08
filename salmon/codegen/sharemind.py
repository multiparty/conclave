from salmon.codegen import CodeGen
from salmon.dag import *
import os
import pystache


class SharemindCodeGen(CodeGen):

    def __init__(self, dag, template_directory="{}/templates/sharemind".format(os.path.dirname(os.path.realpath(__file__)))):

        super(SharemindCodeGen, self).__init__(dag)
        self.template_directory = template_directory

    def _generate(self, job_name, output_directory):
        # generate code for the DAG stored. overwriting
        # method since we have multiple code objects being
        # create, instead of one

        # the data model definitions used by CSVImporter
        # there will be one per store operation
        schemas = {}
        # the code the parties will run to secret-share their
        # inputs with the miners
        input_op_code = ""
        # the code the miners will run after the data has been
        # secret shared
        prot_op_code = ""

        # topological traversal
        nodes = self.dag.topSort()
        # for each op
        for node in nodes:
            if isinstance(node, Store):
                # the store operation adds to the input task since we
                # need to secret share, as well as to the protocol task
                schemaName, schema, in_code, prot_code = self._generateStore(
                    node)
                schemas[schemaName] = schema
                input_op_code += in_code
                prot_op_code += prot_code
            else:
                print("encountered unknown operator type", repr(node))

        op_code = {
            "schemas": schemas,
            "input_op_code": input_op_code,
            "prot_op_code": prot_op_code
        }
        # expand top-level job template and return code
        return self._generateJob(job_name, op_code)

    def _generateJob(self, job_name, op_code):

        return op_code

    def _generateAggregate(self, agg_op):

        pass

    def _generateConcat(self, concat_op):

        pass

    def _generateCreate(self, create_op):

        pass

    def _generateJoin(self, join_op):

        pass

    def _generateRevealJoin(self, reveal_join_op):

        pass

    def _generateHybridJoin(self, hybrid_join_op):

        pass

    def _generateProject(self, project_op):

        pass

    def _generateMultiply(self, multiply_op):

        pass

    def _generateStore(self, store_op):

        def _toSchema(store_op):

            inRel = store_op.getInRel()
            inCols = inRel.columns
            outRel = store_op.outRel
            outCols = outRel.columns
            colDefs = []
            colDefTemplate = open(
                "{0}/colDef.tmpl".format(self.template_directory), 'r').read()
            for inCol, outCol in zip(inCols, outCols):
                colData = {
                    'IN_NAME': inCol.getName(),
                    'OUT_NAME': outCol.getName(),
                    'TYPE': "uint32"  # hard-coded for now
                }
                colDefs.append(pystache.render(colDefTemplate, colData))
            colDefStr = "\n".join(colDefs)
            relDefTemplate = open(
                "{0}/relDef.tmpl".format(self.template_directory), 'r').read()
            relData = {
                "NAME": outRel.name,
                "COL_DEFS": colDefStr
            }
            relDefStr = pystache.render(relDefTemplate, relData)
            return relDefStr

        def _toCSVImp(store_op):

            template = open(
                "{0}/csvImport.tmpl".format(self.template_directory), 'r').read()
            data = {
                "IN_NAME": store_op.getInRel().name
            }
            return pystache.render(template, data)

        def _toProtocol(store_op):

            template = open(
                "{0}/readFromDb.tmpl".format(self.template_directory), 'r').read()
            data = {
                "NAME": store_op.outRel.name,
                "TYPE": "uint32"
            }
            return pystache.render(template, data)

        return store_op.getInRel().name, _toSchema(store_op), _toCSVImp(store_op), _toProtocol(store_op)
