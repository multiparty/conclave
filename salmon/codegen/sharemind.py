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
            if isinstance(node, Aggregate):
                prot_op_code += self._generateAggregate(node)
            if isinstance(node, Concat):
                prot_op_code += self._generateConcat(node)
            if isinstance(node, Join):
                prot_op_code += self._generateJoin(node)
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

        topLevelProtTemplate = open(
            "{0}/protocol.tmpl".format(self.template_directory), 'r').read()
        data = {
            "PROTOCOL_CODE": prot_op_code
        }
        expandedProtCode = pystache.render(topLevelProtTemplate, data)

        op_code = {
            "schemas": schemas,
            "input": input_op_code,
            "protocol": expandedProtCode
        }
        # expand top-level job template and return code
        return self._generateJob(job_name, op_code)

    def _generateJob(self, job_name, op_code):

        return op_code

    def _generateAggregate(self, agg_op):

        template = open(
            "{0}/aggregateSum.tmpl".format(self.template_directory), 'r').read()
        data = {
            "TYPE": "xor_uint32",
            "OUT_REL_NAME": agg_op.outRel.name,
            "IN_REL_NAME": agg_op.getInRel().name,
            "KEY_COL_IDX": agg_op.keyCol.idx,
            "AGG_COL_IDX": agg_op.aggCol.idx
        }
        return pystache.render(template, data)

    def _generateConcat(self, concat_op):

        inRels = concat_op.getInRels()
        assert len(inRels) > 1

        # Sharemind only allows us to concatenate two relations
        # so we need to chain calls repeatedly for more
        catTemplate = open(
            "{0}/catExpr.tmpl".format(self.template_directory), 'r').read()

        cats = catTemplate
        for inRel in inRels[:-2]:
            data = {
                "LEFT_REL": inRel.name,
                "RIGHT_REL": catTemplate
            }
            cats = pystache.render(cats, data)
        outer = open(
            "{0}/concatDef.tmpl".format(self.template_directory), 'r').read()
        data = {
            "OUT_REL": concat_op.outRel.name,
            "TYPE": "xor_uint32",
            "CATS": cats
        }
        outer = pystache.render(outer, data)
        data = {
            "LEFT_REL": inRels[-2].name,
            "RIGHT_REL": inRels[-1].name
        }
        return pystache.render(outer, data)

    def _generateCreate(self, create_op):

        raise Exception("Create operator encountered during Sharemind codegen")

    def _generateJoin(self, join_op):

        template = open(
            "{0}/join.tmpl".format(self.template_directory), 'r').read()
        data = {
            "TYPE": "xor_uint32",
            "OUT_REL": join_op.outRel.name,
            "LEFT_IN_REL": join_op.getLeftInRel().name,
            "LEFT_KEY_COL": join_op.leftJoinCol.idx,
            "RIGHT_IN_REL": join_op.getRightInRel().name,
            "RIGHT_KEY_COL": join_op.rightJoinCol.idx
        }
        return pystache.render(template, data)

    def _generateRevealJoin(self, reveal_join_op):

        pass

    def _generateHybridJoin(self, hybrid_join_op):

        pass

    def _generateProject(self, project_op):

        pass

    def _generateMultiply(self, multiply_op):

        pass

    def _generateStore(self, store_op):
        # TODO: there can be two different stores, input and output

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
                    'TYPE': "xor_uint32"  # hard-coded for now
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
                "TYPE": "xor_uint32"
            }
            return pystache.render(template, data)

        return store_op.getInRel().name, _toSchema(store_op), _toCSVImp(store_op), _toProtocol(store_op)
