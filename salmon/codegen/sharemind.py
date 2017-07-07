from salmon.codegen import CodeGen
from salmon.dag import *


class SharemindCodeGen(CodeGen):

    def __init__(self, dag):

        super(SharemindCodeGen, self).__init__(dag)

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

            return "foo"

        def _toCSVImp(store_op):

            return "bar"

        def _toProtocol(store_op):

            return "baz"

        return store_op.getInRel().name, _toSchema(store_op), _toCSVImp(store_op), _toProtocol(store_op)
