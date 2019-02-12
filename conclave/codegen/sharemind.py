import os
import sys

import pystache

from conclave.codegen import CodeGen
from conclave.dag import *
from conclave.job import SharemindJob
from conclave.rel import *


class SharemindCodeGen(CodeGen):
    """ Codegen subclass for generating Sharemind code. """

    def __init__(self, config, dag: Dag, pid: int,
                 template_directory="{}/templates/sharemind".format(os.path.dirname(os.path.realpath(__file__)))):

        if not "sharemind" in config.system_configs:
            print("Missing Sharemind configuration in CodeGenConfig!")
            sys.exit(1)
        self.sm_config = config.system_configs['sharemind']

        super(SharemindCodeGen, self).__init__(config, dag)
        self.template_directory = template_directory
        self.pid = pid

    def generate(self, job_name: str, output_directory: str):
        """ Generate code for DAG passed and write to file. """

        job, code = self._generate(job_name, self.config.code_path)
        # store the code in type-specific files
        self._write_code(code, job_name)
        # return job object
        return job

    def _generate(self, job_name: str, output_directory: str):
        """ Generate Sharemind code for DAG passed to CodeGen"""

        # nodes in topological order
        nodes = self.dag.top_sort()

        # determine party that will act as the controller
        controller_pid = self._get_controller_pid(nodes)
        # determine all input parties
        input_parties = self._get_input_parties(nodes)

        # dict of all generated code
        op_code = {}

        # generate input code and schemas for input
        if self.pid in input_parties:
            # the data model definitions used by CSVImporter
            # and the code the party will run to secret-share its
            # inputs with the miners
            schemas, input_code = self._generate_input_code(
                nodes, job_name, self.config.code_path)
            op_code["schemas"] = schemas
            op_code["input"] = input_code

        # if we are the controller we need to generate miner
        # code and controller code
        if self.pid == controller_pid:
            # the code the miners will run after the data has been
            # secret shared
            miner_code = self._generate_miner_code(nodes)
            # code to submit the job and receive the output
            # (currently assumes there is only one output party)
            submit_code = self._generate_controller_code(
                nodes, job_name, self.config.code_path)
            op_code["miner"] = miner_code
            op_code["submit"] = submit_code["outer"]
            op_code["submitInner"] = submit_code["inner"]

        # create job
        job = SharemindJob(job_name, self.config.code_path + "/" + job_name,
                           controller_pid, input_parties)
        # check if this party participates in any way
        # if not op_code:
        #     job.skip = True
        return job, op_code

    def _generate_miner_code(self, nodes: list):
        """ Generate code that will run on Sharemind miners. """

        # TODO: this code should be re-using base class _generate method
        # the code that will run on the miners
        # TODO: handle subclassing more gracefully
        miner_code = ""
        for node in nodes:
            if isinstance(node, LeakyIndexAggregate):
                miner_code += self._generate_leaky_index_aggregate(node)
            elif isinstance(node, IndexAggregate):
                miner_code += self._generate_index_aggregate(node)
            elif isinstance(node, Aggregate):
                miner_code += self._generate_aggregate(node)
            elif isinstance(node, Concat):
                miner_code += self._generate_concat(node)
            elif isinstance(node, Create):
                miner_code += self._generate_create(node)
            elif isinstance(node, Divide):
                miner_code += self._generate_divide(node)
            elif isinstance(node, FlagJoin):
                miner_code += self._generate_flag_join(node)
            elif isinstance(node, IndexJoin):
                miner_code += self._generate_index_join(node)
            elif isinstance(node, Join):
                miner_code += self._generate_join(node)
            elif isinstance(node, Multiply):
                miner_code += self._generate_multiply(node)
            elif isinstance(node, Open):
                miner_code += self._generate_open(node)
            elif isinstance(node, Project):
                miner_code += self._generate_project(node)
            elif isinstance(node, Close):
                miner_code += self._generate_close(node)
            elif isinstance(node, Shuffle):
                miner_code += self._generate_shuffle(node)
            elif isinstance(node, Persist):
                miner_code += self._generate_persist(node)
            elif isinstance(node, Filter):
                miner_code += self._generate_filter(node)
            elif isinstance(node, DistinctCount):
                miner_code += self._generate_distinct_count(node)
            elif isinstance(node, ConcatCols):
                miner_code += self._generate_concat_cols(node)
            elif isinstance(node, SortBy):
                miner_code += self._generate_sort_by(node)
            elif isinstance(node, Blackbox):
                miner_code += self._generate_blackbox(node)
            else:
                print("encountered unknown operator type", repr(node))

        # expand top-level protocol template
        template = open(
            "{0}/protocol.tmpl".format(self.template_directory), 'r').read()
        return pystache.render(
            template, {"PROTOCOL_CODE": miner_code})

    def _get_controller_pid(self, nodes: list):
        """ Returns pid of Controller. """

        # we need all open ops to get all output parties
        open_ops = filter(lambda op_node: isinstance(op_node, Open), nodes)
        # union of all stored_withs gives us all output parties
        output_parties = set().union(
            *[op.out_rel.stored_with for op in open_ops])
        # only support one output party
        assert len(output_parties) == 1, len(output_parties)
        # that output party will be the controller
        return next(iter(output_parties))  # pop

    def _get_input_parties(self, nodes: list):
        """ Returns pid's of all parties in this computation. """

        # we need all close ops to get all input parties
        close_ops = filter(lambda op_node: isinstance(op_node, Close), nodes)
        # union of all storedWiths gives us all input parties
        input_parties = set().union(
            *[op.get_in_rel().stored_with for op in close_ops])
        # want these in-order
        return sorted(list(input_parties))

    def _generate_input_code(self, nodes: list, job_name: str, output_directory: str):
        """ Generates code for loading inputs. """

        # all schemas of the relations this party will input
        schemas = {}
        # only need close ops to generate schemas
        close_ops = filter(lambda op_node: isinstance(op_node, Close), nodes)
        # only need schemas for my close ops
        my_close_ops = filter(
            lambda close_op: self.pid in close_op.get_in_rel().stored_with, close_ops)
        # all CSVImports this party will perform
        hdfs_import_statements = []
        import_statements = []
        for close_op in my_close_ops:
            # generate schema and get its name
            name, schema, header = self._generate_schema(close_op)
            schemas[name] = schema
            # TODO: hack hack hack
            if self.sm_config.use_hdfs:
                hdfs_import_statements.append(self._generate_hdfs_import(
                    close_op, header, job_name)[:-1])
            else:
                hdfs_import_statements.append("cp {} {}".format(
                    self.config.output_path + "/" + name + ".csv",
                    self.config.code_path + "/" + job_name + "/" + name + ".csv"
                )
                )
            # generate csv import code
            import_statements.append(self._generate_csv_import(
                close_op, output_directory, job_name)[:-1])
        input_code = "&&".join(import_statements)
        # expand top-level
        # TODO: hack hack hack
        if self.sm_config.use_docker:
            top_level_template = open(
                "{0}/csv_import_top_level.tmpl".format(self.template_directory), 'r').read()
        else:
            top_level_template = open(
                "{0}/csv_import_no_docker.tmpl".format(self.template_directory), 'r').read()
        top_level_data = {
            "SHAREMIND_HOME": self.sm_config.home_path,
            "HDFS_IMPORTS": "\n".join(hdfs_import_statements),
            "IMPORTS": input_code
        }
        # return schemas and input code
        return schemas, pystache.render(top_level_template, top_level_data)

    def _generate_submit_code(self, nodes: list, job_name: str, code_path: str):
        """ Generates code that submits Sharemind code to miners. """

        # hack HDFS output
        open_ops = filter(lambda op_node: isinstance(op_node, Open), nodes)
        hdfs_cmds = []
        for open_op in open_ops:
            name = open_op.out_rel.name
            # TODO: hack hack hack
            if self.sm_config.use_hdfs:
                hdfs_cmd = "hadoop fs -put {}.csv {}.csv".format(
                    code_path + "/" + job_name + "/" + name,
                    self.config.output_path + "/" + name
                )
            else:
                hdfs_cmd = "mv {}.csv {}.csv".format(
                    code_path + "/" + job_name + "/" + name,
                    self.config.output_path + "/" + name
                )
            hdfs_cmds.append(hdfs_cmd)
        hdfs_cmds_str = "\n".join(hdfs_cmds)

        # code for submitting job to miners
        template = None
        # TODO: hack hack hack
        if self.sm_config.use_docker:
            template = open(
                "{0}/submit.tmpl".format(self.template_directory), 'r').read()
        else:
            template = open(
                "{0}/submit_no_docker.tmpl".format(self.template_directory), 'r').read()
        data = {
            "SHAREMIND_HOME": self.sm_config.home_path,
            "CODE_PATH": code_path + "/" + job_name,
            "HDFS_CMDS": hdfs_cmds_str
        }

        # inner template (separate shell script)
        template_inner = open(
            "{0}/submit_inner.tmpl".format(self.template_directory), 'r').read()

        # we need all open ops to get the size of the output rels for parsing
        open_ops = list(filter(lambda op_node: isinstance(op_node, Open), nodes))
        rel_names = [open_op.out_rel.name for open_op in open_ops]
        num_cols = [str(len(open_op.out_rel.columns)) for open_op in open_ops]
        rels_meta_str = " ".join(
            ["--rels-meta {}:{}".format(rname, ncols) for (rname, ncols) in zip(rel_names, num_cols)])

        data_inner = {
            "CODE_PATH": code_path + "/" + job_name,
            "RELS_META": rels_meta_str,
            "LOCAL_OUTPUT_PATH": self.config.code_path + "/" + job_name
        }
        return {
            "outer": pystache.render(template, data),
            "inner": pystache.render(template_inner, data_inner)
        }

    def _generate_controller_code(self, nodes: list, job_name: str, output_directory: str):
        """ Generates code that is run by the Sharemind controller. """

        submit_code = self._generate_submit_code(
            nodes, job_name, output_directory)
        return submit_code

    def _generate_aggregate(self, agg_op: Aggregate):
        """ Generate code for Aggregate operations. """

        template = open(
            "{0}/aggregate_sum.tmpl".format(self.template_directory), 'r').read()

        # for now, only 1 groupCol in mpc ops
        # TODO: update template with multi-col
        data = {
            "TYPE": "uint32",
            "OUT_REL_NAME": agg_op.out_rel.name,
            "LEAKY_SUFFIX": "Leaky" if self.config.use_leaky_ops else "",
            "IN_REL_NAME": agg_op.get_in_rel().name,
            "KEY_COL_IDX": agg_op.group_cols[0].idx,
            "AGG_COL_IDX": agg_op.agg_col.idx
        }
        return pystache.render(template, data)

    def _generate_sort_by(self, sort_by_op: SortBy):
        """ Generate code for SortBy operations. """

        template = open(
            "{0}/sort_by.tmpl".format(self.template_directory), 'r').read()

        data = {
            "TYPE": "uint32",
            "OUT_REL_NAME": sort_by_op.out_rel.name,
            "IN_REL_NAME": sort_by_op.get_in_rel().name,
            "SORT_BY_COL_IDX": sort_by_op.sort_by_col.idx
        }
        return pystache.render(template, data)

    def _generate_leaky_index_aggregate(self, lky_idx_agg_op: LeakyIndexAggregate):
        """ Generate code for LeakyIndexAggregate operations. """

        template = open(
            "{0}/leaky_index_aggregate_sum.tmpl".format(self.template_directory), 'r').read()

        data = {
            "TYPE": "uint32",
            "OUT_REL_NAME": lky_idx_agg_op.out_rel.name,
            "IN_REL_NAME": lky_idx_agg_op.get_in_rel().name,
            "AGG_COL_IDX": lky_idx_agg_op.agg_col.idx,
            "KEYS_REL": lky_idx_agg_op.dist_keys.out_rel.name,
            "INDEXES_REL": lky_idx_agg_op.keys_to_idx_map.out_rel.name
        }
        return pystache.render(template, data)

    def _generate_index_aggregate(self, idx_agg_op: IndexAggregate):
        """ Generate code for Index Aggregate operations. """

        template = open(
            "{0}/index_aggregate_sum.tmpl".format(self.template_directory), 'r').read()

        data = {
            "TYPE": "uint32",
            "OUT_REL_NAME": idx_agg_op.out_rel.name,
            "IN_REL_NAME": idx_agg_op.get_in_rel().name,
            "GROUP_COL_IDX": idx_agg_op.group_cols[0].idx,
            "AGG_COL_IDX": idx_agg_op.agg_col.idx,
            "EQ_FLAG_REL": idx_agg_op.eq_flag_op.out_rel.name,
            "SORTED_KEYS_REL": idx_agg_op.sorted_keys_op.out_rel.name
        }
        return pystache.render(template, data)

    def _generate_close(self, close_op: Close):
        """ Generate code for Close operations. """

        template = open(
            "{0}/close.tmpl".format(self.template_directory), 'r').read()
        data = {
            "TYPE": "uint32",
            "OUT_REL_NAME": close_op.out_rel.name,
            "IN_REL_NAME": close_op.get_in_rel().name
        }
        return pystache.render(template, data)

    def _generate_concat_cols(self, concat_cols_op: ConcatCols):
        if len(concat_cols_op.get_in_rels()) != 2:
            raise NotImplementedError("Only support concat cols of two relations")
        if concat_cols_op.use_mult:
            template = open(
                "{0}/mult.tmpl".format(self.template_directory), 'r').read()
            data = {
                "TYPE": "uint32",
                "OUT_REL": concat_cols_op.out_rel.name,
                "LEFT_IN_REL": concat_cols_op.get_in_rels()[0].name,
                "RIGHT_IN_REL": concat_cols_op.get_in_rels()[1].name
            }
            return pystache.render(template, data)
        else:
            template = open(
                "{0}/concat_cols.tmpl".format(self.template_directory), 'r').read()
            data = {
                "TYPE": "uint32",
                "OUT_REL": concat_cols_op.out_rel.name,
                "LEFT_IN_REL": concat_cols_op.get_in_rels()[0].name,
                "RIGHT_IN_REL": concat_cols_op.get_in_rels()[1].name
            }
            return pystache.render(template, data)

    def _generate_concat(self, concat_op: Concat):
        """ Generate code for Concat operations. """

        in_rels = concat_op.get_in_rels()
        assert len(in_rels) > 1

        # Sharemind only allows us to concatenate two relations at a time
        # so we need to chain calls repeatedly for more
        cat_template = open(
            "{0}/cat_expr.tmpl".format(self.template_directory), 'r').read()

        cats = cat_template
        for in_rel in in_rels[:-2]:
            data = {
                "LEFT_REL": in_rel.name,
                "RIGHT_REL": cat_template
            }
            cats = pystache.render(cats, data)
        outer = open(
            "{0}/concat_def.tmpl".format(self.template_directory), 'r').read()
        data = {
            "OUT_REL": concat_op.out_rel.name,
            "TYPE": "uint32",
            "CATS": cats
        }
        outer = pystache.render(outer, data)
        data = {
            "LEFT_REL": in_rels[-2].name,
            "RIGHT_REL": in_rels[-1].name
        }
        return pystache.render(outer, data)

    def _generate_create(self, create_op: Create):
        """ Generate code for Create operations. """

        template = open(
            "{0}/read_from_db.tmpl".format(self.template_directory), 'r').read()
        data = {
            "NAME": create_op.out_rel.name,
            "TYPE": "uint32",
            "ADD_FLAGS": "false" if self.config.use_leaky_ops else "true"
        }
        return pystache.render(template, data)

    def _generate_divide(self, divide_op: Divide):
        """ Generate code for Divide operations. """

        template = open(
            "{0}/divide.tmpl".format(self.template_directory), 'r').read()

        operands = [op.idx if isinstance(
            op, Column) else op for op in divide_op.operands]
        operands_str = ",".join(str(op) for op in operands)
        scalar_flags = [0 if isinstance(
            op, Column) else 1 for op in divide_op.operands]
        scalar_flags_str = ",".join(str(op) for op in scalar_flags)

        data = {
            "TYPE": "uint32",
            "OUT_REL": divide_op.out_rel.name,
            "IN_REL": divide_op.get_in_rel().name,
            "TARGET_COL": divide_op.target_col.idx,
            # hacking array brackets
            "OPERANDS": "{" + operands_str + "}",
            "SCALAR_FLAGS": "{" + scalar_flags_str + "}"
        }
        return pystache.render(template, data)

    # def _generate_flag_join(self, flag_join_op: FlagJoin):
    #     """ Generate code for FlagJoin operations. """
    #
    #     template = open(
    #         "{0}/flag_join.tmpl".format(self.template_directory), 'r').read()
    #     flags_rel = flag_join_op.join_flag_op.out_rel
    #     left_rel = flag_join_op.left_parent.out_rel
    #     right_rel = flag_join_op.right_parent.out_rel
    #     # sharemind adds all columns from right-rel to the result
    #     # so we need to explicitely exclude these
    #     cols_to_keep = list(range(len(left_rel.columns) + len(right_rel.columns)))
    #     cols_to_exclude = [col.idx + len(left_rel.columns)
    #                        for col in flag_join_op.right_join_cols]
    #     cols_to_keep_str = ",".join(
    #         [str(idx) for idx in cols_to_keep if idx not in cols_to_exclude])
    #
    #     data = {
    #         "TYPE": "uint32",
    #         "OUT_REL": flag_join_op.out_rel.name,
    #         "FLAGS_REL": flags_rel.name,
    #         "LEFT_IN_REL": flag_join_op.get_left_in_rel().name,
    #         "LEFT_KEY_COLS": str(flag_join_op.left_join_cols[0].idx),
    #         "RIGHT_IN_REL": flag_join_op.get_right_in_rel().name,
    #         "RIGHT_KEY_COLS": str(flag_join_op.right_join_cols[0].idx),
    #         "COLS_TO_KEEP": "{" + cols_to_keep_str + "}"
    #     }
    #     return pystache.render(template, data)

    def _generate_flag_join(self, flag_join_op: FlagJoin):
        """ Generate code for FlagJoin operations. """

        template = open(
            "{0}/obl_idx_step_one.tmpl".format(self.template_directory), 'r').read()
        flags_rel = flag_join_op.join_flag_op.out_rel
        data = {
            "TYPE": "uint32",
            "OUT_REL": flag_join_op.out_rel.name,
            "PREFIX": flag_join_op.out_rel.name,
            "IN_REL": flag_join_op.get_left_in_rel().name,
            "FLAGS_REL": flag_join_op.get_right_in_rel().name,
            "LEN_REL": flags_rel.name
        }
        return pystache.render(template, data)

    def _generate_index_join(self, index_join_op: IndexJoin):
        """ Generate code for Index Join operations. """

        template = open(
            "{0}/index_join.tmpl".format(self.template_directory), 'r').read()
        index_rel = index_join_op.index_rel.out_rel

        data = {
            "TYPE": "uint32",
            "OUT_REL": index_join_op.out_rel.name,
            "LEFT_IN_REL": index_join_op.get_left_in_rel().name,
            "LEFT_KEY_COLS": str(index_join_op.left_join_cols[0].idx),
            "RIGHT_IN_REL": index_join_op.get_right_in_rel().name,
            "RIGHT_KEY_COLS": str(index_join_op.right_join_cols[0].idx),
            "INDEX_REL": index_rel.name
        }
        return pystache.render(template, data)

    def _generate_join(self, join_op: Join):
        """ Generate code for Join operations. """

        template = open(
            "{0}/join.tmpl".format(self.template_directory), 'r').read()
        left_key_cols_str = ",".join([str(col.idx)
                                      for col in join_op.left_join_cols])
        right_key_cols_str = ",".join(
            [str(col.idx) for col in join_op.right_join_cols])
        left_rel = join_op.left_parent.out_rel
        right_rel = join_op.right_parent.out_rel

        # sharemind adds all columns from right-rel to the result
        # so we need to explicitely exclude these
        cols_to_keep = list(
            range(
                len(left_rel.columns)
                + len(right_rel.columns)
                + (1 if not self.config.use_leaky_ops else 0)))  # account for flag column
        print(cols_to_keep)
        cols_to_exclude = [col.idx + len(left_rel.columns)
                           for col in join_op.right_join_cols]
        cols_to_keep_str = ",".join(
            [str(idx) for idx in cols_to_keep if idx not in cols_to_exclude])

        data = {
            "TYPE": "uint32",
            "OUT_REL": join_op.out_rel.name,
            "LEAKY_SUFFIX": "Leaky" if self.config.use_leaky_ops else "",
            "LEFT_IN_REL": join_op.get_left_in_rel().name,
            "LEFT_KEY_COLS": "{" + left_key_cols_str + "}",
            "RIGHT_IN_REL": join_op.get_right_in_rel().name,
            "RIGHT_KEY_COLS": "{" + right_key_cols_str + "}",
            "COLS_TO_KEEP": "{" + cols_to_keep_str + "}"
        }
        return pystache.render(template, data)

    def _generate_open(self, open_op: Open):
        """ Generate code for Open operations. """

        template = open(
            "{0}/publish.tmpl".format(self.template_directory), 'r').read()

        data = {
            "OUT_REL": open_op.out_rel.name,
            "IN_REL": open_op.get_in_rel().name,
            "FILTER_FLAGS": "false" if self.config.use_leaky_ops else "true"
        }
        return pystache.render(template, data)

    def _generate_shuffle(self, shuffle_op: Shuffle):
        """ Generate code for Shuffle operations. """

        template = open(
            "{0}/shuffle.tmpl".format(self.template_directory), 'r').read()

        data = {
            "TYPE": "uint32",
            "OUT_REL": shuffle_op.out_rel.name,
            "IN_REL": shuffle_op.get_in_rel().name
        }
        return pystache.render(template, data)

    def _generate_persist(self, persist_op: Persist):
        """ Generate code for Persist operations. """

        template = open(
            "{0}/persist.tmpl".format(self.template_directory), 'r').read()

        data = {
            "OUT_REL": persist_op.out_rel.name,
            "IN_REL": persist_op.get_in_rel().name,
        }
        return pystache.render(template, data)

    def _generate_project(self, project_op: Project):
        """ Generate code for Project operations. """

        template = open(
            "{0}/project.tmpl".format(self.template_directory), 'r').read()
        selected_cols = project_op.selected_cols
        selected_col_str = ",".join([str(col.idx) for col in selected_cols])

        data = {
            "TYPE": "uint32",
            "OUT_REL": project_op.out_rel.name,
            "IN_REL": project_op.get_in_rel().name,
            # hacking array brackets
            "SELECTED_COLS": "{" + selected_col_str + "}"
        }
        return pystache.render(template, data)

    def _generate_filter(self, filter_op: Filter):
        """ Generate code for Filter operations. """

        template = open(
            "{0}/filter.tmpl".format(self.template_directory), 'r').read()
        # TODO
        if self.config.use_leaky_ops:
            raise Exception("No sharemind support leaky filter")

        left_col = filter_op.filter_col
        right_operand = str(filter_op.other_col.idx) if not filter_op.is_scalar else str(filter_op.scalar)

        data = {
            "TYPE": "uint32",
            "OUT_REL": filter_op.out_rel.name,
            "OP": "Lt" if filter_op.operator == "<" else "Eq",
            "IN_REL": filter_op.get_in_rel().name,
            # hacking array brackets
            "LEFT_COL": "{" + str(left_col.idx) + "}",
            "RIGHT_COL": "{" + right_operand + "}"
        }
        return pystache.render(template, data)

    def _generate_distinct_count(self, distinct_count_op: DistinctCount):
        """ Generate code for DistinctCount operations. """

        template = open(
            "{0}/distinct_count.tmpl".format(self.template_directory), 'r').read()
        if self.config.use_leaky_ops:
            raise Exception("No sharemind support leaky dist count")

        selected_col = distinct_count_op.selected_col

        data = {
            "TYPE": "uint32",
            "OUT_REL": distinct_count_op.out_rel.name,
            "IN_REL": distinct_count_op.get_in_rel().name,
            "SELECTED_COL": str(selected_col.idx),
            "USE_SORT": "true" if distinct_count_op.use_sort else "false"
        }
        return pystache.render(template, data)

    def _generate_multiply(self, multiply_op: Multiply):
        """ Generate code for Multiply operations. """

        template = open(
            "{0}/multiply.tmpl".format(self.template_directory), 'r').read()

        operands = [op.idx if isinstance(
            op, Column) else op for op in multiply_op.operands]
        operands_str = ",".join(str(op) for op in operands)
        scalar_flags = [0 if isinstance(
            op, Column) else 1 for op in multiply_op.operands]
        scalar_flags_str = ",".join(str(op) for op in scalar_flags)

        data = {
            "TYPE": "uint32",
            "OUT_REL": multiply_op.out_rel.name,
            "IN_REL": multiply_op.get_in_rel().name,
            "TARGET_COL": multiply_op.target_col.idx,
            # hacking array brackets
            "OPERANDS": "{" + operands_str + "}",
            "SCALAR_FLAGS": "{" + scalar_flags_str + "}"
        }
        return pystache.render(template, data)

    @staticmethod
    def _generate_blackbox(blackbox_op: Blackbox):
        if blackbox_op.backend != "sharemind":
            raise Exception("Wrong backend blackbox op {} {}".format(blackbox_op.backend, blackbox_op))
        return blackbox_op.code

    def _generate_schema(self, close_op: Close):
        """ Generate schema for Sharemind job. """

        in_rel = close_op.get_in_rel()
        in_cols = in_rel.columns
        out_rel = close_op.out_rel
        out_cols = out_rel.columns
        col_defs = []
        col_def_template = open(
            "{0}/col_def.tmpl".format(self.template_directory), 'r').read()
        for in_col, out_col in zip(in_cols, out_cols):
            col_data = {
                'IN_NAME': in_col.get_name(),
                'OUT_NAME': out_col.get_name(),
                'TYPE': "uint32"  # hard-coded for now
            }
            col_defs.append(pystache.render(col_def_template, col_data))
        col_def_str = "\n".join(col_defs)
        rel_def_template = open(
            "{0}/rel_def.tmpl".format(self.template_directory), 'r').read()
        rel_data = {
            "NAME": close_op.get_in_rel().name,
            "COL_DEFS": col_def_str
        }
        rel_def_header = ",".join([c.name for c in in_cols])
        rel_def_str = pystache.render(rel_def_template, rel_data)
        return in_rel.name, rel_def_str, rel_def_header

    def _generate_hdfs_import(self, close_op: Close, header: str, job_name: str):
        """ Generate HDFS import code. """

        template = open(
            "{0}/hdfs_import.tmpl".format(self.template_directory), 'r').read()
        data = {
            "IN_NAME": close_op.get_in_rel().name,
            "SCHEMA_HEADER": header,
            "INPUT_PATH": self.config.input_path,
            "CODE_PATH": self.config.code_path + "/" + job_name,
        }
        return pystache.render(template, data)

    def _generate_csv_import(self, close_op: Close, output_directory, job_name: str):
        """ Generate code for CSV file import. """

        def _delim_lookup(delim: str):
            """ Lookup how file inputs are delimited. """
            if delim == ",":
                return "c"
            else:
                raise Exception("unknown delimiter")

        template = open(
            "{0}/csv_import.tmpl".format(self.template_directory), 'r').read()
        data = {
            "IN_NAME": close_op.get_in_rel().name,
            "INPUT_PATH": self.config.input_path,
            "CODE_PATH": self.config.code_path + "/" + job_name,
            'DELIMITER': _delim_lookup(self.config.delimiter),
        }
        return pystache.render(template, data)

    def _write_code(self, code_dict: dict, job_name: str):
        """ Write generated code to file. """

        def _write(root_dir: str, fn: str, ext: str, content: str, job_name: str):

            fullpath = "{}/{}/{}.{}".format(root_dir, job_name, fn, ext)
            os.makedirs(os.path.dirname(fullpath), exist_ok=True)
            with open(fullpath, "w") as f:
                f.write(content)

        ext_lookup = {
            "schemas": "xml",
            "input": "sh",
            "submit": "sh",
            "submitInner": "sh",
            "miner": "sc"
        }

        # write files
        job_code_path = self.config.code_path
        for code_type, code in code_dict.items():
            if code_type == "schemas":
                schemas = code
                for schema_name in schemas:
                    _write(job_code_path, schema_name,
                           ext_lookup[code_type], schemas[schema_name], job_name)
            else:
                _write(job_code_path, code_type, ext_lookup[code_type], code, job_name)
