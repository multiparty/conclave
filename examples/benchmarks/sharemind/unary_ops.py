from salmon.codegen import CodeGenConfig
from salmon.codegen.sharemind import SharemindCodeGen, SharemindCodeGenConfig
import salmon.dispatch
import salmon.net
from salmon.comp import dagonly
import salmon.lang as sal
from salmon.utils import *
import sys


def setup():

    # define inputs
    colsIn1 = [
        defCol("a", "INTEGER", [1]),
        defCol("b", "INTEGER", [1])
    ]
    colsIn2 = [
        defCol("a", "INTEGER", [2]),
        defCol("b", "INTEGER", [2])
    ]
    colsIn3 = [
        defCol("a", "INTEGER", [3]),
        defCol("b", "INTEGER", [3])
    ]

    in1 = sal.create("in1", colsIn1, set([1]))
    in2 = sal.create("in2", colsIn2, set([2]))
    in3 = sal.create("in3", colsIn3, set([3]))

    cl1 = sal._close(in1, "cl1", set([1, 2, 3]))
    cl2 = sal._close(in2, "cl2", set([1, 2, 3]))
    cl3 = sal._close(in3, "cl3", set([1, 2, 3]))

    rel = sal.concat([cl1, cl2, cl3], "rel")

    return set([in1, in2, in3]), rel


def agg(pid, config, sharemind_peer, f_size):

    @dagonly
    def protocol():

        inputs, rel = setup()
        res = sal.aggregate(rel, "agg", ["a"], "b", "+", "total")

        opened = sal._open(res, "opened", 1)

        return inputs

    cg = SharemindCodeGen(config, protocol(), pid)
    job = cg.generate("agg_{}".format(f_size), "")
    job_queue = [job]

    salmon.dispatch.dispatch_all(None, sharemind_peer, job_queue)


def col_div(pid, config, sharemind_peer, f_size):

    @dagonly
    def protocol():

        inputs, rel = setup()
        res = sal.divide(rel, 'div1', 'a', ['a', 'b'])

        opened = sal._open(res, "opened", 1)

        return inputs

    cg = SharemindCodeGen(config, protocol(), pid)
    job = cg.generate("col_div_{}".format(f_size), "")
    job_queue = [job]

    salmon.dispatch.dispatch_all(None, sharemind_peer, job_queue)


def col_mult(pid, config, sharemind_peer, f_size):

    @dagonly
    def protocol():

        inputs, rel = setup()
        res = sal.multiply(rel, 'mult1', 'a', ['a', 'b'])

        opened = sal._open(res, "opened", 1)

        return inputs

    cg = SharemindCodeGen(config, protocol(), pid)
    job = cg.generate("col_mult_{}".format(f_size), "")
    job_queue = [job]

    salmon.dispatch.dispatch_all(None, sharemind_peer, job_queue)


def scalar_div(pid, config, sharemind_peer, f_size):

    @dagonly
    def protocol():

        inputs, rel = setup()
        res = sal.divide(rel, 'div1', 'a', ['a', 1])

        opened = sal._open(res, "opened", 1)

        return inputs

    cg = SharemindCodeGen(config, protocol(), pid)
    job = cg.generate("scalar_div_{}".format(f_size), "")
    job_queue = [job]

    salmon.dispatch.dispatch_all(None, sharemind_peer, job_queue)


def scalar_mult(pid, config, sharemind_peer, f_size):

    @dagonly
    def protocol():

        inputs, rel = setup()
        res = sal.multiply(rel, 'mult1', 'a', ['a', 1])

        opened = sal._open(res, "opened", 1)

        return inputs

    cg = SharemindCodeGen(config, protocol(), pid)
    job = cg.generate("scalar_mult_{}".format(f_size), "")
    job_queue = [job]

    salmon.dispatch.dispatch_all(None, sharemind_peer, job_queue)


def project(pid, config, sharemind_peer, f_size):

    @dagonly
    def protocol():

        inputs, rel = setup()

        cols = [column.name for column in rel.outRel.columns][::-1]

        res = sal.project(rel, "proja", cols)

        opened = sal._open(res, "opened", 1)
        return inputs

    cg = SharemindCodeGen(config, protocol(), pid)
    job = cg.generate("project_{}".format(f_size), "")
    job_queue = [job]

    salmon.dispatch.dispatch_all(None, sharemind_peer, job_queue)


def join(pid, config, sharemind_peer, f_size):

    @dagonly
    def protocol():

        colsIn1 = [
            defCol("a", "INTEGER", [1]),
            defCol("b", "INTEGER", [1])
        ]
        in1 = sal.create("in1", colsIn1, set([1]))
        colsIn2 = [
            defCol("a", "INTEGER", [2]),
            defCol("b", "INTEGER", [2])
        ]
        in2 = sal.create("in2", colsIn2, set([2]))

        cl1 = sal._close(in1, "cl1", set([1, 2, 3]))
        cl2 = sal._close(in2, "cl2", set([1, 2, 3]))

        res = sal.join(cl1, cl2, "res", ["a"], ["a"])

        opened = sal._open(res, "opened", 1)
        return set([in1, in2])

    cg = SharemindCodeGen(config, protocol(), pid)
    job = cg.generate("join_{}".format(f_size), "")
    job_queue = [job]

    salmon.dispatch.dispatch_all(None, sharemind_peer, job_queue)


def no_hdfs():

    pid = int(sys.argv[1])
    num_tuples = sys.argv[2]
    op = sys.argv[3]

    # use if running locally
    #sharemind_config = {
    #    "pid": pid,
    #    "parties": {
    #        1: {"host": "localhost", "port": 9001},
    #        2: {"host": "localhost", "port": 9002},
    #        3: {"host": "localhost", "port": 9003}
    #    }
    #}
    sharemind_config = {
        "pid": pid,
        "parties": {
            1: {"host": "ca-spark-node-0", "port": 9001},
            2: {"host": "cb-spark-node-0", "port": 9002},
            3: {"host": "cc-spark-node-0", "port": 9003}
        }
    }

    workflow_name = "{}_{}_{}".format(op, num_tuples, pid)
    sm_cg_config = SharemindCodeGenConfig(
        workflow_name, "/mnt/shared", use_hdfs=False, use_docker=True)
    codegen_config = CodeGenConfig(
        workflow_name).with_sharemind_config(sm_cg_config)
    codegen_config.code_path = "/mnt/shared/" + workflow_name
    codegen_config.input_path = "/mnt/shared" + "/" + num_tuples
    codegen_config.output_path = "/mnt/shared" + "/" + num_tuples

    sm_peer = salmon.net.setup_peer(sharemind_config)

    if op == 'agg':
        agg(pid, codegen_config, sm_peer, num_tuples)
    elif op == 'col_div':
        col_div(pid, codegen_config, sm_peer, num_tuples)
    elif op == 'col_mult':
        col_mult(pid, codegen_config, sm_peer, num_tuples)
    elif op == 'scalar_div':
        scalar_div(pid, codegen_config, sm_peer, num_tuples)
    elif op == 'col_mult':
        scalar_mult(pid, codegen_config, sm_peer, num_tuples)
    elif op == 'project':
        project(pid, codegen_config, sm_peer, num_tuples)
    elif op == 'join':
        join(pid, codegen_config, sm_peer, num_tuples)
    else:
        print("Unknown:", op)


if __name__ == "__main__":

    no_hdfs()
