import salmon.dag as saldag
import salmon.comp as comp
import salmon.partition as part
from salmon.config import CodeGenConfig
from salmon.codegen.sharemind import SharemindCodeGen
from salmon.codegen.spark import SparkCodeGen
from salmon.codegen.python import PythonCodeGen

def codegen(protocol, config, mpc_frameworks, local_frameworks):

    # currently only allow one local and one mpc framework
    assert len(mpc_frameworks) == 1 and len(local_frameworks) == 1

    # set up code gen config object
    if isinstance(config, CodeGenConfig):
        cfg = config
    else:
        cfg = CodeGenConfig.from_dict(config)

    # apply optimizations
    dag = comp.rewriteDag(saldag.OpDag(protocol()))
    # partition into subdags that will run in specific frameworks
    mapping = part.heupart(dag, mpc_frameworks, local_frameworks)
    # for each sub dag run code gen and add resulting job to job queue
    jobqueue = []
    for job_num, (fmwk, subdag, storedWith) in enumerate(mapping):
        print(job_num, fmwk)
        if fmwk == "sharemind":
            name = "{}-sharemind-job-{}".format(cfg.name, job_num)
            job = SharemindCodeGen(cfg, subdag, cfg.pid).generate(
                name, cfg.output_path)
            jobqueue.append(job)
        elif fmwk == "spark":
            name = "{}-spark-job-{}".format(cfg.name, job_num)
            job = SparkCodeGen(cfg, subdag).generate(name,
                                                     cfg.output_path)
            jobqueue.append(job)
        elif fmwk == "python":
            name = "{}-python-job-{}".format(cfg.name, job_num)
            job = PythonCodeGen(cfg, subdag).generate(name,
                                                      cfg.output_path)
            jobqueue.append(job)
        else:
            raise Exception("Unknown framework: " + fmwk)
        
        # TODO: this probably doesn't belong here
        if not config.pid in storedWith:
            job.skip = True
    # return job
    return jobqueue
