import salmon.dag as saldag
import salmon.comp as comp
import salmon.partition as part
from salmon.codegen.sharemind import SharemindCodeGen
from salmon.codegen.spark import SparkCodeGen

def codegen(protocol, config):

    # apply optimizations
    dag = comp.rewriteDag(saldag.OpDag(protocol()))
    # prune for party
    pruned = comp.pruneDag(dag, config["general"]["pid"])
    # partition into subdags that will run in specific frameworks
    mapping = part.heupart(dag)
    # for each sub dag run code gen and add resulting job to job queue
    jobqueue = []
    for job_num, (fmwk, subdag) in enumerate(mapping):
        print(job_num, fmwk)
        if fmwk == "sharemind":
            job = SharemindCodeGen(subdag, config["general"]["pid"]).generate(
                "sharemind-job-" + str(job_num), config["sharemind"]["home"])
            jobqueue.append(job)
        elif fmwk == "spark":
            job = SparkCodeGen(subdag).generate(
                "spark-job-" + str(job_num), config["spark"]["home"])
            jobqueue.append(job)
        else:
            raise Exception("Unknown framework: " + fmwk)
    # return job
    return jobqueue
