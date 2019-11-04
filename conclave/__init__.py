import conclave.comp as comp
import conclave.dag as condag
import conclave.partition as part
from conclave.codegen import scotch
from conclave.codegen.python import PythonCodeGen
from conclave.codegen.sharemind import SharemindCodeGen
from conclave.codegen.spark import SparkCodeGen
from conclave.codegen.oblivc import OblivcCodeGen
from conclave.codegen.jiff import JiffCodeGen
from conclave.codegen.single_party import SinglePartyCodegen
from conclave.config import CodeGenConfig
from conclave.dispatch import dispatch_all
from conclave.net import SalmonPeer
from conclave.net import setup_peer


def generate_code(protocol: callable, cfg: CodeGenConfig, mpc_frameworks: list,
                  local_frameworks: list, apply_optimizations: bool = True):
    """
    Applies optimization rewrite passes to protocol, partitions resulting dag, and generates backend specific code
    for each sub-dag.
    :param protocol: protocol to compile
    :param cfg: conclave configuration
    :param mpc_frameworks: available mpc backend frameworks
    :param local_frameworks: available local-processing backend frameworks
    :param apply_optimizations: flag indicating if optimization rewrite passes should be applied to condag
    :return: queue of job objects to be executed by dispatcher
    """

    dag = condag.OpDag(protocol())
    job_queue = []

    if "single-party-spark" not in set(mpc_frameworks) and "single-party-python" not in set(mpc_frameworks):

        # currently only allow one local and one mpc framework
        assert len(mpc_frameworks) == 1 and len(local_frameworks) == 1

        # only apply optimizations if required
        if apply_optimizations:
            dag = comp.rewrite_dag(dag, cfg)

        # partition into sub-dags that will run in specific frameworks
        mapping = part.heupart(dag, mpc_frameworks, local_frameworks)

        # for each sub-dag run code gen and add resulting job to job queue
        for job_num, (framework, sub_dag, stored_with) in enumerate(mapping):
            print(job_num, framework)
            if framework == "sharemind":
                name = "{}-sharemind-job-{}".format(cfg.name, job_num)
                job = SharemindCodeGen(cfg, sub_dag, cfg.pid).generate(name, cfg.output_path)
                job_queue.append(job)
            elif framework == "spark":
                name = "{}-spark-job-{}".format(cfg.name, job_num)
                job = SparkCodeGen(cfg, sub_dag).generate(name, cfg.output_path)
                job_queue.append(job)
            elif framework == "python":
                name = "{}-python-job-{}".format(cfg.name, job_num)
                job = PythonCodeGen(cfg, sub_dag).generate(name, cfg.output_path)
                job_queue.append(job)
            elif framework == "obliv-c":
                name = "{}-oblivc-job-{}".format(cfg.name, job_num)
                job = OblivcCodeGen(cfg, sub_dag, cfg.pid).generate(name, cfg.output_path)
                job_queue.append(job)
            elif framework == "jiff":
                name = "{}-jiff-job-{}".format(cfg.name, job_num)
                job = JiffCodeGen(cfg, sub_dag, cfg.pid).generate(name, cfg.output_path)
                job_queue.append(job)
            else:
                raise Exception("Unknown framework: " + framework)

            # TODO: this probably doesn't belong here
            if cfg.pid not in stored_with:
                job.skip = True

    else:

        assert len(mpc_frameworks) == 1

        if mpc_frameworks[0] == "single-party-spark":

            name = "{}-spark-job-0".format(cfg.name)
            job = SinglePartyCodegen(cfg, dag, "spark").generate(name, cfg.output_path)
            job_queue.append(job)

        elif mpc_frameworks[0] == "single-party-python":

            name = "{}-python-job-0".format(cfg.name)
            job = SinglePartyCodegen(cfg, dag, "python").generate(name, cfg.output_path)
            job_queue.append(job)

        else:

            raise Exception("Unknown framework: {}".format(mpc_frameworks[0]))

    return job_queue


def dispatch_jobs(job_queue: list, conclave_config: CodeGenConfig, time_dispatch: bool = False):
    """
    Dispatches jobs to respective backends.
    :param time_dispatch: will record the execution time of dispatch if true
    :param job_queue: jobs to dispatch
    :param conclave_config: conclave configuration
    """

    networked_peer = None

    # if more than one party is involved in the protocol, we need a networked peer
    if len(conclave_config.all_pids) > 1:
        networked_peer = _setup_networked_peer(conclave_config.network_config)

    if time_dispatch:
        # TODO use timeit
        import time
        import datetime

        start_time = time.time()
        dispatch_all(conclave_config, networked_peer, job_queue)
        elapsed_time = time.time() - start_time
        formatted_time = datetime.timedelta(milliseconds=(elapsed_time * 1000))
        print("TIMED", conclave_config.name, round(elapsed_time, 3), formatted_time)

        with open("timing_results.csv", "a+") as time_f:
            out = ",".join([conclave_config.name, str(round(elapsed_time, 3)), str(formatted_time)])
            time_f.write(out + "\n")
    else:
        dispatch_all(conclave_config, networked_peer, job_queue)


def generate_and_dispatch(protocol: callable, conclave_config: CodeGenConfig, mpc_frameworks: list,
                          local_frameworks: list, apply_optimizations: bool = True):
    """
    Calls generate_code to generate code from protocol and :func:`~conclave.__init__.dispatch_jobs` to
    dispatch it.
    """

    job_queue = generate_code(protocol, conclave_config, mpc_frameworks, local_frameworks, apply_optimizations)
    dispatch_jobs(job_queue, conclave_config)


def _setup_networked_peer(network_config):
    return setup_peer(network_config)
