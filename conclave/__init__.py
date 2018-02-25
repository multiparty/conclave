import conclave.comp as comp
import conclave.dag as condag
import conclave.partition as part
from conclave.codegen import scotch
from conclave.codegen.python import PythonCodeGen
from conclave.codegen.sharemind import SharemindCodeGen
from conclave.codegen.spark import SparkCodeGen
from conclave.config import CodeGenConfig
from conclave.dispatch import dispatch_all
from conclave.net import SalmonPeer
from conclave.net import setup_peer


def generate_code(protocol: callable, conclave_config: CodeGenConfig, mpc_frameworks: list,
                  local_frameworks: list, apply_optimizations: bool = True):
    """
    Applies optimization rewrite passes to protocol, partitions resulting condag, and generates backend specific code for
    each sub-condag.
    :param protocol: protocol to compile
    :param conclave_config: conclave configuration
    :param mpc_frameworks: available mpc backend frameworks
    :param local_frameworks: available local-processing backend frameworks
    :param apply_optimizations: flag indicating if optimization rewrite passes should be applied to condag
    :return: queue of job objects to be executed by dispatcher
    """

    # currently only allow one local and one mpc framework
    assert len(mpc_frameworks) == 1 and len(local_frameworks) == 1

    # set up code gen config object
    if isinstance(conclave_config, CodeGenConfig):
        cfg = conclave_config
    else:
        cfg = CodeGenConfig.from_dict(conclave_config)

    dag = condag.OpDag(protocol())
    # only apply optimizations if required
    if apply_optimizations:
        dag = comp.rewrite_dag(dag, use_leaky_ops=conclave_config.use_leaky_ops)
    # partition into subdags that will run in specific frameworks
    mapping = part.heupart(dag, mpc_frameworks, local_frameworks)
    # for each sub condag run code gen and add resulting job to job queue
    job_queue = []
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
        else:
            raise Exception("Unknown framework: " + framework)

        # TODO: this probably doesn't belong here
        if conclave_config.pid not in stored_with:
            job.skip = True
    return job_queue


def dispatch_jobs(job_queue: list, conclave_config: CodeGenConfig, time_dispatch: bool = True):
    """
    Dispatches jobs to respective backends.
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
        startTime = time.time()
        dispatch_all(conclave_config, networked_peer, job_queue)
        elapsedTime = time.time() - startTime
        print("TIMED", conclave_config.name, round(elapsedTime, 3))
        with open("timing_results.csv", "a+") as time_f:
            out = ",".join([conclave_config.name, str(round(elapsedTime, 3))])
            time_f.write(out + "\n")
    else:
        dispatch_all(conclave_config, networked_peer, job_queue)
    

def generate_and_dispatch(protocol: callable, conclave_config: CodeGenConfig, mpc_frameworks: list,
                          local_frameworks: list, apply_optimizations: bool = True):
    """
    Calls generate_code to generate code from protocol and :func:`~salmon.__init__.dispatch_jobs` to
    dispatch it.
    """

    job_queue = generate_code(protocol, conclave_config, mpc_frameworks, local_frameworks, apply_optimizations)
    dispatch_jobs(job_queue, conclave_config)


def _setup_networked_peer(network_config):
    return setup_peer(network_config)
