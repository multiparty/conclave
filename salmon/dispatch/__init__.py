import salmon.job
from . import sharemind, spark, python


def dispatch_all(conclave_config, networked_peer, job_queue):
    """
    Dispatches jobs in job queue.
    :param conclave_config: conclave configuration
    :param networked_peer: networked peer in case dispatching involves more than one party; none otherwise
    :param job_queue: jobs to dispatch
    """

    # create a lookup from job class to instantiated dispatcher
    dispatchers = {
        salmon.job.SharemindJob: sharemind.SharemindDispatcher(networked_peer) if networked_peer else None,
        salmon.job.SparkJob: spark.SparkDispatcher(
            conclave_config.system_configs[
                "spark"].spark_master_url) if "spark" in conclave_config.system_configs else None,
        salmon.job.PythonJob: python.PythonDispatcher()
    }

    # dispatch each job
    for job in job_queue:
        if not job.skip:
            try:
                # look up dispatcher and dispatch
                dispatchers[type(job)].dispatch(job)
            except Exception as e:
                print(e)
        else:
            print("Skipping other party's job: ", job)
