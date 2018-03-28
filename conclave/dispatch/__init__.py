import conclave.job
from . import sharemind, spark, python, oblivc


def _synchronize(networked_peer):
    """
    Waits for other peers to finish all jobs before shutting down.
    """
    # TODO
    if networked_peer:
        pass


def dispatch_all(conclave_config, networked_peer, job_queue: list):
    """
    Dispatches jobs in job queue.
    """

    # create a lookup from job class to instantiated dispatcher
    dispatchers = {
        conclave.job.SharemindJob:
            sharemind.SharemindDispatcher(networked_peer) if networked_peer else None,
        conclave.job.SparkJob:
            spark.SparkDispatcher(
                conclave_config.system_configs["spark"].spark_master_url)
            if "spark" in conclave_config.system_configs else None,
        conclave.job.PythonJob: python.PythonDispatcher(),
        conclave.job.OblivCJob: oblivc.OblivCDispatcher()
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

    _synchronize(networked_peer)
