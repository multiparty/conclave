import salmon.job
from . import sharemind, spark, python


def dispatch_all(spark_master, networked_peer, job_queue):
    """
    Dispatches jobs in job queue.
    :param spark_master: url of spark master node TODO move this into config object
    :param networked_peer: networked peer in case dispatching involves more than one party; none otherwise
    :param job_queue: jobs to dispatch
    """

    # create a lookup from job class to instantiated dispatcher
    dispatchers = {
        salmon.job.SharemindJob: sharemind.SharemindDispatcher(networked_peer) if networked_peer else None,
        salmon.job.SparkJob: spark.SparkDispatcher(spark_master) if spark_master else None,
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
