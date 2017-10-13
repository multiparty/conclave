import salmon.job
from . import sharemind, spark


def dispatch_all(spark_master, sharemind_peer, jobs):

    # create a lookup from job class to instantiated dispatcher
    dispatchers = {
        salmon.job.SharemindJob: sharemind.SharemindDispatcher(sharemind_peer) if sharemind_peer else None,
        salmon.job.SparkJob: spark.SparkDispatcher(spark_master) if spark_master else None
    }

    # dispatch each job
    for job in jobs:
        try:
            # look up dispatcher and dispatch
            dispatchers[type(job)].dispatch(job)
        except Exception as e:
            print(e)
