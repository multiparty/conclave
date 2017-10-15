import salmon.job
from . import sharemind, spark, python


def dispatch_all(spark_master, sharemind_peer, jobs):

    # create a lookup from job class to instantiated dispatcher
    dispatchers = {
        salmon.job.SharemindJob: sharemind.SharemindDispatcher(sharemind_peer) if sharemind_peer else None,
        salmon.job.SparkJob: spark.SparkDispatcher(spark_master) if spark_master else None,
        salmon.job.PythonJob: python.PythonDispatcher()
    }

    # dispatch each job
    for job in jobs:
        if not job.skip:
            try:
                # look up dispatcher and dispatch
                dispatchers[type(job)].dispatch(job)
            except Exception as e:
                print(e)
        else:
            print("Skipping other party's job: ", job)
