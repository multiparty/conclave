import salmon.job
from . import sharemind

def dispatch_all(peer, jobs):

    # create a lookup from job class to instantiated dispatcher
    dispatchers = {
        salmon.job.SharemindJob: sharemind.SharemindDispatcher(peer)                
    }

    # dispatch each job
    for job in jobs:
        try:
            # lookup dispatcher and dispatch
            dispatchers[type(job)].dispatch(job)
        except Exception as e:
            print(e) 

