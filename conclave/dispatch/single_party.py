


class SinglePartyDispatcher:

    def __init__(self, peer):

        self.peer = peer
        self.loop = peer.loop
        self.to_wait_on = {}
        self.early = set()

    def dispatch(self, job):

        if job.fmwk == "python":

            print("Python")

        elif job.fmwk == "spark":

            print("Spark")

        else:

            raise Exception("Unknown framework: {}".format(job.fmwk))