from subprocess import call


class SparkDispatcher():
    '''
    Dispatches Spark jobs
    '''

    def __init__(self, peer):
        self.peer = peer
        self.loop = peer.loop
        self.to_wait_on = {}
        self.early = set()

    def _dispatch(self, job):

        # TODO: update for spark parameters, inpt/outpt file locations
        cmd = "spark-submit {0}/{1}.py {1}.csv {1}_out.csv"\
            .format(job.root_dir, job.name)

        print("Running script located at: {}/{}"
              .format(job.root_dir, job.name)
              )

        try:
            call(["bash", cmd])
        except Exception as e:
            print(e)

    def dispatch(self, job):

        self.peer.register_dispatcher(self)

        self._dispatch(job)

        self.peer.dispatcher = None

    def receive_msg(self, msg):

        done_peer = msg.pid
        if done_peer in self.to_wait_on:
            self.to_wait_on[done_peer].set_result(True)
        else:
            self.early.add(done_peer)
            print("early message", msg)


