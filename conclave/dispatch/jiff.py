from subprocess import call


class JiffDispatcher:

    def __init__(self, peer):

        self.peer = peer
        self.loop = peer.loop
        self.to_wait_on = {}
        self.early = set()

    def dispatch(self, job):

        # register self as current dispatcher with peer
        self.peer.register_dispatcher(self)

        cmd = "{}/run.sh".format(job.code_dir)

        print("{}: {}/run.sh dispatching"
              .format(job.name, job.code_dir))

        try:
            call(["/bin/bash", cmd])
        except Exception as e:
            print(e)

        self.peer.dispatcher = None
        self.to_wait_on = {}
        self.early = set()
