from subprocess import call


class JiffDispatcher:

    def __init__(self, peer, config):

        self.peer = peer
        self.config = config
        try:
            self.server_pid = config.system_configs["jiff"].server_pid
        except KeyError:
            print("Missing Jiff config \n")
        self.loop = peer.loop
        self.to_wait_on = {}
        self.early = set()

    def dispatch(self, job):

        # register self as current dispatcher with peer
        self.peer.register_dispatcher(self)

        if self.peer.pid == self.server_pid:
            cmd = "bash {0}/run_server.sh & bash {0}/run.sh".format(job.code_dir)
        else:
            cmd = "bash {0}/run.sh".format(job.code_dir)

        print("Jiff: {0}/run.sh dispatching"
              .format(job.code_dir))

        try:
            call(cmd, shell=True)
        except Exception as e:
            print(e)

        self.peer.dispatcher = None
        self.to_wait_on = {}
        self.early = set()
