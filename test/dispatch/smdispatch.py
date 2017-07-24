import salmon.dispatch
import salmon.net
from salmon.job import *
import sys

if __name__ == "__main__":

    pid = int(sys.argv[1])
    config = {
        "pid": pid,
        "parties": {
            1: {"host": "localhost", "port": 9001},
            2: {"host": "localhost", "port": 9002},
            3: {"host": "localhost", "port": 9003}
        }
    }
    peer = salmon.net.setup_peer(config)

    job = SharemindJob("sharemind-job", 1, {1, 2, 3})
    jobs = [job]

    salmon.dispatch.dispatch_all(peer, jobs)
