import asyncio
import pickle
import sys


class DoneMsg():

    def __init__(self, party_id, job_name):

        self.party_id = party_id
        self.job_name = job_name


class PeerConfig():

    def __init__(self, pid, parties):

        self.pid = pid
        self.parties = parties
        self.host, self.port = self.parties[self.pid]


class SalmonProtocol(asyncio.Protocol):
    # defines what messages salmon peers can send to each other

    def __init__(self, peer):

        self.peer = peer
        self.buffer = b""
        self.transport = None

    def connection_made(self, transport):

        self.transport = transport

    def data_received(self, data):

        self.buffer += data
        self.handle_lines()

    def parse_line(self, line):

        msg = None
        try:
            msg = pickle.loads(line)
        except Exception as e:
            raise e
        finally:
            return msg

    def handle_lines(self):

        # using delimiters for now
        # TODO: switch to sending length flags
        while b"\n\n\n" in self.buffer:
            line, self.buffer = self.buffer.split(b"\n\n\n", 1)
            parsed = self.parse_line(line)
            if parsed:
                self.peer.receive_msg(parsed)
            else:
                print("failed to parse line:", line)


class SalmonPeer():
    # handles communication with other peers

    def __init__(self, loop, config):

        self.pid = config["pid"]
        self.parties = config["parties"]
        self.host = self.parties[self.pid]["host"]
        self.port = self.parties[self.pid]["port"]
        self.peer_connections = {}
        self.dispatcher = None
        self.server = loop.create_server(
            lambda: SalmonProtocol(self),
            host=self.host, port=self.port)
        self.loop = loop

    def connect_to_others(self):

        # create future for each peer, run until those are complete
        pids_to_connect = filter(lambda p: p < self.pid, self.parties.keys())
        for other_pid in pids_to_connect:
            other_host = self.parties[other_pid]["host"]
            other_port = self.parties[other_pid]["port"]
            # TODO: handle retrying
            conn = loop.create_connection(
                lambda: SalmonProtocol(self), other_host, other_port)
            loop.run_until_complete(conn)

    def send_msg(self, peer_id, msg):

        pass

    def receive_msg(self, msg):

        if self.dispatcher:
            self.dispatcher.receive_msg(msg)
        else:
            raise Exception("No dispatcher registered.")


class SharemindDispatcher():

    def __init__(self, loop, peer):

        self.loop = loop
        self.peer = peer

    def dispatch(self, job):

        # track which participants have completed data submission
        self.to_wait_on = {}
        for contributor in job.data_contributors:
            self.to_wait_on[contributor] = asyncio.Future()

        # register self as current dispatcher with peer
        peer.dispatcher = self

        # wait until other peers are done submitting
        futures = self.to_wait_on.values()
        loop.run_until_complete(asyncio.gather(*futures))

        # submit job to miners, etc.
        print("proceed")
        from time import sleep
        sleep(5)

        # un-register with dispatcher
        peer.dispatcher = None

    def receive_msg(self, msg):

        if msg in self.to_wait_on:
            self.to_wait_on[msg].set_result(True)
        else:
            print("weird message", msg)


class SharemindJob():

    def __init__(self, data_contributors):

        self.data_contributors = data_contributors


def setup_peer(config):

    loop = asyncio.get_event_loop()
    peer = SalmonPeer(loop, config)
    loop.run_until_complete(peer.server)
    return loop, peer

pid = int(sys.argv[1])
config = {
    "pid": pid,
    "parties": {
        1: {"host": "localhost", "port": 9001},
        2: {"host": "localhost", "port": 9002},
        3: {"host": "localhost", "port": 9003}
    }
}
loop, peer = setup_peer(config)
peer.connect_to_others()
print("done")
# dispatcher = SharemindDispatcher(loop, peer)

# job = SharemindJob({"1", "2", "3"})
# dispatcher.dispatch(job)

# another_job = SharemindJob({"1"})
# dispatcher.dispatch(another_job)
