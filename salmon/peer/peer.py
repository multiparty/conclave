import asyncio
import functools
import pickle
import sys


class IAMMsg():

    def __init__(self, pid):

        self.pid = pid


class DoneMsg():

    def __init__(self, pid, task_name):

        self.pid = pid
        self.task_name = task_name


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
            print(e)
        return msg

    def _handle_iam_msg(self, iam_msg):

        other_pid = iam_msg.pid
        if other_pid not in self.peer.peer_connections:
            raise Exception(
                "Unknown peer attempting to register: " + str(other_pid))
        conn = self.peer.peer_connections[other_pid]
        if isinstance(conn, asyncio.Future):
            conn.set_result((self.transport, self))
        else:
            raise Exception("Unexpected peer registration attempt")

    def _handle_done_msg(self, done_msg):

        if self.peer.dispatcher:
            self.peer.dispatcher.receive_msg(done_msg)
        else:
            raise Exception("No dispatcher registered")

    def handle_msg(self, msg):

        if isinstance(msg, IAMMsg):
            self._handle_iam_msg(msg)
        elif isinstance(msg, DoneMsg):
            self._handle_done_msg(msg)
        else:
            raise Exception("Weird message: " + str(msg))

    def handle_lines(self):

        # using delimiters for now
        # TODO: switch to sending length flags
        while b"\n\n\n" in self.buffer:
            line, self.buffer = self.buffer.split(b"\n\n\n", 1)
            parsed = self.parse_line(line)
            if parsed:
                self.handle_msg(parsed)
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

        def _send_IAM(pid, conn):

            msg = IAMMsg(pid)
            formatted = pickle.dumps(msg) + b"\n\n\n"
            transport, protocol = conn.result()
            transport.write(formatted)

        to_wait_on = []
        for other_pid in self.parties.keys():
            if other_pid < self.pid:
                other_host = self.parties[other_pid]["host"]
                other_port = self.parties[other_pid]["port"]
                print("Will connect to {} at {}:{}".format(
                    other_pid, other_host, other_port))
                # create connection
                conn = asyncio.ensure_future(self.loop.create_connection(
                    lambda: SalmonProtocol(self), other_host, other_port))
                self.peer_connections[other_pid] = conn
                # once connection is ready, register own ID with other peer
                conn.add_done_callback(functools.partial(_send_IAM, self.pid))
                # TODO: figure out way to wait on message delivery
                # instead of on connection
                to_wait_on.append(conn)
            elif other_pid > self.pid:
                print("Will wait for {} to connect".format(other_pid))
                # expect connection from other peer
                connection_made = asyncio.Future()
                self.peer_connections[other_pid] = connection_made
                to_wait_on.append(connection_made)
        self.loop.run_until_complete(asyncio.gather(
            *to_wait_on))
        # done connecting
        # unwrap futures that hold ready connections
        for pid in self.peer_connections:
            completed_future = self.peer_connections[pid]
            # the result is a (transport, protocol) tuple
            # we only want the transport
            self.peer_connections[pid] = completed_future.result()[0]
        print(self.peer_connections)

    def send_msg(self, receiver, msg):

        formatted = pickle.dumps(msg) + b"\n\n\n"
        self.peer_connections[receiver].write(formatted)


class SharemindDispatcher():

    def __init__(self, loop, peer):

        self.loop = loop
        self.peer = peer
        self.to_wait_on = {}

    def _dispatch_as_controller(self, job):

        # track which participants have completed data submission
        for input_party in job.input_parties:
            if input_party != self.peer.pid:
                self.to_wait_on[input_party] = asyncio.Future()

        # register self as current dispatcher with peer
        peer.dispatcher = self

        # wait until other peers are done submitting
        futures = self.to_wait_on.values()
        loop.run_until_complete(asyncio.gather(*futures))

        # submit job to miners, etc.
        print("proceed")


    def _regular_dispatch(self, job):

        # register self as current dispatcher with peer
        peer.dispatcher = self

        # mock work
        import time
        time.sleep(self.peer.pid * 2)

        # notify controller that we're done
        done_msg = DoneMsg(self.peer.pid, job.name + ".input")
        self.peer.send_msg(job.controller, done_msg)

        # wait on controller to confirm that the job has finished
        self.to_wait_on = {job.controller: asyncio.Future()}
        loop.run_until_complete(self.to_wait_on[job.controller])


    def dispatch(self, job):

        if self.peer.pid == job.controller:
            self._dispatch_as_controller(job)
        else:
            self._regular_dispatch(job)
        # un-register with dispatcher
        peer.dispatcher = None
        # not waiting on any peers
        self.to_wait_on = {}

    def receive_msg(self, msg):

        done_peer = msg.pid
        if done_peer in self.to_wait_on:
            self.to_wait_on[done_peer].set_result(True)
        else:
            print("weird message", msg)


class SharemindJob():

    def __init__(self, name, controller, input_parties):

        self.name = name
        self.controller = controller
        self.input_parties = input_parties


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
dispatcher = SharemindDispatcher(loop, peer)

job = SharemindJob("sharemind-job", 1, {1, 2, 3})
dispatcher.dispatch(job)
