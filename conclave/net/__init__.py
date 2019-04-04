import asyncio
import functools
import pickle


class IAMMsg:
    """ Message identifying peer. """

    def __init__(self, pid: int):
        self.pid = pid

    def __str__(self):
        return "IAMMsg({})".format(self.pid)


class DoneMsg:
    """ Message signifying that peer has finished a task. """

    def __init__(self, pid: int, task_name: str):
        self.pid = pid
        self.task_name = task_name

    def __str__(self):
        return "DoneMsg({})".format(self.pid)


class FailMsg:
    """ Message signifying that peer failed to complete a task. """

    # TODO
    pass


class SalmonProtocol(asyncio.Protocol):
    """
    The Salmon network protocol defines what messages salmon
    peers can send each other and how to interpret these.
    """

    def __init__(self, peer):
        """ Initialize SalmonProtocol object. """

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

        print("iam msg received", iam_msg)
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

        print("done msg received", done_msg)
        if self.peer.dispatcher:
            self.peer.dispatcher.receive_msg(done_msg)
        else:
            self.peer.msg_buffer.append(done_msg)

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


class SalmonPeer:
    """
    A salmon network peer exposes networking functionality. Used to transfer
    messages to other peers and forward the received messages to the other peers.
    """

    def __init__(self, loop, config):

        self.pid = config["pid"]
        self.parties = config["parties"]
        self.host = self.parties[self.pid]["host"]
        self.port = self.parties[self.pid]["port"]
        self.peer_connections = {}
        self.dispatcher = None
        self.msg_buffer = []
        self.server = loop.create_server(
            lambda: SalmonProtocol(self),
            host=self.host, port=self.port)
        self.loop = loop

    def register_dispatcher(self, dispatcher):

        # HACK
        # having a message buffer per connection might
        # make more sense
        self.dispatcher = dispatcher
        # early messages got buffered so we need to
        # forward them to newly-registered the dispatcher
        for msg in self.msg_buffer:
            print("msgmsg", msg)
            if isinstance(msg, DoneMsg):
                self.dispatcher.receive_msg(msg)
        self.msg_buffer = [msg for msg in self.msg_buffer if isinstance(msg, DoneMsg)]

    def connect_to_others(self):

        @asyncio.coroutine
        def _create_connection_retry(f, other_host, other_port):
            while True:
                conn = None
                try:
                    conn = yield from self.loop.create_connection(f, other_host, other_port)
                except OSError:
                    print("Retrying connection to {} {}".format(other_host, other_port))
                    yield from asyncio.sleep(1)
                else:
                    return conn

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
                # using deprecated asyncio.async for 3.4.3 support
                conn = asyncio.async(_create_connection_retry(
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
        self.loop.run_until_complete(asyncio.gather(*to_wait_on))
        # prevent new incoming connections
        self.server.close()
        self.loop.run_until_complete(self.server.wait_closed())
        # TODO: think about another round of synchronization here
        # done connecting
        # unwrap futures that hold ready connections
        for pid in self.peer_connections:
            completed_future = self.peer_connections[pid]
            # the result is a (transport, protocol) tuple
            # we only want the transport
            self.peer_connections[pid] = completed_future.result()[0]

    def _send_msg(self, receiver, msg):

        # sends formatted message
        formatted = pickle.dumps(msg) + b"\n\n\n"
        self.peer_connections[receiver].write(formatted)

    def send_done_msg(self, receiver, task_name):

        # sends message indicating task completion
        done_msg = DoneMsg(self.pid, task_name)
        self._send_msg(receiver, done_msg)


def setup_peer(config):
    """
    Creates a peer and connects peer to all other peers. Blocks until connection succeeds.
    :param config: network configuration
    :return: connected peer
    """
    loop = asyncio.get_event_loop()
    peer = SalmonPeer(loop, config)
    peer.server = loop.run_until_complete(peer.server)
    peer.connect_to_others()
    return peer
