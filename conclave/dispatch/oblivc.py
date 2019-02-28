import asyncio
import time
import json
import pystache

from subprocess import call


class OblivCDispatcher:

    def __init__(self, peer, config):

        self.peer = peer
        self.config = config
        self.loop = peer.loop
        self.to_wait_on = {}
        self.early = set()
        self.header_template = \
            """
            #include <obliv.h>
            #include <obliv.oh>
            
            #define ROWS {{{ROWS}}}
            #define COLS {{{COLS}}}
            
            typedef struct
            {
                {{{TYPE}}} mat[ROWS][COLS];
                int rows;
                int cols;
            } Io;
            
            typedef struct
            {
                char *src;
                char *out;
                Io in;
                int out_rows;
                int out_cols;
                {{{TYPE}}} **ret;
            
            } protocolIo;
            
            typedef struct
            {
                int rows;
                int cols;
                obliv {{{TYPE}}} *keepRows;
                obliv {{{TYPE}}} **mat;
            
            } intermediateMat;
            
            void protocol(void *args);
            """

    def generate_header(self, job):

        print(job.code_dir)

        with open("{0}/header_params.json".format(job.code_dir), 'r') as conf:
            params = json.load(conf)

        with open(params["IN_PATH"], 'r') as input_data:
            file_data = input_data.read()
            rows = file_data.split("\n")
            row_count = 0
            for r in rows:
                if r != '':
                    row_count += 1
            cols = len(rows[0].split(","))

        data = {
            "TYPE": params["TYPE"],
            "ROWS": len(row_count) - 1,
            "COLS": cols
        }

        header_file = pystache.render(self.header_template, data)

        print("***\n\nWriting header file here {}\n\n***".format(job.code_dir))
        header = open("{}/workflow.h".format(job.code_dir), 'w')
        header.write(header_file)

    def _dispatch(self, job):
        """
        Dispatch Obliv-C job.
        """

        self.generate_header(job)

        cmd = "{}/bash.sh".format(job.code_dir)

        print("{}: {}/bash.sh dispatching Obliv-C job. "
              .format(job.name, job.code_dir))

        try:
            call(["/bin/bash", cmd])
        except Exception as e:
            print(e)

    def party_one_dispatch(self, job):

        self.peer.send_done_msg(2, job.name + ".party_one")

        self._dispatch(job)

    def party_two_dispatch(self, job):

        for input_party in job.input_parties:
            if input_party != self.peer.pid and input_party not in self.early:
                self.to_wait_on[input_party] = asyncio.Future()

        futures = self.to_wait_on.values()
        self.loop.run_until_complete(asyncio.gather(*futures))

        # hack
        time.sleep(5)

        self._dispatch(job)

    def dispatch(self, job):

        # register self as current dispatcher with peer
        self.peer.register_dispatcher(self)

        if int(self.peer.pid) == 1:
            self.party_one_dispatch(job)

        elif int(self.peer.pid) == 2:
            self.party_two_dispatch(job)

        else:
            raise Exception("Party ID {0} out of bounds (must be 1 or 2)".format(self.peer.pid))

        self.peer.dispatcher = None
        self.to_wait_on = {}
        self.early = set()

    def receive_msg(self, msg):
        """ Receive message from other party in computation. """

        done_peer = msg.pid
        if done_peer in self.to_wait_on:
            self.to_wait_on[done_peer].set_result(True)
        else:
            self.early.add(done_peer)
            print("early message", msg)
