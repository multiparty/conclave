import asyncio
import json
import time
from subprocess import call

import pystache


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

        with open("{0}/header_params.json".format(job.code_dir), 'r') as conf:
            params = json.load(conf)

        row_count = 0
        with open(params["IN_PATH"], 'r') as input_data:
            file_data = input_data.read()
            rows = file_data.split("\n")
            for r in rows:
                if r != '':
                    row_count += 1
            cols = len(rows[0].split(","))

        data = {
            "TYPE": params["TYPE"],
            "ROWS": row_count - 1,
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

    def dispatch_as_evaluator(self, job):
        """
        Wait until submit party is ready.
        """

        if job.submit_party not in self.early:
            self.to_wait_on[job.submit_party] = asyncio.Future()

        future = self.to_wait_on.values()
        self.loop.run_until_complete(asyncio.gather(*future))

        time.sleep(10)
        self._dispatch(job)

    def dispatch(self, job):

        # register self as current dispatcher with peer
        self.peer.register_dispatcher(self)

        if int(self.peer.pid) == int(job.submit_party):
            print("Dispatching as Garbler.\n")
            self.peer.send_done_msg(job.evaluator_party, job.name + '.submit')
            self._dispatch(job)
        elif int(self.peer.pid) == int(job.evaluator_party):
            print("Dispatching as Evaluator.\n")
            self.dispatch_as_evaluator(job)
        else:
            print("Weird PID: {}".format(self.peer.pid))

        self.peer.dispatcher = None
        self.to_wait_on = {}
        self.early = set()

    def receive_msg(self, msg):
        """ Receive message from other party in computation. """

        done_peer = msg.pid
        if done_peer in self.to_wait_on:
            print("Obliv-C DoneMsg received.\n")
            self.to_wait_on[done_peer].set_result(True)
        else:
            self.early.add(done_peer)
            print("early message", msg)
