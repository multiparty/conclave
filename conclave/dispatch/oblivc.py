import asyncio
from subprocess import call


class OblivCDispatcher:

    def __init__(self, peer):

        self.peer = peer
        self.loop = peer.loop
        self.to_wait_on = {}
        self.early = set()

    def _dispatch(self, job):
        """
        Dispatch Obliv-C job.
        """

        cmd = "{}/bash.sh".format(job.code_dir)

        print("{}: {}/bash.sh dispatching Obliv-C job. "
              .format(job.name, job.code_dir))

        try:
            call(["/bin/bash", cmd])
        except Exception as e:
            print(e)

    def party_one_dispatch(self, job):

        # TODO: clean up
        other_pid = 2
        for pid in job.input_parties:
            if pid != self.peer.pid:
                other_pid = pid

        # notify controller that we're done
        self.peer.send_done_msg(other_pid, job.name + ".party_one")

    def party_two_dispatch(self, job):

        for input_party in job.input_parties:
            if input_party != self.peer.pid and input_party not in self.early:
                self.to_wait_on[input_party] = asyncio.Future()

        # wait until other peers are done submitting
        futures = self.to_wait_on.values()
        self.loop.run_until_complete(asyncio.gather(*futures))

        self._dispatch(job)

        # notify other parties that job is done
        for party in self.peer.parties:
            if party != self.peer.pid:
                self.peer.send_done_msg(party, job.name + ".party_two")

    def dispatch(self, job):

        if self.peer.pid == 1:
            self.party_one_dispatch(job)

        elif self.peer.pid == 2:
            self.party_two_dispatch(job)

        else:
            raise Exception("Party ID {0} out of bounds (must be 1 or 2)".format(self.peer.pid))

        self.peer.dispatcher = None
        self.to_wait_on = {}
        self.early = set()