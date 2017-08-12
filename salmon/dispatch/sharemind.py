import asyncio
from subprocess import call

class SharemindDispatcher():
    '''
    Dispatches sharemind jobs
    '''

    def __init__(self, peer):

        self.peer = peer
        self.loop = peer.loop
        self.to_wait_on = {}

    def _input_data(self, job):

        cmd = "{}/{}/input.sh".format(
            job.root_dir,
            job.name
        )
        print("Will run data submission: " + cmd)
        call(["bash", cmd])

    def _submit_to_miners(self, job):

        cmd = "{}/{}/controller.sh".format(
            job.root_dir,
            job.name
        )
        print("Will submit jobs to miners: " + cmd)
        call(["bash", cmd])

    def _dispatch_as_controller(self, job):

        # track which participants have completed data submission
        for input_party in job.input_parties:
            if input_party != self.peer.pid:
                self.to_wait_on[input_party] = asyncio.Future()

        # wait until other peers are done submitting
        futures = self.to_wait_on.values()
        self.loop.run_until_complete(asyncio.gather(*futures))

        # submit data to miners
        self._input_data(job)

        # submit job to miners
        self._submit_to_miners(job)

        # notify other parties that job is done
        for input_party in job.input_parties:
            if input_party != self.peer.pid:
                self.peer.send_done_msg(input_party, job.name + ".controller")

        print("done")

    def _regular_dispatch(self, job):

        # submit data to miners
        self._input_data(job)

        # notify controller that we're done
        self.peer.send_done_msg(job.controller, job.name + ".input")

        # wait on controller to confirm that the job has finished
        self.to_wait_on = {job.controller: asyncio.Future()}
        self.loop.run_until_complete(self.to_wait_on[job.controller])

    def dispatch(self, job):

        # register self as current dispatcher with peer
        self.peer.dispatcher = self

        if self.peer.pid == job.controller:
            self._dispatch_as_controller(job)
        else:
            self._regular_dispatch(job)
        # un-register with dispatcher
        self.peer.dispatcher = None
        # not waiting on any peers
        self.to_wait_on = {}

    def receive_msg(self, msg):

        done_peer = msg.pid
        if done_peer in self.to_wait_on:
            self.to_wait_on[done_peer].set_result(True)
        else:
            print("weird message", msg)
