from subprocess import call


class SparkDispatcher:
    """ Dispatches Spark jobs. """

    def __init__(self, master_url):
        """ Initialize SparkDispatcher object """
        self.master = master_url

    def dispatch(self, job):
        """ Dispatch Spark job. """
        cmd = "{}/bash.sh".format(job.code_dir)

        print("{}: {}/bash.sh dispatching to Spark master at {}"
              .format(job.name, job.code_dir, self.master))

        try:
            call(["/bin/bash", cmd, self.master])
        except Exception as e:
            print(e)
