from subprocess import call


class OblivCDispatcher:

    def dispatch(self, job):
        """ Dispatch Spark job. """
        cmd = "{}/bash.sh".format(job.code_dir)

        print("{}: {}/bash.sh dispatching Obliv-C job. "
              .format(job.name, job.code_dir))

        try:
            call(["/bin/bash", cmd])
        except Exception as e:
            print(e)
