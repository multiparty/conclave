from subprocess import call


class PythonDispatcher:
    """ Dispatches Python jobs. """

    def dispatch(self, job):

        cmd = "{}/workflow.py".format(job.code_dir)

        print("{}: {}/workflow.py running"
              .format(job.name, job.code_dir))

        try:
            call(["python", cmd])
        except Exception as e:
            print(e)
