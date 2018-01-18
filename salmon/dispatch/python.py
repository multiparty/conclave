from subprocess import call
from salmon.job import PythonJob


class PythonDispatcher:
    """ Dispatches Python jobs. """

    def dispatch(self, job: PythonJob):

        cmd = "{}/workflow.py".format(job.code_dir)

        print("{}: {}/workflow.py running"
              .format(job.name, job.code_dir))

        try:
            call(["python3", cmd])
        except Exception as e:
            print(e)
