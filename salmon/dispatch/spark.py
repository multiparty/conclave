from subprocess import call


class SparkDispatcher():
    '''
    Dispatches Spark jobs
    '''
    def __init__(self, master_url):
        self.master = master_url

    def dispatch(self, job):

        cmd = "{0}/{1}/bash.sh {2}"\
            .format(job.root_dir, job.name, self.master)

        print("{}/{}/bash.sh dispatching to Spark master at {}"
              .format(job.root_dir, job.name, self.master)
              )

        try:
            call(["/bin/bash", cmd])
        except Exception as e:
            print(e)
