from subprocess import call


class SparkDispatcher():
    '''
    Dispatches Spark jobs
    '''
    def __init__(self):
        pass

    def dispatch(self, job):

        cmd = "{0}/{1}.sh"\
            .format(job.root_dir, job.name)

        print("Running script {}.sh located at: {}"
              .format(job.name, job.root_dir)
              )

        try:
            call(["bash", cmd])
        except Exception as e:
            print(e)






