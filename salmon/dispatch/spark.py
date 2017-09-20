from subprocess import call


class SparkDispatcher():
    '''
    Dispatches Spark jobs
    '''
    def __init__(self):
        pass

    def dispatch(self, job):

        # can handle spark config options in codegen
        # TODO: configurable inpt/outpt files?
        cmd = "{0}/{1}.py {1}.csv {1}_out.csv"\
            .format(job.root_dir, job.name)

        print("Running script {}.py located at: {}"
              .format(job.name, job.root_dir)
              )

        try:
            call(["spark-submit", cmd])
        except Exception as e:
            print(e)






