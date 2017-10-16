
class Job():

    def __init__(self, name, code_dir):

        self.name = name
        self.code_dir = code_dir
        # set skip to True if dispatching party is not involved in it
        self.skip = False


class SharemindJob(Job):

    def __init__(self, name, code_dir, controller, input_parties):

        super(SharemindJob, self).__init__(name, code_dir)
        self.controller = controller
        self.input_parties = input_parties


class SparkJob(Job):

    def __init__(self, name, code_dir):

        super(SparkJob, self).__init__(name, code_dir)


class PythonJob(Job):

    def __init__(self, name, code_dir):

        super(PythonJob, self).__init__(name, code_dir)
