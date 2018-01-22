
class Job:
    """ Top level Job class. """

    def __init__(self, name: str, code_dir: str):
        """ Initialize Job object. """
        self.name = name
        self.code_dir = code_dir
        # set skip to True if dispatching party is not involved in it
        self.skip = False


class SharemindJob(Job):
    """ Job subclass for Sharemind jobs. """

    def __init__(self, name: str, code_dir: str, controller: int, input_parties: list):
        """ Initialize SharemindJob object. """

        super(SharemindJob, self).__init__(name, code_dir)
        self.controller = controller
        self.input_parties = input_parties


class SparkJob(Job):
    """ Job subclass for Spark jobs. """

    def __init__(self, name: str, code_dir: str):
        """ Initialize SparkJob object. """

        super(SparkJob, self).__init__(name, code_dir)


class PythonJob(Job):
    """ Job subclass for Python jobs. """

    def __init__(self, name: str, code_dir: str):
        """ Initialize PythonJob object. """

        super(PythonJob, self).__init__(name, code_dir)
