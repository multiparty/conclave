
class Job:
    """
    Top level Job class.
    """

    def __init__(self, name: str, code_dir: str):

        self.name = name
        self.code_dir = code_dir
        # set skip to True if dispatching party is not involved in it
        self.skip = False


class SharemindJob(Job):
    """
    Job subclass for Sharemind jobs.
    """

    def __init__(self, name: str, code_dir: str, controller: int, input_parties: list):

        super(SharemindJob, self).__init__(name, code_dir)
        self.controller = controller
        self.input_parties = input_parties


class SparkJob(Job):
    """
    Job subclass for Spark jobs.
    """

    def __init__(self, name: str, code_dir: str):

        super(SparkJob, self).__init__(name, code_dir)


class PythonJob(Job):
    """
    Job subclass for Python jobs.
    """

    def __init__(self, name: str, code_dir: str):

        super(PythonJob, self).__init__(name, code_dir)


class SinglePartyJob(Job):

    def __init__(self, name: str, code_dir: str, fmwk: str, compute_party: int, input_parties: list):

        super(SinglePartyJob, self).__init__(name, code_dir)
        self.fmwk = fmwk
        self.compute_party = compute_party
        self.input_parties = input_parties


class JiffJob(Job):
    """
    Jiff subclass for jiff jobs.
    """

    def __init__(self, name: str, code_dir: str):

        super(JiffJob, self).__init__(name, code_dir)


class OblivCJob(Job):
    """
    Job subclass for Obliv-C jobs.
    """

    def __init__(self, name: str, code_dir: str, input_parties: [list, None] = None,
                 submit_party: int = 1, evaluator_party: int = 2):

        super(OblivCJob, self).__init__(name, code_dir)
        self.submit_party = submit_party
        self.evaluator_party = evaluator_party

        if input_parties is None:
            self.input_parties = [1, 2]
        else:
            self.input_parties = input_parties
