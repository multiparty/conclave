
class SharemindJob():

    def __init__(self, name, code_dir, controller, input_parties):

        self.name = name
        self.code_dir = code_dir
        self.controller = controller
        self.input_parties = input_parties


class SparkJob():

    def __init__(self, name, code_dir):

        self.name = name
        self.code_dir = code_dir
