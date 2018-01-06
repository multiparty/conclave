import os
import tempfile


class CodeGenConfig:

    def __init__(self, job_name=None):
        self.inited = True
        self.delimiter = ','
        if job_name is not None:
            self.name = job_name
            self.code_path = "/tmp/{}-code".format(job_name)
        else:
            self.code_path = tempfile.mkdtemp(suffix="-code", prefix="salmon-")
            self.name = os.path.basename(self.code_path)
        self.input_path = '/tmp'
        self.output_path = '/tmp'
        self.system_configs = {}
        self.pid = 1

    def with_pid(self, pid):
        if not self.inited:
            self.__init__()
        self.pid = pid

        return self

    def with_delimiter(self, delimiter):
        if not self.inited:
            self.__init__()
        self.delimiter = delimiter

        return self

    def with_sharemind_config(self, cfg):
        if not self.inited:
            self.__init__()
        self.system_configs['sharemind'] = cfg

        return self

    def from_dict(cfg):
        ccfg = CodeGenConfig(cfg['name'])

        ccfg.delimiter = cfg['delimiter']
        ccfg.code_path = cfg['code_path']
        ccfg.input_path = cfg['input_path']
        ccfg.output_path = cfg['output_path']

        ccfg.pid = cfg['pid']

        return ccfg