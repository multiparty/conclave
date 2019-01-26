""" Conclave configuration objects. """
import os
import tempfile


class NetworkConfig:
    """ Config object for network module. """

    def __init__(self, parties: list, pid: int = 1):

        self.inited = True
        self.pid = pid
        # List of HDFS master nodes. Mapping between party running the computation
        # and their own master node / port is indicated in network_config['parties'],
        # where the PID corresponds to each tuple.
        self.parties = parties

    def set_network_config(self):
        """ Return network configuration dict. """

        network_config = {}
        network_config['pid'] = self.pid
        network_config["parties"] = {}
        for i in range(len(self.parties)):
            network_config["parties"][i + 1] = {}
            network_config["parties"][i + 1]["host"] = self.parties[i]["host"]
            network_config["parties"][i + 1]["port"] = self.parties[i]["port"]

        return network_config


class SwiftConfig:
    """
    Configuration for accessing data from Swift.
    """

    def __init__(self, cfg):

        self.source = cfg['source']
        self.dest = cfg['dest']


class SharemindCodeGenConfig:
    """ Sharemind configuration. """

    def __init__(self, home_path='/tmp', use_docker=True, use_hdfs=True):

        self.home_path = home_path
        self.use_docker = use_docker
        self.use_hdfs = use_hdfs


class SparkConfig:
    """ Spark configuration."""

    def __init__(self, spark_master_url):

        self.spark_master_url = spark_master_url


class OblivcConfig:
    """
    Obliv-c configuration.
    """

    def __init__(self, oc_path: str, ip_and_port: str):

        self.oc_path = oc_path
        self.ip_and_port = ip_and_port


class CodeGenConfig:
    """ Config object for code generation module. """

    def __init__(self, job_name: [str, None] = None, pid: int = 1):
        """ Initialize CodeGenConfig object. """

        self.inited = True
        self.delimiter = ','
        if job_name is not None:
            self.name = job_name
            self.code_path = "/tmp/{}-code/".format(job_name)
        else:
            self.code_path = tempfile.mkdtemp(suffix="-code", prefix="salmon-")
            self.name = os.path.basename(self.code_path)
        self.use_leaky_ops = True
        self.use_swift = False
        self.input_path = '/tmp'
        self.output_path = '/tmp'
        self.system_configs = {}
        self.pid = pid
        self.all_pids = [1, 2, 3]
        self.network_config = {
            "pid": pid,
            "parties": {
                1: {"host": "ca-spark-node-0", "port": 9001},
                2: {"host": "cb-spark-node-0", "port": 9002},
                3: {"host": "cc-spark-node-0", "port": 9003}
            }
        }

    def with_pid(self, pid: int):
        """ Change pid of this party (default is 1). """

        if not self.inited:
            self.__init__()

        self.pid = pid

        return self

    def with_delimiter(self, delimiter: str):
        """ Set delimiter for input files (default is ','). """

        if not self.inited:
            self.__init__()

        self.delimiter = delimiter

        return self

    def with_sharemind_config(self, cfg: SharemindCodeGenConfig):
        """ Add SharemindCodeGenConfig object to this object. """

        if not self.inited:
            self.__init__()

        self.system_configs["sharemind"] = cfg

        return self

    def with_swift_config(self, cfg: SwiftConfig):

        if not self.inited:
            self.__init__()

        self.system_configs["swift"] = cfg

        return self

    def with_spark_config(self, cfg: SparkConfig):
        """ Add SparkConfig object to this object. """

        if not self.inited:
            self.__init__()

        self.system_configs["spark"] = cfg

        return self

    def with_oc_config(self, cfg: OblivcConfig):
        """ Add OblivcConfig object to this object. """

        if not self.inited:
            self.__init__()

        self.system_configs["oblivc"] = cfg

        return self

    def with_network_config(self, cfg: NetworkConfig):
        """ Add network config to this object. """

        if not self.inited:
            self.__init__()

        self.network_config = cfg.set_network_config()

        return self

    # TODO: remove?
    def from_dict(self, cfg: dict):
        """ Create config from dict """

        ccfg = CodeGenConfig(cfg['name'])

        ccfg.delimiter = cfg['delimiter']
        ccfg.code_path = cfg['code_path']
        ccfg.input_path = cfg['input_path']
        ccfg.output_path = cfg['output_path']

        ccfg.pid = cfg['pid']

        return ccfg


