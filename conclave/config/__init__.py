""" Conclave configuration objects. """
import os
import tempfile


"""
*** EXAMPLE INVOCATION ***
TODO this isn't up-to-date
net_conf = NetworkConfig(
    ["ca-spark-node-0", "cb-spark-node-0", "cc-spark-node-0"],
    [8020, 8020, 8020],
    1
)

spark_conf = SparkConfig("spark://ca-spark-node-0:8020")

sharemind_conf = SharemindCodeGenConfig("/mnt/data")

config_all = CodeGenConfig("big_job"). \
    with_sharemind_config(sharemind_conf). \
    with_spark_config(spark_conf). \
    with_network_config(net_conf)
    
"""


class NetworkConfig:
    """ Config object for network module. """

    def __init__(self, parties: list, pid: int = 1):
        """ Initialize NetworkConfig object. """

        self.inited = True
        self.pid = pid
        # List of HDFS master nodes. Mapping between party running the computation
        # and their own master node / port is indicated in network_config['parties'],
        # where the PID corresponds to each tuple.
        self.parties = parties

    def set_network_config(self):
        """ Return network configuration dict. """

        network_config = {
            "pid": self.pid,
            "parties": {
                1: {"host": self.parties[0]['host'], "port": self.parties[0]['port']},
                2: {"host": self.parties[1]['host'], "port": self.parties[1]['port']},
                3: {"host": self.parties[2]['host'], "port": self.parties[2]['port']}
            }
        }

        return network_config


class SharemindCodeGenConfig:
    """ Sharemind configuration. """

    def __init__(self, home_path='/tmp', use_docker=True, use_hdfs=True):
        """ Initialize SharemindCodeGenConfig object. """

        self.home_path = home_path
        self.use_docker = use_docker
        self.use_hdfs = use_hdfs


class SparkConfig:
    """ Spark configuration."""

    def __init__(self, spark_master_url):
        """ Initialize SparkConfig object. """

        self.spark_master_url = spark_master_url


class CodeGenConfig:
    """ Config object for code generation module. """

    def __init__(self, job_name: [str, None] = None, pid: int = 1):
        """ Initialize CodeGenConfig object. """

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
        self.pid = pid
        self.all_pids = [1, 2, 3]
        self.network_config = {
            "pid": pid,
            "parties": {
                1: {"host": "localhost", "port": 9001},
                2: {"host": "localhost", "port": 9002},
                3: {"host": "localhost", "port": 9003}
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

    def with_spark_config(self, cfg: SparkConfig):
        """ Add SparkConfig object to this object. """

        if not self.inited:
            self.__init__()
        self.system_configs["spark"] = cfg

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


