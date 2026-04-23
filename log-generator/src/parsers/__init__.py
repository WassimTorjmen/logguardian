from .linux_parser import LinuxParser
from .ssh_parser import SSHParser
from .hadoop_parser import HadoopParser
from .spark_parser import SparkParser
from .supercomputer_parser import SupercomputerParser
from .hdfs_parser import HDFSParser

ALL_PARSERS = {
    "linux": LinuxParser,
    "ssh": SSHParser,
    "hadoop": HadoopParser,
    "spark": SparkParser,
    "supercomputer": SupercomputerParser,
    "hdfs": HDFSParser,
}
