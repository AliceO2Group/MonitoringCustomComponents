from sys import argv, exit
from Core.SensorClass import SensorClass
import os
from Utilities.Utilities import readConf


def usage():
    print "usage: {} [-conf conf_path] "\
        "start|stop|restart|debug]".format(argv[0])


if 'http_proxy' in os.environ:
    del os.environ['http_proxy']

if 'https_proxy' in os.environ:
    del os.environ['https_proxy']


default_yaml_file = os.path.dirname(argv[0])+"/conf/conf.yaml"
allowed_command = ["start", "stop", "restart", "debug"]
yaml_file = None
cmd = None

if __name__ == "__main__":
    if len(argv) == 4 and argv[1] == "-conf" and argv[3] in allowed_command:
        yaml_file = argv[2]
        cmd = argv[3]
    elif len(argv) == 2 and argv[1] in allowed_command:
        yaml_file = default_yaml_file
        cmd = argv[1]
    else:
        usage()
        exit(1)

    # Default paths
    dirname = os.path.dirname(os.path.realpath(argv[0]))
    defaultLogfile = argv[0].replace("py", "log")
    defaultAbsLogfile = os.path.normpath(os.path.join(dirname, defaultLogfile))
    defaultPidfile = argv[0].replace("py", "pid")
    defaultAbsPidfile = os.path.normpath(os.path.join(dirname, defaultPidfile))
    defaultOutfile = argv[0].replace("py", "out")
    defaultAbsOutfile = os.path.normpath(os.path.join(dirname, defaultOutfile))
    defaultErrfile = argv[0].replace("py", "err")
    defaultAbsErrfile = os.path.normpath(os.path.join(dirname, defaultErrfile))

    # Import YAML configuration file
    conf = readConf(yaml_file)
    flumeSparkSens = SensorClass(conf,
                                 cmd=cmd,
                                 defaultAbsOutfile=defaultAbsOutfile,
                                 defaultAbsErrfile=defaultAbsErrfile,
                                 defaultAbsLogfile=defaultAbsLogfile,
                                 defaultAbsPidfile=defaultAbsPidfile)

    if cmd == "debug":
        flumeSparkSens.run()
    else:
        if 'start' == cmd:
            flumeSparkSens.start()
        elif 'stop' == cmd:
            flumeSparkSens.stop()
        elif 'restart' == cmd:
            flumeSparkSens.restart()
        else:
            usage()
            exit(1)
