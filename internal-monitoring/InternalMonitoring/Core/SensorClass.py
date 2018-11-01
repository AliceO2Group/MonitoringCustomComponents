import os
from InputPlugins.FlumeSensor import FlumeSensor
from InputPlugins.SparkSensor import SparkSensor
from OutputPlugins.UpdInfluxdbSender import UpdInfluxdbSender
from OutputPlugins.TerminalModule import TerminalModule
from Utilities.Daemon import Daemon
from Utilities.LogConfiguration import getLogger
from Utilities.Sched import Sched
from Utilities.Utilities import print_json
from time import time

try:
    import logging
    logger = logging.getLogger('SensorClass')
except Exception as e:
    logger = None


class SensorClass(Daemon):
    log_level_allowed = ["CRITICAL", "ERROR", "WARN", "INFO", "DEBUG"]

    def __init__(self,
                 conf,
                 cmd='exec',
                 defaultAbsOutfile='/tmp/hadoop_mon.out',
                 defaultAbsErrfile='/tmp/hadoop_mon.err',
                 defaultAbsPidfile='/tmp/hadoop_mon.pid',
                 defaultAbsLogfile='/tmp/hadoop_mon.log',
                 defaultLogLevel="INFO"):
        self.conf = conf
        self.currentSamplePeriod = 0
        self.defaultAbsLogfile = defaultAbsLogfile
        self.defaultAbsPidfile = defaultAbsPidfile
        self.defaultLogLevel = defaultLogLevel
        self.cmd = cmd
        self.__importConfiguration()

        if cmd == "debug":
            print_json(conf)
        else:
            Daemon.__init__(self,
                            pidfile=self.pidfile,
                            stdout=defaultAbsOutfile,
                            stderr=defaultAbsErrfile)

    def __importConfiguration(self):
        """
        Import configuration
        """

        if "log" in self.conf:
            logConf = dict()
            logConf["logfile"] = self.conf["log"].get("logfile",
                                                      self.defaultAbsLogfile)
            logConf["loglevel"] = self.conf["log"].get("level",
                                                       self.defaultLogLevel)
            self.logger = getLogger(logConf, debug=self.cmd == 'debug')

        self.pidfile = self.conf.get("pidfile", self.defaultAbsPidfile)
        self.logger.info("pidfile: {0}".format(self.pidfile))

        # If sched is not defined, the code is executed once
        schedConf = dict()
        schedConf["period"] = None
        if "sched" in self.conf:
            if "period" in self.conf["sched"]:
                period = self.conf["sched"]["period"]
                try:
                    schedConf["period"] = int(period)
                    self.logger.info("sched.period: {0}"
                                     .format(schedConf["period"]))
                except Exception as e:
                    self.logger.info("sched.period disabled. Exception %s", e)

        self.sched = Sched(schedConf)

        if "input" in self.conf:
            inputConf = dict()
            inputConf["modules"] = list()
            if type(self.conf["input"]) != list:
                raise Exception("'input' section must be a list")
            else:
                for inputModule in self.conf["input"]:
                    if "name" in inputModule:
                        inputName = inputModule["name"]
                        inputConf["modules"].append(inputName)
                        inputConf[inputName] = None
                    else:
                        raise Exception("'name' parameter must be present")
                    if "conf" in inputModule:
                        inputConf[inputName] = inputModule["conf"]
            self.inputConf = inputConf
        else:
            raise Exception("'input' section not present")

        logger.info("Found the following input module(s): %s",
                    self.inputConf["modules"])

        if "output" in self.conf:
            outputConf = dict()
            outputConf["modules"] = list()
            if type(self.conf["output"]) != list:
                raise Exception("'output' section must be a list")
            else:
                for outputModule in self.conf["output"]:
                    if "name" in outputModule:
                        outputName = outputModule["name"]
                        outputConf["modules"].append(outputName)
                        outputConf[outputName] = None
                    else:
                        raise Exception("'name' parameter must be present")
                    if "conf" in outputModule:
                        outputConf[outputName] = outputModule["conf"]
            self.outputConf = outputConf
        else:
            raise Exception("'output' section not present")
        logger.info("Found the following output module(s): %s",
                    self.outputConf["modules"])

    def task(self, tsNs):
        lMeas = list()
        for module in self.inputConf["modules"]:
            if "flume" == module:
                lMeas += FlumeSensor(self.inputConf[module]).collectData(tsNs)
            elif "spark" == module:
                lMeas += SparkSensor(self.inputConf[module]).collectData(tsNs)

        logger.info("gathered {} metrics".format(len(lMeas)))
        logger.info("output modules: {}".format(self.outputConf["modules"]))

        if self.cmd == "debug":
            TerminalModule().show(lMeas)
        else:
            for module in self.outputConf["modules"]:
                if "influxdb-udp" == module:
                    UpdInfluxdbSender(self.outputConf[module]).send(lMeas)

    def run(self):
        if self.sched.active:
            period = self.sched.getPeriod()
            while(True):
                self.sched.wait()

                preTimeTask = time()
                self.task(int(time()*1000000000))
                postTimeTask = time()

                taskDuration = postTimeTask - preTimeTask
                logger.debug("taskDuration: %s. Period: %s",
                             taskDuration, period)
                if(taskDuration > period * 0.8):
                    logger.warning("Task duration is close the period. %s/%s",
                                   taskDuration, period)
        else:
            self.task()
