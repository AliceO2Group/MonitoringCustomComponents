#!/usr/bin/env python

import urllib2
try:
    import json
except:
    import simplejson as json

try:
    import logging
    logger = logging.getLogger('FlumeSensor')
except Exception as e:
    logger = None


class FlumeSensor(object):
    """
    Class used to collect monitoring data from Apache Flume agents
    """

    def __init__(self, conf):
        """
        Import configuration
        """

        if type(conf) != dict:
            raise Exception("Configuration file is not a dictionary")

        if "endpoints" not in conf:
            raise Exception("'endpoints' keyword not in the configuration")

        self.conf = dict()
        self.conf["timeout"] = 10

        for k, v in conf.iteritems():
            self.conf[k] = v

    def collectData(self, timestampNs):
        """
        Collects Apache Flume monitoring data from the agent HTTP endpoint
        Return: list of monitoring data
        """

        active_hosts = 0
        active_components = 0
        lMeas = list()
        for agent in self.conf["endpoints"]:
            url = "http://{}/metrics".format(agent["endpoint"])
            try:
                result = json.load(
                        urllib2.urlopen(url, timeout=self.conf["timeout"]))
            except Exception as e:
                msg = "Impossible get data from: {0}".format(agent["name"])
                msg += " Exception: {0}".format(e)
                if logger is not None:
                    logger.warning(msg)
                else:
                    print(msg)
                continue
            else:
                active_hosts += 1
                for item in result.keys():
                    active_components += 1
                    dMeas = dict()
                    dMeas["tags"] = dict()
                    dMeas["time"] = timestampNs
                    dMeas["fields"] = dict()
                    dMeas["tags"]["hostname"] = agent["name"]
                    dMeas["tags"]["endpoint"] = agent["endpoint"]
                    dMeas["tags"]["component"] = item
                    for key in result[item].keys():
                        if key != "Type":
                            dMeas["fields"][key] = eval(result[item][key])
                        else:
                            dMeas["measurement"] = 'flume_'+result[item][key]
                    lMeas.append(dMeas)
        msg = "Collected {} metrics from flume endpoints".format(len(lMeas))
        logger.debug(msg)
        return lMeas
