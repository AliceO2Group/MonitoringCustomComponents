#!/usr/bin/env python

import urllib2
import json

try:
    import logging
    logger = logging.getLogger('SparkSensor')
except Exception as e:
    logger = None


class SparkSensor(object):
    """
    Class used to collect monitoring data from Apache Spark drivers
    """

    def __init__(self, conf):
        """
        Import configuration
        """

        if type(conf) != dict:
            raise Exception("Configuration file is not a dictionary")

        if "endpoints" not in conf:
            raise Exception("'endpoints' keyword not in the configuration")

        self.__endpoints = self.__importEndpoints(conf)
        self.__dIds = dict()
        self.__IsIdsUpdated = False

    def __importEndpoints(self, conf):
        return [i["endpoint"] for i in conf["endpoints"]]

    def __getDataFromUrl(self, url):
        """
        Send the data request to the endpoint.
        Returns list of received data
        """

        try:
            req = urllib2.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            con = urllib2.urlopen(req)
            results = json.loads(con.read())
        except Exception as e:
            msg = "Impossible get stats data from url: {0}  \
            Exception: {1}".format(url, e)
            if logger is not None:
                logger.warning(msg)
            else:
                print(msg)
            return list()
        else:
            return results

    def importIds(self):
        """
        Extracts the spark job ids from input JSON
        """

        nCollectedIds = 0
        lIds = list()
        for endpoint in self.__endpoints:
            url = "http://{0}/api/v1/applications".format(endpoint)
            results = self.__getDataFromUrl(url)
            lIds = [result["id"] for result in results]
            nCollectedIds += len(lIds)
            self.__dIds[endpoint] = lIds

        logger.debug("Collected {0} ids".format(nCollectedIds))

    def getStreamingStatisticsInfo(self, timestampNs):
        """
        Parse the input JSON to extract Streaming statistics
        """

        lExcludeField = ["startTime"]
        lMeas = list()
        for endpoint, lIds in self.__dIds.iteritems():
            baseUrl = "http://{0}/api/v1/applications/".format(endpoint)
            for sparkId in lIds:
                url = baseUrl+"{0}/streaming/statistics".format(sparkId)
                result = self.__getDataFromUrl(url)
                try:
                    dMeas = dict()
                    dMeas["tags"] = dict()
                    dMeas["fields"] = dict()
                    dMeas["time"] = timestampNs
                    dMeas["measurement"] = "spark_streaming_stats"
                    dMeas["tags"]["id"] = sparkId
                    for k1, v1 in result.iteritems():
                        if k1 in lExcludeField:
                            continue
                        dMeas["fields"][k1] = int(v1)
                    lMeas.append(dMeas)
                except Exception as e:
                    msg = "Error during data parsing.\
                     Exception: {0}".format(e)
                    logger.warning(msg)
                    return list()
        return lMeas

    def getStreamingBatchesInfo(self):
        """
        Parse the input JSON to extract Streaming Batches information
        """

        lExcludeField = ["batchId", "batchTime"]
        lTags = ["status"]
        lMeas = list()
        for endpoint, lIds in self.__dIds.iteritems():
            baseUrl = "http://{0}/api/v1/applications/".format(endpoint)
            for sparkId in lIds:
                url = baseUrl+"{0}/streaming/batches".format(sparkId)
                results = self.__getDataFromUrl(url)
                for result in results:
                    batchId = int(result["batchId"])
                    try:
                        dMeas = dict()
                        dMeas["tags"] = dict()
                        dMeas["fields"] = dict()
                        dMeas["time"] = int(batchId)*1000000
                        dMeas["measurement"] = "spark_streaming_batch"
                        dMeas["tags"]["id"] = sparkId
                        dMeas["tags"]["status"] = str(result["status"])
                        for k1, v1 in result.iteritems():
                            if k1 in lExcludeField:
                                continue
                            elif k1 in lTags:
                                dMeas["tags"][k1] = str(v1)
                            else:
                                dMeas["fields"][k1] = int(v1)
                        lMeas.append(dMeas)
                    except Exception as e:
                        msg = "Error during data parsing.\
                         Exception: {0}".format(e)
                        logger.warning(msg)
                        return list()
        return lMeas

    def getActiveExecutorInfo(self, timestampNs):
        """
        Parse the input JSON to extract active Extractors information
        """

        lMeas = list()
        lExcludeField = ["id", "hostPort", "executorLogs"]
        lTags = ["isActive", "isBlacklisted"]
        for endpoint, lIds in self.__dIds.iteritems():
            baseUrl = "http://{0}/api/v1/applications/".format(endpoint)
            for sparkId in lIds:
                url = baseUrl+"{0}/executors".format(sparkId)
                results = self.__getDataFromUrl(url)
                for result in results:
                    try:
                        dMeas = dict()
                        dMeas["tags"] = dict()
                        dMeas["fields"] = dict()
                        dMeas["time"] = timestampNs
                        dMeas["measurement"] = "spark_active_executors"
                        dMeas["tags"]["id"] = sparkId
                        dMeas["tags"]["hostPort"] = str(result["hostPort"])
                        for k1, v1 in result.iteritems():
                            if type(v1) == dict:
                                for k2, v2 in v1.iteritems():
                                    dMeas["fields"][k2] = int(v2)
                            else:
                                if k1 in lExcludeField:
                                    continue
                                elif k1 in lTags:
                                    dMeas["tags"][k1] = str(v1)
                                else:
                                    dMeas["fields"][k1] = int(v1)
                        lMeas.append(dMeas)
                    except Exception as e:
                        msg = "Error during the data parsing.\
                         Exception: {0}".format(e)
                        logger.warning(msg)
                        return list()
        return lMeas

    def getAllExecutorInfo(self, timestampNs):
        """
        Parse the input JSON to extract all Extractors information
        """

        lMeas = list()
        lExcludeField = ["id", "hostPort", "executorLogs"]
        lTags = ["isActive", "isBlacklisted"]
        for endpoint, lIds in self.__dIds.iteritems():
            baseUrl = "http://{0}/api/v1/applications/".format(endpoint)
            for sparkId in lIds:
                url = baseUrl+"{0}/allexecutors".format(sparkId)
                results = self.__getDataFromUrl(url)
                for result in results:
                    try:
                        dMeas = dict()
                        dMeas["tags"] = dict()
                        dMeas["fields"] = dict()
                        dMeas["time"] = timestampNs
                        dMeas["measurement"] = "spark_all_executors"
                        dMeas["tags"]["id"] = sparkId
                        dMeas["tags"]["hostPort"] = str(result["hostPort"])
                        for k1, v1 in result.iteritems():
                            if type(v1) == dict:
                                for k2, v2 in v1.iteritems():
                                    dMeas["fields"][k2] = int(v2)
                            else:
                                if k1 in lExcludeField:
                                    continue
                                elif k1 in lTags:
                                    dMeas["tags"][k1] = str(v1)
                                else:
                                    dMeas["fields"][k1] = int(v1)
                        lMeas.append(dMeas)
                    except Exception as e:
                        msg = "Error during the data parsing.\
                        Exception: {0}".format(e)
                        logger.warning(msg)
                        return list()

        return lMeas

    def collectData(self, timestampNs):
        """
        Collects all monitoring data
        Return: list of measurements
        """

        lMeas = list()
        try:
            self.importIds()
        except Exception as e:
            logger.error("Impossible retrieve data from endpoints."
                         + " Expection: {%s}", e)
        else:
            try:
                lMeas += self.getActiveExecutorInfo(timestampNs)
                lMeas += self.getAllExecutorInfo(timestampNs)
                lMeas += self.getStreamingBatchesInfo()
                lMeas += self.getStreamingStatisticsInfo(timestampNs)
            except Exception as e:
                logger.error("Impossible retrieve data from endpoints."
                             + " Expection: {%s}", e)

        return lMeas
