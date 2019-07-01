from time import time

try:
    import logging
    logger = logging.getLogger('TerminalModule')
except Exception as e:
    logger = None


class TerminalModule(object):
    def __init__(self):
        self.wt1_s = int(time()-7*24*60*60)  # now minus 1w
        self.wt2_s = int(time()+7*24*60*60)  # now plus 1w
        self.wt1_ms = self.wt1_s * 1000
        self.wt2_ms = self.wt2_s * 1000
        self.wt1_us = self.wt1_ms * 1000
        self.wt2_us = self.wt2_ms * 1000
        self.wt1_ns = self.wt1_us * 1000
        self.wt2_ns = self.wt2_us * 1000

    def castValue(self, value):
        if type(value) == int:
            return str(value)+"i"
        if type(value) == float:
            return str(value)
        if type(value) == bool:
            if value is True:
                return "true"
            else:
                return "false"
        return "\""+value+"\""

    def convertTimestamp(self, timestamp):
        mytime = int(timestamp)
        time_ns = str(mytime)
        if mytime < self.wt2_s and mytime >= self.wt1_s:
            time_ns += "000000000"
        elif mytime < self.wt2_ms and mytime >= self.wt1_ms:
            time_ns += "000000"
        elif mytime < self.wt2_us and mytime >= self.wt1_us:
            time_ns += "000"
        elif mytime < self.wt2_ns and mytime >= self.wt1_ns:
            pass
        else:
            while(len(time_ns) < 20):
                time_ns += '0'
            msg = "Impossible to decode timestamp: {0}.".format(mytime)
            msg += " Manually decoded to {0}".format(time_ns)
            if logger is not None:
                logger.warn(msg)
            else:
                print(msg)
        return time_ns

    def convertToInfluxdbProtocol(self, dMeas):
        lKey = list()
        lFields = list()
        if "measurement" in dMeas and "fields" in dMeas:
            lKey.append(dMeas["measurement"])
            if "tags" in dMeas:
                for key, value in dMeas["tags"].iteritems():
                    lKey.append(key+"="+value)
            if len(dMeas["fields"]) > 0:
                for k, v in dMeas["fields"].iteritems():
                    lFields.append(k+"="+self.castValue(v))
            else:
                raise Exception("No data with the measurement")
            lp = ",".join(lKey)+" "+",".join(lFields)
            if "time" in dMeas:
                sTimestamp = self.convertTimestamp(dMeas["time"])
                lp = lp + " " + sTimestamp
            return lp
        else:
            msg = "Impossible to decode the message: {}".format(dMeas)
            if logger is not None:
                logger.warn(msg)
            else:
                print(msg)
        return ""

    def show(self, lMeas):
        lp = list(map(self.convertToInfluxdbProtocol, lMeas))
        for sData in lp:
            print sData
