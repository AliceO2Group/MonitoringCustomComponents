#!/usr/bin/env python

from time import time, sleep
from sys import exit

try:
    import logging
    logger = logging.getLogger('Sched')
except Exception as e:
    logger = None


class Sched:
    """
    Class used to synchronize the execution of a function
    Usage: Define the period (in seconds )
    """

    def __init__(self, schedConf):
        try:
            floatPeriod = float(schedConf["period"])
            if floatPeriod <= 0:
                raise Exception("Negative period in Sched")
            self.active = True
            self._period = floatPeriod
            self._nextTick = 0
        except Exception as e:
            msg = "Impossible parse 'period'. Sched disabled. \
            Exception: {}".format(e)
            if logger is not None:
                logger.critical(msg)
            else:
                print(msg)
            self.active = False

        logger.debug("INIT sched.active: {}".format(self.active))
        logger.debug("INIT sched.period: {}".format(self._period))
        logger.debug("iNIT sched.nexttick: {}".format(self._nextTick))

    def getPeriod(self):
        return self._period

    def _sleep(self):
        if time() >= self._nextTick:
            msg = "Missed the sampled period"
            if logger is not None:
                logger.warning(msg)
            else:
                print(msg)
        else:
            delay = self._nextTick - time()
            logger.debug("delay: {}".format(delay))
            sleep(delay)

    def _incrementNextTick(self):
        self._nextTick = int(round(self._nextTick + self._period))

    def _setNextTick(self, nextTick):
        self._nextTick = int(round(nextTick))

    def wait(self):
        curTime = time()
        nNextTick = int(round(curTime - curTime % self._period + self._period))
        if nNextTick == self._nextTick:
            pass
        elif nNextTick > self._nextTick:
            logger.warning("next_tick and forseen next_tick does not match")
            logger.warning("nNextTick: {0} \
            nextTick: {1}".format(nNextTick, self._nextTick))
            self._setNextTick(nNextTick)
        else:
            # nNextTick < self.nextTick it should never happens since
            # nNextTick is the closest tick.
            # This situation happens when self.nextTick is bad evaluated
            msg = "nNextTime < nextTick: \
            {0} vs {1}".format(nNextTick, self._nextTick)
            if logger is not None:
                logger.debug(msg)
            else:
                print(msg)
            self._setNextTick(nNextTick)

        self._sleep()
        self._incrementNextTick()
