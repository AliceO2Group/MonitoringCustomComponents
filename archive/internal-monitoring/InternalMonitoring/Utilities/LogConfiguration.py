import logging


def getLogger(conf, debug=False):
    """
    Manage the logger configuration
    """

    level = "default"
    loglevel_allowed = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    if "logfile" in conf:
        abs_logfilename = conf["logfile"]
    else:
        raise Exception("logfile miss in the configuration")
    if "loglevel" in conf:
        if conf["loglevel"] in loglevel_allowed:
            level = conf["loglevel"]

    logger = logging.getLogger('')

    if debug:
        h = logging.StreamHandler()
    else:
        h = logging.FileHandler(abs_logfilename)

    # create formatter and add it to the handlers
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    h.setFormatter(formatter)
    # add the handlers to logger
    logger.addHandler(h)

    if level == 'DEBUG':
        logger.setLevel(logging.DEBUG)
    elif level == 'INFO':
        logger.setLevel(logging.INFO)
    elif level == 'WARNING':
        logger.setLevel(logging.WARNING)
    elif level == "ERROR":
        logger.setLevel(logging.ERROR)
    elif level == 'CRITICAL':
        logger.setLevel(logging.CRITICAL)
    else:
        logger.setLevel(logging.INFO)

    return logger
