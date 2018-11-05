	
try:
    import logging
    logger = logging.getLogger('Utilities')
except Exception as e:
    logger = None


def print_json(json_data):
    """
    Print JSON in a human readible way
    """

    try:
        import json
    except:
        import simplejson as json
    try:
        print json.dumps(json_data, indent=2)
    except Exception as e:
        msg = "Impossible print JSON. Exception: {}".format(e)
        if logger is not None:
            logger.error(msg)
        else:
            print(msg)


def readConf(yaml_file):
    """
    Read a yaml configuration file
    """

    import yaml
    try:
        with open(yaml_file, 'r') as stream:
            conf = yaml.safe_load(stream)
            return conf
    except Exception as e:
        msg = "Impossible import YAML file. Exception: {}".format(e)
        if logger is not None:
            logger.error(msg)
        else:
            print(msg)
        return None
