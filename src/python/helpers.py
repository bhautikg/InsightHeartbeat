import json

def parse_config(configfile):
    """
    reads configs saved as json record in configuration file and returns them
    :type configfile: str       path to config file
    :rtype          : dict      configs
    """
    conf = json.load(open(configfile, "r"))
    return conf