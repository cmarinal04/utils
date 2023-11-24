from configparser import ConfigParser
import os

def config(section):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(f'{os.getcwd()}/connections.ini')
    # get section
    config_dict = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config_dict[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the file'.format(section))
    
    return config_dict