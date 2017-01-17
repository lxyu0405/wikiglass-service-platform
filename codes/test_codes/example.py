# -*- coding: utf-8 -*-
import ConfigParser

from codes.common.char_utils import CharTools

CONFIG = ConfigParser.ConfigParser()
# config file path
CONFIG.read("../settings/global.conf")
# year version
YEAR = CONFIG.get("system_version", "year")
# pbworks_db config
PB_DB_USERNAME = CONFIG.get("pbworks_db_conf", "username")
PB_DB_PWD = CONFIG.get("pbworks_db_conf", "password")
PB_DB_HOST = CONFIG.get("pbworks_db_conf", "db_host")
PB_DB_NAME = CONFIG.get("pbworks_db_conf", "db_name")
# bluespice_db config
BS_DB_USERNAME = CONFIG.get("bluespice_db_conf", "username")
BS_DB_PWD = CONFIG.get("bluespice_db_conf", "password")
BS_DB_HOST = CONFIG.get("bluespice_db_conf", "db_host")
BS_DB_NAME = CONFIG.get("bluespice_db_conf", "db_name")
# log file path
LOGFILE = CONFIG.get("logs_conf", "common_log")

def test_list_param(this_list):
    if this_list is None:
        this_list = []
    else:
        this_list.append(1)

if __name__ == "__main__":
    test_string = u'让我来写一个hello world'
    print(test_string)
    print(CharTools.clean_alph(test_string))
    '''
    list = []
    print(list) #[0]
    test_list_param(list)
    print(list) # [0,1]'''

    '''
    print("year: " + YEAR)
    print("db_username: " + PB_DB_USERNAME)
    print("db_password: " + PB_DB_PWD)
    print("db_host: " + PB_DB_HOST)
    print("db_name: " + PB_DB_NAME)

    print("log file path: " + LOGFILE)
    '''
