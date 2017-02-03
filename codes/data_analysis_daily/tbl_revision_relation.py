#!/usr/bin/python
# -*- coding: utf-8 -*-
import mysql.connector
import ConfigParser
import logging
from mysql.connector import errorcode
import datetime
import time

# diy library
import sys
sys.path.append('/home/oper/wikiglass-data-service/wikiglass-service-platform/codes/models')
from RevisionRelationModel import RevisionRelationModel

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
# log file path
LOGFILE = CONFIG.get("logs_conf", "common_log")


try:
    logging.basicConfig(filename=LOGFILE, level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')

    # Connect to pbworks_db database
    cnx = mysql.connector.connect(user=PB_DB_USERNAME, password=PB_DB_PWD, host=PB_DB_HOST, database=PB_DB_NAME,
                                  charset='utf8mb4')
    cur = cnx.cursor(buffered=True)
    cur.execute("use " + PB_DB_NAME)

    end_date = datetime.datetime.now()
    end_unix_timestamp = time.mktime(end_date.timetuple())
    end_date_string = end_date.strftime("%y-%m-%d %H:%M:%S")
    start_date = datetime.datetime.now() - datetime.timedelta(days=2)
    start_unix_timestamp = time.mktime(start_date.timetuple())
    start_date_string = start_date.strftime("%y-%m-%d %H:%M:%S")

    cur.execute(""" SELECT rev1.revision_id, rev1.page_id, rev1.user_id, rev1.version, rev2.user_id, rev2.version
                    FROM Revision AS rev1, Revision AS rev2
                    WHERE rev1.page_id = rev2.page_id AND rev1.version + 1 = rev2.version
                    AND rev2.timestamp BETWEEN %s AND %s
                    ORDER BY rev1.page_id""", (start_unix_timestamp, end_unix_timestamp))
    revision_relation_list = cur.fetchall()

    logging.debug("[tbl_revision_relation] " + start_date_string + " ~ " + end_date_string + ": " + str(len(revision_relation_list)))

    for revision_relation in revision_relation_list:
        revision_relation_model = RevisionRelationModel()
        revision_relation_model.revision_id = revision_relation[0]
        revision_relation_model.page_id = revision_relation[1]
        revision_relation_model.user_from_id = revision_relation[2]
        revision_relation_model.user_to_id = revision_relation[4]

        cur.execute("SELECT * FROM Revision_relation WHERE revision_id = " + str(revision_relation_model.revision_id))
        check_exist = cur.fetchall()

        # this revision record already exist
        if len(check_exist) > 0:
            continue

        cur.execute(""" SELECT User_name, Group_id
                        FROM User_stats_by_group
                        WHERE User_perm = 'write' and User_id = '""" + revision_relation_model.user_from_id + "' LIMIT 1")
        user_from_info = cur.fetchall()

        cur.execute(""" SELECT User_name, Group_id
                        FROM User_stats_by_group
                        WHERE User_perm = 'write' and User_id = '""" + revision_relation_model.user_to_id + "' LIMIT 1")
        user_to_info = cur.fetchall()

        if len(user_from_info) == 0 or len(user_to_info) == 0:
            continue

        revision_relation_model.user_from_name = user_from_info[0][0]
        revision_relation_model.group_id = user_from_info[0][1]
        revision_relation_model.user_to_name = user_to_info[0][0]

        # prepare data with accurate timestamp
        cur.execute(""" INSERT INTO Revision_relation (revision_id, user_from_id, user_from_name, user_to_id,
                        user_to_name, page_id, group_id, time_stamp)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                    (revision_relation_model.revision_id, revision_relation_model.user_from_id,
                     revision_relation_model.user_from_name, revision_relation_model.user_to_id,
                     revision_relation_model.user_to_name, revision_relation_model.page_id,
                     revision_relation_model.group_id, end_date_string))
        cnx.commit()

    # Close	mysql database connection
    cur.close()

except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Error: Wrong name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database doesn't exist")
    else:
        print(err)
else:
    cnx.close()
