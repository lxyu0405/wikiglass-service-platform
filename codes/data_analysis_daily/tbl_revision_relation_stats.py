#!/usr/bin/python
import mysql.connector
import ConfigParser
import logging
from mysql.connector import errorcode

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

    cur.execute("DELETE FROM Revision_relation_stats WHERE group_id LIKE '" + YEAR + "%'")
    logging.debug("[tbl_revision_relation_stats] ")

    cur.execute(" SELECT DISTINCT(group_id) FROM Revision_relation WHERE group_id LIKE '" + YEAR + "%'")
    group_id_list = cur.fetchall()

    for group_id in group_id_list:
        cur.execute(""" SELECT DISTINCT(User_id), User_name
                        FROM User_stats_by_group
                        WHERE User_perm = 'write' AND Group_id = '""" + group_id[0] + "'")
        group_member_list = cur.fetchall()

        group_stats_dict = {}
        # create dict for statistics
        for i in range(len(group_member_list)):
            for j in range(len(group_member_list)):
                new_key = group_member_list[i][0] + "&^&" + group_member_list[i][1] + "->" + \
                          group_member_list[j][0] + "&^&" + group_member_list[j][1]
                group_stats_dict[new_key] = 0

        cur.execute(""" SELECT user_from_id, user_from_name, user_to_id, user_to_name
                        FROM Revision_relation
                        WHERE group_id = '""" + group_id[0] + "'")
        group_revision_list = cur.fetchall()

        for group_revision in group_revision_list:
            thisKey = group_revision[0] + "&^&" + group_revision[1] + "->" + group_revision[2] + "&^&" + group_revision[3]
            if thisKey not in group_stats_dict.keys():
                logging.debug("[tbl_revision_relation_stats][Error] Can't find the key in Dic: " + thisKey)
                continue
            else:
                group_stats_dict[thisKey] += 1
        # analyze the result in group_stats_dict
        for key in group_stats_dict.keys():
            userFrom, userTo = key.split("->")
            userFromId, userFromName = userFrom.split("&^&")
            userToId, userToName = userTo.split("&^&")
            print(userFromId + " -> " + userToId + " : " + str(group_stats_dict[key]))
            cur.execute(""" INSERT INTO Revision_relation_stats (user_from_id, user_from_name, user_to_id,
                            user_to_name, group_id, total_revision_count)
                            VALUES (%s, %s, %s, %s, %s, %s)""",
                        (userFromId, userFromName, userToId, userToName, group_id[0], group_stats_dict[key]))
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
