#!/usr/bin/python
import mysql.connector
import ConfigParser
import logging
from mysql.connector import errorcode

# diy library
from codes.models.WikiModel import WikiModel


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

GROUP_MODEL_LIST = []

try:
    logging.basicConfig(filename=LOGFILE, level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')

    # Connect to pbworks_db database
    cnx = mysql.connector.connect(user=PB_DB_USERNAME, password=PB_DB_PWD, host=PB_DB_HOST, database=PB_DB_NAME,
                                  charset='utf8mb4')
    cur = cnx.cursor(buffered=True)
    cur.execute("use " + PB_DB_NAME)

    # Get a full wiki list
    cur.execute("SELECT wiki_id, class_name, group_no FROM Wiki WHERE year = " + YEAR)
    wiki_list = cur.fetchall()

    # Save all group info in GROUP_MODEL_LIST
    for group in wiki_list:
        group_model = WikiModel()
        group_model.wiki_id = group[0]
        group_model.class_name = group[1]
        group_model.group_no = group[2]
        GROUP_MODEL_LIST.append(group_model)

    for group_model in GROUP_MODEL_LIST:
        # Get the page count of that wiki
        cur.execute("SELECT count(*) FROM Page WHERE wiki_id = '" + group_model.wiki_id + "'")
        page_count = cur.fetchone()
        total_pages = int(page_count[0])
        total_words = 0

        # Get sentenece quality info of the group
        cur.execute("SELECT sentence_id, level FROM Sentence_quality WHERE page_id LIKE '{0}\_%' ".format(group_model.wiki_id))
        sentence_lvl_list = cur.fetchall()

        low_lvl_count, high_lvl_count = 0, 0

        for sentence_lvl in sentence_lvl_list:
            sentence_lvl_level = sentence_lvl[1]
            if sentence_lvl_level == "level 1":
                low_lvl_count += 1
            if sentence_lvl_level == "level 3":
                high_lvl_count += 1

        logging.debug("[tbl_group] wiki_id: " + group_model.wiki_id + ", class_id: " + group_model.class_name +
                      ", group_index: " + str(group_model.group_no) + ", total_pages: " + str(total_pages) +
                      ", low_level_count: " + str(low_lvl_count) + ", high_level_count: " + str(high_lvl_count))

        # Insert to Group_Stats
        cur.execute("""INSERT INTO Group_Stats
                        (Wiki_id, Class_id, Group_index, No_of_pages, No_of_words, High_level_thinking, Low_level_thinking)
                        VALUES (%s, %s, %s, %s, %s, %s, %s) ON duplicate key UPDATE
                        No_of_pages = if( No_of_pages <> values(No_of_pages), values(No_of_pages), No_of_pages )
                        High_level_thinking = if ( High_level_thinking <> values(High_level_thinking), values(High_level_thinking), High_level_thinking )
                        Low_level_thinking = if ( Low_level_thinking <> values(Low_level_thinking), values(Low_level_thinking), Low_level_thinking )""",
                    (group_model.wiki_id, group_model.class_name, group_model.group_no, total_pages, total_words, high_lvl_count, low_lvl_count))
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
