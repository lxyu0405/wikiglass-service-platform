#!/usr/bin/python
# -*- coding: utf-8 -*-
# system library
import mysql.connector
import logging
import ConfigParser
from mysql.connector import errorcode

# diy library
from codes.common.utils import ToolBox
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

# Prepare data for 5 key tables
WIKI_MODEL_LIST = []
PAGE_MODEL_LIST = []
USER_MODEL_LIST = []
REVISION_MODEL_LIST = []

try:
    logging.basicConfig(filename=LOGFILE, level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')

    # Connect to pbworks_db database
    cnx = mysql.connector.connect(user=PB_DB_USERNAME, password=PB_DB_PWD, host=PB_DB_HOST, database=PB_DB_NAME, charset='utf8mb4')
    cur = cnx.cursor(buffered=True)
    cur.execute("use " + PB_DB_NAME)

    cur.execute("SELECT wiki_id, admin_key, wiki_url FROM Wiki WHERE year = '" + YEAR + "' AND school = 'twgss'")
    wikis = cur.fetchall()

    for row in wikis:
        wiki_model = WikiModel()
        wiki_model.wiki_id = row[0]
        wiki_model.admin_key = row[1]
        wiki_model.wiki_url = row[2]
        # Add the model to the list
        WIKI_MODEL_LIST.append(wiki_model)

    for wiki_model in WIKI_MODEL_LIST:
        print(wiki_model.wiki_url + "/api_v2/op/GetObjectsNOM/admin_key/" + wiki_model.admin_key + "/object_types/page")
        # prepare data for page list
        ToolBox.get_page_list(wiki_model.wiki_id, wiki_model.wiki_url, wiki_model.admin_key, PAGE_MODEL_LIST)
        # prepare data for user list
        ToolBox.get_user_list(wiki_model.wiki_url, wiki_model.admin_key, USER_MODEL_LIST)

        # Load json file from GetPageRevisions, Prepare data for Revision table
        for page_model in PAGE_MODEL_LIST:
            ToolBox.get_revision_list(wiki_model.wiki_url, wiki_model.admin_key,
                                      page_model.page_id, page_model.page_name, REVISION_MODEL_LIST)

        # Insert data into table Page
        for page_model in PAGE_MODEL_LIST:
            cur.execute("""INSERT INTO Page (page_id,wiki_id,page_name,page_url)
                            VALUES (%s,%s,%s,%s) ON DUPLICATE KEY update page_name =
                            if( page_name <> values(page_name), values(page_name), page_name ) """,
                        (page_model.page_id, wiki_model.wiki_id, page_model.page_name, page_model.page_url))
            cnx.commit()

        for user_model in USER_MODEL_LIST:
            # Insert data into table User
            cur.execute("""INSERT INTO User (user_id, full_name, username, perm)
                            VALUES (%s,%s,%s,%s) ON DUPLICATE KEY update
                            full_name = if( full_name <> values(full_name), values(full_name), full_name ),
                            username = if( username <> values(username), values(username), username ),
                            perm = if( perm <> values(perm), values(perm), perm ) """,
                        (user_model.user_id, user_model.full_name, user_model.username, user_model.perm))
            cnx.commit()
            # Insert data into table User_wiki
            if user_model.perm == "write" :
                cur.execute("""INSERT INTO User_wiki (user_id,uid,wiki_id)
                                VALUES (%s,%s,%s) ON DUPLICATE KEY UPDATE
                                user_id = if( user_id <> values(user_id), values(user_id), user_id )""",
                            (wiki_model.wiki_id + "_" + user_model.user_id, user_model.user_id, wiki_model.wiki_id))
            cnx.commit()

        # Insert data into table Revision
        for revision_model in REVISION_MODEL_LIST:
            print(revision_model.page_id + " " + str(revision_model.timestamp) + " " + str(revision_model.version)
                          + " " + str(revision_model.oid) + " " + revision_model.user_id)
            if revision_model.user_id != 'Error':
                cur.execute("""INSERT INTO Revision (page_id, timestamp, version, oid, user_id, content)
                select * from (select %s, %s, %s, %s, %s, %s) as tmp
                where not exists (select * from Revision where page_id = %s and version = %s ) LIMIT 1 """,
                            (revision_model.page_id, revision_model.timestamp, revision_model.version + 1,
                             revision_model.oid, revision_model.user_id, revision_model.content,
                             revision_model.page_id, revision_model.version + 1))
            cnx.commit()
        # clear list to get it ready for next wiki
        PAGE_MODEL_LIST = []
        USER_MODEL_LIST = []
        REVISION_MODEL_LIST = []

# Exception
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    else:
        print(err)
else:
    cnx.close()
