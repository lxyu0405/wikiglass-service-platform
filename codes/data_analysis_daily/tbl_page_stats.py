#!/usr/bin/python
import mysql.connector
import ConfigParser
import logging
from mysql.connector import errorcode

CONFIG = ConfigParser.ConfigParser()
# config file path
CONFIG.read("/home/oper/wikiglass-data-service/wikiglass-service-platform/codes/settings/global.conf")
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

    # Get a full page list
    cur.execute("""SELECT Page.page_id, Page.wiki_id, SUBSTRING_INDEX(Page.page_id, '_', -1)
                    FROM Page, Wiki
                    WHERE Page.wiki_id = Wiki.wiki_id AND Wiki.year = """ + YEAR)
    page_list = cur.fetchall()

    # Loop through all pages in page list
    for page in page_list:
        page_id = page[0]
        wiki_id = page[1]
        page_index = page[2]

        # Get revision count of that page
        cur.execute(""" SELECT count(*)
                        FROM Revision, User
                        WHERE page_id = '""" + page_id + """'
                        AND User.user_id = Revision.user_id
                        AND User.perm = 'write'""")
        revision_count = cur.fetchone()
        total_revisions = int(revision_count[0])
        total_words = 0

        logging.debug("[tbl_page] page_id: " + page_id + ", wiki_id: " + wiki_id + ", page_index: " + str(
            page_index) + ", total_revisions: " + str(total_revisions))

        # Insert to Page_Stats
        cur.execute(""" INSERT INTO Page_Stats (Page_id, Wiki_id, Page_index, No_of_revisions)
                        VALUES (%s, %s, %s, %s) ON duplicate key UPDATE
                        No_of_revisions = if( No_of_revisions <> values(No_of_revisions), values(No_of_revisions), No_of_revisions ) """,
                    (page_id, wiki_id, page_index, total_revisions))
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
