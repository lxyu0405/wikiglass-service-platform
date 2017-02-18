#!/usr/bin/python
import mysql.connector
import logging
import ConfigParser
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

    # Get a full wiki list
    cur.execute("SELECT wiki_id FROM Wiki WHERE year = " + YEAR + " ORDER BY class_name ASC, group_no ASC")
    wiki_list = cur.fetchall()

    # Loop through all wikis in wiki list
    for wiki in wiki_list:
        # Get performance of every student
        cur.execute(""" SELECT User_id, User_name, User_no, User_perm, COUNT(*), SUM(No_of_involved_revision),
                        SUM(Total_words_addition), SUM(Total_words_deletion), SUM(Total_words_change)
                        FROM User_stats_by_page
                        WHERE page_id LIKE '{0}\_%'
                        GROUP BY User_id
                        ORDER BY User_no""".format(wiki[0]))
        student = cur.fetchall()

        # Loop through all students
        for row in student:
            user_id = '' if row[0] is None else row[0]
            user_name = '' if row[1] is None else row[1]
            user_no = '' if row[2] is None else row[2]
            user_perm = '' if row[3] is None else row[3]
            count = 0 if row[4] is None else row[4]
            total_involved_revision = 0 if row[5] is None else row[5]
            total_words_addition = 0 if row[6] is None else row[6]
            total_words_deletion = 0 if row[7] is None else row[7]
            total_words_changes = 0 if row[8] is None else row[8]

            cur.execute("""SELECT sentence_id, level
                            FROM Sentence_quality
                            WHERE user_id = '""" + user_id + "' AND page_id LIKE '{0}\_%'".format(wiki[0]))
            sentence_lvl_list = cur.fetchall()

            low_lvl_count, high_lvl_count = 0, 0
            for sentence_lvl in sentence_lvl_list:
                sentence_lvl_level = sentence_lvl[1]
                if sentence_lvl_level == "level 1":
                    low_lvl_count += 1
                if sentence_lvl_level == "level 3":
                    high_lvl_count += 1

            # Get the count of pages without any revisions by that student
            cur.execute(""" SELECT count(*) AS user_count
                            FROM User_stats_by_page
                            WHERE page_id LIKE '" + wiki[0] + "\_%'
                            AND No_of_involved_revision = 0 AND User_id = '""" + user_id + "'")
            absence_data = cur.fetchone()
            absence_no = int(absence_data[0])
            count -= absence_no

            logging.debug("[tbl_user_group] user_name: " + user_name
                          + ", wiki_id: " + wiki[0]
                          + ", total_page: " + str(count)
                          + ", total_revision: " + str(total_involved_revision)
                          + ", addition: " + str(total_words_addition)
                          + ", deletion: " + str(total_words_deletion)
                          + ", changes: " + str(total_words_changes)
                          + ", low_lvl_count: " + str(low_lvl_count)
                          + ", high_lvl_count: " + str(high_lvl_count))

            # Insert to User_stats_by_group
            cur.execute(""" INSERT INTO User_stats_by_group (User_id, User_name, User_no, User_perm, Group_id,
                            Total_involved_pages, Total_involved_revisions, Total_words_addition, Total_words_deletion,
                            Total_words_change, High_level_thinking, Low_level_thinking)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON duplicate key UPDATE
                            User_name = if( User_name <> values(User_name), values(User_name), User_name ),
                            Total_involved_pages = if( Total_involved_pages <> values(Total_involved_pages),
                            values(Total_involved_pages), Total_involved_pages ),
                            Total_involved_revisions = if( Total_involved_revisions <> values(Total_involved_revisions),
                            values(Total_involved_revisions), Total_involved_revisions ),
                            Total_words_addition = if( Total_words_addition <> values(Total_words_addition),
                            values(Total_words_addition), Total_words_addition ),
                            Total_words_deletion = if( Total_words_deletion <> values(Total_words_deletion),
                            values(Total_words_deletion), Total_words_deletion ),
                            Total_words_change = if( Total_words_change <> values(Total_words_change),
                            values(Total_words_change), Total_words_change ),
                            High_level_thinking = if( High_level_thinking <> values(High_level_thinking),
                            values(High_level_thinking), High_level_thinking ),
                            Low_level_thinking = if( Low_level_thinking <> values(Low_level_thinking),
                            values(Low_level_thinking), Low_level_thinking ) """,
                            (user_id, user_name, user_no, user_perm, wiki[0], count, total_involved_revision,
                             total_words_addition, total_words_deletion, total_words_changes, high_lvl_count,
                             low_lvl_count))
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
