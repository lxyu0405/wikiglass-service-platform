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
        target_wiki = wiki[0]
        # Get a user list
        cur.execute(""" SELECT u.user_id, u.full_name, u.username, u.perm
                        FROM User_wiki AS uw
                        LEFT OUTER JOIN User AS u
                        ON u.user_id = uw.uid
                        WHERE uw.wiki_id = %s AND u.perm = 'write' AND u.username IS NOT NULL
                        ORDER BY u.perm""", (target_wiki,))
        user_list = cur.fetchall()

        # Get a page list
        cur.execute("SELECT Page_id FROM Page_Stats WHERE Wiki_id = '" + target_wiki + "' ORDER BY Page_index ASC")
        page_list = cur.fetchall()

        # Loop through all pages in a wiki
        for page in page_list:
            # Get revision information of that page
            cur.execute(""" SELECT User_id, User_name, User_no, User_perm, Page_id, COUNT(*),
                                SUM(Words_addition), SUM(Words_deletion), SUM(Words_change)
                            FROM Revision_Stats
                            WHERE page_id = %s
                            GROUP BY User_id
                            ORDER BY User_no""", (page[0],))
            student = cur.fetchall()

            user_of_page = []
            # Loop through all students who made revisions
            for row in student:
                user_id = '' if row[0] is None else row[0]
                user_name = '' if row[1] is None else row[1]
                user_no = '' if row[2] is None else row[2]
                user_perm = '' if row[3] is None else row[3]
                page_id = '' if row[4] is None else row[4]
                count = 0 if row[5] is None else row[5]
                total_words_addition = 0 if row[6] is None else row[6]
                total_words_deletion = 0 if row[7] is None else row[7]
                total_words_changes = 0 if row[8] is None else row[8]

                user_of_page.append(user_id)

                # user page sentence stats
                cur.execute(""" SELECT sentence_id, level
                                FROM Sentence_quality
                                WHERE page_id = %s AND user_id = %s""", (page[0], user_id))
                sentence_lvl_list = cur.fetchall()

                low_lvl_count, high_lvl_count = 0, 0

                for sentence_lvl in sentence_lvl_list:
                    sentence_lvl_level = sentence_lvl[1]
                    if sentence_lvl_level == "level 1":
                        low_lvl_count += 1
                    if sentence_lvl_level == "level 3":
                        high_lvl_count += 1

                logging.debug("[tbl_user_page] user_name: " + user_name
                              + ", user_perm: " + user_perm
                              + ", page_id: " + page_id
                              + ", addition: " + str(total_words_addition)
                              + ", deletion: " + str(total_words_deletion)
                              + ", changes: " + str(total_words_changes)
                              + ", low_lvl_count: " + str(low_lvl_count)
                              + ", high_lvl_count: " + str(high_lvl_count))

                cur.execute(""" INSERT INTO User_stats_by_page (User_id, User_name, User_no, User_perm, Page_id,
                                No_of_involved_revision, Total_words_addition, Total_words_deletion,
                                Total_words_change, High_level_thinking, Low_level_thinking)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON duplicate key UPDATE
                                User_name = if( User_name <> values(User_name), values(User_name), User_name ),
                                No_of_involved_revision = if( No_of_involved_revision <> values(No_of_involved_revision),
                                values(No_of_involved_revision), No_of_involved_revision ),
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
                                (user_id, user_name, user_no, user_perm, page_id, count, total_words_addition,
                                 total_words_deletion, total_words_changes, high_lvl_count, low_lvl_count))
                cnx.commit()

            # Loop through all students who did not make revisions
            for user in user_list:
                if user[0] not in user_of_page:
                    user_id = user[0]
                    user_name = user[1]
                    user_no = user[2]
                    user_perm = user[3]
                    page_id = page[0]
                    count = 0
                    total_words_addition = 0
                    total_words_deletion = 0
                    total_words_changes = 0

                    cur.execute(""" INSERT INTO User_stats_by_page (User_id, User_name, User_no, User_perm, Page_id,
                                    No_of_involved_revision, Total_words_addition, Total_words_deletion,
                                    Total_words_change, High_level_thinking, Low_level_thinking)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 0, 0) ON duplicate key UPDATE
                                    User_name = if( User_name <> values(User_name), values(User_name), User_name ),
                                    No_of_involved_revision = if( No_of_involved_revision <> values(No_of_involved_revision),
                                    values(No_of_involved_revision), No_of_involved_revision ),
                                    Total_words_addition = if( Total_words_addition <> values(Total_words_addition),
                                    values(Total_words_addition), Total_words_addition ),
                                    Total_words_deletion = if( Total_words_deletion <> values(Total_words_deletion),
                                    values(Total_words_deletion), Total_words_deletion ),
                                    Total_words_change = if( Total_words_change <> values(Total_words_change),
                                    values(Total_words_change), Total_words_change ) """,
                                    (user_id, user_name, user_no, user_perm, page_id, count, total_words_addition,
                                     total_words_deletion, total_words_changes))
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
