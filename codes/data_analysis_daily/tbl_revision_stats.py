#!/usr/bin/python
# -*- coding: utf-8 -*-
import mysql.connector
import ConfigParser
import logging
from mysql.connector import errorcode

# diy library
import sys
sys.path.append('/home/oper/wikiglass-data-service/wikiglass-service-platform/codes/common')
sys.path.append('/home/oper/wikiglass-data-service/wikiglass-service-platform/codes/models')
from RevisionModel import RevisionModel
from UserModel import UserModel
from char_utils import CharTools


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

REVISION_MODEL_LIST = []

try:
    logging.basicConfig(filename=LOGFILE, level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')

    # Connect to pbworks_db database
    cnx = mysql.connector.connect(user=PB_DB_USERNAME, password=PB_DB_PWD, host=PB_DB_HOST, database=PB_DB_NAME,
                                  charset='utf8mb4')
    cur = cnx.cursor(buffered=True)
    cur.execute("use " + PB_DB_NAME)

    # Get a full page_id list
    cur.execute(""" SELECT Page.page_id, Page.wiki_id
                    FROM Page, Wiki
                    WHERE Page.wiki_id = Wiki.wiki_id AND Wiki.year = """ + YEAR)
    page_list = cur.fetchall()

    # Loop through all pages in page list
    for page_info in page_list:
        page_id = page_info[0]
        page_wiki_id = page_info[1]
        # Get a full revision list in that page
        cur.execute(""" select revision_id, page_id, version, user_id, timestamp
                        from Revision where page_id = '""" + page_id + """' order by version ASC """)
        revision_list = cur.fetchall()

        # Loop through all revisions in revision list
        for row in revision_list:
            revision_model = RevisionModel()
            revision_model.revision_id = row[0]
            revision_model.page_id = row[1]
            revision_model.version = row[2]
            revision_model.user_id = row[3]
            revision_model.timestamp = row[4]
            REVISION_MODEL_LIST.append(revision_model)

        # HashMap to store content of adjacent versions
        hashmap_previous_content = {}
        hashmap_this_content = {}
        for revision_model in REVISION_MODEL_LIST:
            # Get sentence quality info of certain revision
            cur.execute(""" SELECT sentence_id, level
                            FROM Sentence_quality
                            WHERE initial_revision_id = %s OR current_revision_id = %s""",
                        (revision_model.revision_id, revision_model.revision_id))
            sentence_lvl_list = cur.fetchall()

            low_lvl_count, high_lvl_count = 0, 0

            for sentence_lvl in sentence_lvl_list:
                sentence_lvl_level = sentence_lvl[1]
                if sentence_lvl_level == "level 1":
                    low_lvl_count += 1
                if sentence_lvl_level == "level 3":
                    high_lvl_count += 1

            # Get user information who made that revision
            cur.execute(""" select ifnull(full_name,''), ifnull(username,''), ifnull(perm,'')
                            from User
                            where user_id = '""" + revision_model.user_id + "'")
            user = cur.fetchone()

            if user is None:
                continue

            user_model = UserModel()
            user_model.full_name = user[0]
            user_model.username = user[1]
            user_model.perm = user[2]

            # Insert partial data to Revision_Stats
            cur.execute(""" insert into Revision_Stats (Revision_id, Page_id, Revision_index, User_id, User_name,
                            User_no, User_perm, High_level_thinking, Low_level_thinking, Revision_creation_time)
                            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) on duplicate key update
                            Revision_id = Revision_id """,
                        (revision_model.revision_id, revision_model.page_id, revision_model.version,
                         revision_model.user_id, user_model.full_name, user_model.username, user_model.perm,
                         high_lvl_count, low_lvl_count, revision_model.timestamp))
            cnx.commit()

            # Get content data in that revision
            cur.execute("select content from Revision where revision_id = " + str(revision_model.revision_id))
            content_data = cur.fetchone()

            revision_model.content = content_data[0].decode('unicode_escape')

            # empty content
            if revision_model.content is None:
                continue

            # remove html tags, nbsp, \n from raw content
            # replace is used to fix CONTENT
            revision_model.content = CharTools.clean_nbsp(CharTools.clean_meaningless_symbol(
                CharTools.clean_spaces(CharTools.clean_html_tags(revision_model.content))))
            revision_model.no_of_words = len(revision_model.content)

            word_addition, word_deletion, word_changes = 0, 0, 0
            # construct HashMaps for real_content
            for character in revision_model.content:
                if character in hashmap_this_content:
                    hashmap_this_content[character] += 1
                else:
                    hashmap_this_content[character] = 1

            # two hashmaps subtraction, save result in hashmap_previous_content
            # because hashmap_this_content should be saved for next hashmap_previous_content
            # subtract hashmap_this_content from hashmap_previous_content
            for this_key in hashmap_this_content.keys():
                if this_key in hashmap_previous_content.keys():
                    hashmap_previous_content[this_key] -= hashmap_this_content[this_key]
                else:
                    hashmap_previous_content[this_key] = -hashmap_this_content[this_key]

            # word_addition, for (k,v) in hashmap_previous_content, whose v is negative
            # word_deletion, for (k,v) in hashmap_previous_content, whose v is positive
            for res_key in hashmap_previous_content.keys():
                if hashmap_previous_content[res_key] < 0:
                    word_addition -= hashmap_previous_content[res_key]
                else:
                    word_deletion += hashmap_previous_content[res_key]

            word_changes = word_deletion + word_addition

            # After getting the number of word deletion and addition, the current content will
            # replace the previous content to allow the comparison of the next revision.
            hashmap_previous_content = hashmap_this_content
            hashmap_this_content = {}

            logging.debug("[tbl_revision] page_id: " + page_id
                          + ", revision_index: " + str(revision_model.version)
                          + ", total_words: " + str(revision_model.no_of_words)
                          + ", word_addition: " + str(word_addition)
                          + ", word_deletion: " + str(word_deletion)
                          + ", word_changes: " + str(word_changes))

            cur.execute(""" UPDATE Revision_Stats
                            SET Words_addition = %s, Words_deletion = %s, Words_change = %s, Total_words = %s
                            WHERE Revision_id = %s""", (word_addition, word_deletion, word_changes,
                        revision_model.no_of_words, revision_model.revision_id))
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
