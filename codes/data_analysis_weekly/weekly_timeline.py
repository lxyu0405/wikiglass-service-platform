#!/usr/bin/python
import mysql.connector
import datetime
import ConfigParser
from mysql.connector import errorcode
import logging
import time

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

REVISION_MODEL_LIST = []

try:
    logging.basicConfig(filename=LOGFILE, level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')

    # Connect to pbworks_db database
    cnx = mysql.connector.connect(user=PB_DB_USERNAME, password=PB_DB_PWD, host=PB_DB_HOST, database=PB_DB_NAME,
                                  charset='utf8mb4')
    cur = cnx.cursor(buffered=True)
    cur.execute("use " + PB_DB_NAME)

    # Current timestamp (ending time of the execution)
    end_time = time.time()
    # Getting a List of all school
    cur.execute("SELECT school, start_time FROM School WHERE year = %s", (YEAR,))
    school_list = cur.fetchall()

    # Summarizing data for all schools
    for school in school_list:
        school_name = school[0]
        start_time = school[1].strftime("%Y-%m-%d %H:%M:%S")
        week_start = time.mktime(datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S").timetuple())
        week_start_string = datetime.datetime.fromtimestamp(week_start).strftime('%Y-%m-%d %H:%M:%S')
        print school_name

        # Getting a list of all groups
        cur.execute("SELECT wiki_id FROM Wiki WHERE year = %s AND school = %s", (YEAR, school_name,))
        group_list = cur.fetchall()

        # Getting a list of all students
        cur.execute("""SELECT u.full_name, uw.wiki_id
                        FROM User_wiki AS uw LEFT
                        OUTER JOIN User AS u ON u.user_id = uw.uid
                        INNER JOIN Wiki As w ON w.wiki_id = uw.wiki_id
                        WHERE w.year = %s AND w.school = %s AND u.perm = 'write' AND u.username IS NOT NULL
                        ORDER BY uw.wiki_id,u.username""", (YEAR, school_name,))
        student_list = cur.fetchall()

        # Summarizing data in weekly basis
        while week_start < end_time:
            week_start_string = datetime.datetime.fromtimestamp(week_start).strftime('%Y-%m-%d %H:%M:%S')
            week_end = week_start + 604799
            week_end_string = datetime.datetime.fromtimestamp(week_end).strftime('%Y-%m-%d %H:%M:%S')

            # weekly revision count region
            if True:
                # Getting revision count (number of revision counts of each group)
                cur.execute("""SELECT w.wiki_id, COUNT(*)
                                FROM Revision_Stats AS r, Wiki AS w
                                WHERE r.page_id LIKE CONCAT(w.wiki_id,'\_%') AND w.year = %s AND w.school = %s
                                AND r.Revision_creation_time BETWEEN %s AND %s
                                GROUP BY w.wiki_id""", (YEAR, school_name, week_start, week_end,))
                data = cur.fetchall()

                # Inserting data of revision count (If a group does not have any revision counts,
                # set 0 to the revision_count)
                group_id = []
                rev_count = []
                for row in data:
                    wiki_id = row[0]
                    count = row[1]
                    group_id.append(wiki_id)
                    rev_count.append(count)
                    cur.execute(""" INSERT INTO Weekly_revision_count (group_id,revision_count,ts_week_start)
                                    VALUES (%s,%s,%s) ON DUPLICATE KEY UPDATE
                                    revision_count = if( revision_count <> values(revision_count),
                                    values(revision_count), revision_count )""",
                                (wiki_id, count, week_start_string))
                cnx.commit()
                for group in group_list:
                    if group[0] not in group_id:
                        cur.execute(""" INSERT INTO Weekly_revision_count (group_id,revision_count,ts_week_start)
                                        VALUES (%s,%s,%s) ON DUPLICATE KEY UPDATE
                                        revision_count = if( revision_count <> values(revision_count),
                                        values(revision_count), revision_count )""",
                                    (group[0], 0, week_start_string))
                cnx.commit()

            # weekly word count region
            if True:
                # Getting word count (number of word counts of each group)
                cur.execute(""" SELECT w.wiki_id, SUM(r.Words_change)
                                FROM Revision_Stats AS r, Wiki AS w
                                WHERE r.page_id LIKE CONCAT(w.wiki_id,'\_%') AND w.year = %s AND w.school = %s
                                AND r.Revision_creation_time BETWEEN %s AND %s
                                GROUP BY w.wiki_id""", (YEAR, school_name, week_start, week_end,))
                data = cur.fetchall()

                # Inserting data of word count (If a group does not have any word counts, set 0 to the word_count)
                group_id = []
                word_count = []
                for row in data:
                    wiki_id = row[0]
                    count = row[1]
                    group_id.append(wiki_id)
                    word_count.append(count)
                    cur.execute(""" INSERT INTO Weekly_word_count (group_id,word_count,ts_week_start)
                                    VALUES (%s,%s,%s) ON DUPLICATE KEY UPDATE
                                    word_count = if( word_count <> values(word_count), values(word_count), word_count )""",
                                (wiki_id, count, week_start_string))
                cnx.commit()
                for group in group_list:
                    if group[0] not in group_id:
                        cur.execute(""" INSERT INTO Weekly_word_count (group_id,word_count,ts_week_start)
                                        VALUES (%s,%s,%s) ON DUPLICATE KEY UPDATE
                                        word_count = if( word_count <> values(word_count),
                                        values(word_count), word_count )""",
                                    (group[0], 0, week_start_string))
                cnx.commit()

            # weekly word amendment region
            if True:
                # Getting word amendment (number of word changes of each student)
                cur.execute("""SELECT w.wiki_id, r.User_name, SUM(r.Words_change)
                                FROM Revision_Stats AS r, Wiki AS w
                                WHERE r.page_id LIKE CONCAT(w.wiki_id,'\_%') AND w.year = %s AND w.school = %s
                                AND r.Revision_creation_time BETWEEN %s AND %s AND r.User_perm = 'write'
                                GROUP BY r.User_id
                                ORDER BY w.wiki_id, r.User_id""", (YEAR, school_name, week_start, week_end,))
                data = cur.fetchall()

                # Inserting data of word amendment (If a student does not have any word changes,
                # set 0 to the word_amendment_count)
                group_id = []
                student_name = []
                word_amendment_count = []
                for row in data:
                    wiki_id = row[0]
                    student = row[1]
                    count = row[2]
                    group_id.append(wiki_id)
                    student_name.append(student)
                    word_amendment_count.append(count)
                    cur.execute(""" INSERT INTO Weekly_word_amendment (group_id, student_name,
                                    word_amendment_count, ts_week_start)
                                    VALUES (%s,%s,%s,%s) ON DUPLICATE KEY UPDATE
                                    word_amendment_count = if( word_amendment_count <> values(word_amendment_count),
                                    values(word_amendment_count), word_amendment_count )""",
                                (wiki_id, student, count, week_start_string))
                cnx.commit()
                for student in student_list:
                    if student[0] not in student_name:
                        cur.execute(""" INSERT INTO Weekly_word_amendment (group_id,student_name,
                                        word_amendment_count, ts_week_start)
                                        VALUES (%s,%s,%s,%s) ON DUPLICATE KEY UPDATE
                                        word_amendment_count = if( word_amendment_count <> values(word_amendment_count),
                                        values(word_amendment_count), word_amendment_count )""",
                                    (student[1], student[0], 0, week_start_string))
                cnx.commit()

            # weekly sentence level region
            if True:
                cur.execute(" SELECT User_id, User_name, Group_id FROM User_stats_by_group WHERE User_perm = 'write'")
                user_info_list = cur.fetchall()

                low_lvl_dict, high_lvl_dict, user_group_dict, user_name_dict = {}, {}, {}, {}
                for user_info in user_info_list:
                    user_info_user_id = user_info[0]
                    user_info_user_name = user_info[1]
                    user_info_group_id = user_info[2]
                    # save pair(user_id, user_name) information
                    user_name_dict[user_info_user_id] = user_info_user_name
                    # save pair(user_id, group_id) information
                    user_group_dict[user_info_user_id] = user_info_group_id
                    # initialize low_lvl_thinking dictionary
                    low_lvl_dict[user_info_user_id] = 0
                    # initialize high_lvl_thinking dictionary
                    high_lvl_dict[user_info_user_id] = 0

                # get all students information
                cur.execute(""" SELECT user_name, user_id, level FROM Sentence_quality
                                WHERE timestamp BETWEEN %s AND %s""", (week_start, week_end))
                sentence_info_list = cur.fetchall()

                # extract useful information from SQL query result
                for sentence_info in sentence_info_list:
                    sentence_info_user_name = sentence_info[0]
                    sentence_info_user_id = sentence_info[1]
                    sentence_info_level = sentence_info[2]

                    if sentence_info_user_id not in low_lvl_dict.keys():
                        low_lvl_dict[sentence_info_user_id] = 0
                    if sentence_info_user_id not in high_lvl_dict.keys():
                        high_lvl_dict[sentence_info_user_id] = 0
                    # count low thinking level
                    if sentence_info_level == "level 1":
                        low_lvl_dict[sentence_info_user_id] += 1
                    # count high thinking level
                    if sentence_info_level == "level 3":
                        high_lvl_dict[sentence_info_user_id] += 1

                # summarize data and insert into Daily_sentence_level_stats table
                for sum_user_id in user_group_dict.keys():
                    sum_group_id = user_group_dict[sum_user_id]
                    sum_user_name = user_name_dict[sum_user_id]
                    sum_low_thinking_cnt = low_lvl_dict[sum_user_id]
                    sum_high_thinking_cnt = high_lvl_dict[sum_user_id]
                    cur.execute(""" INSERT INTO Weekly_sentence_level_stats (group_id, student_name,
                                    high_thinking_count, low_thinking_count, ts_week_start, ts)
                                    VALUES (%s, %s, %s, %s, %s, %s) ON duplicate key UPDATE
                                    high_thinking_count = if ( high_thinking_count <> values(high_thinking_count),
                                    values(high_thinking_count), high_thinking_count ),
                                    low_thinking_count = if ( low_thinking_count <> values(low_thinking_count),
                                    values(low_thinking_count), low_thinking_count )""",
                                (sum_group_id, sum_user_name, sum_high_thinking_cnt, sum_low_thinking_cnt,
                                 week_start_string, week_end_string))
                    cnx.commit()

            # Next week
            week_start += 604800

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
