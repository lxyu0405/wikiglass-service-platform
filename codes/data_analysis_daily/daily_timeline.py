#!/usr/bin/python
# -*- coding: utf-8 -*-
import mysql.connector
import datetime
import ConfigParser
import logging
from mysql.connector import errorcode
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

try:
    logging.basicConfig(filename=LOGFILE, level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')

    # Connect to pbworks_db database
    cnx = mysql.connector.connect(user=PB_DB_USERNAME, password=PB_DB_PWD, host=PB_DB_HOST, database=PB_DB_NAME,
                                  charset='utf8mb4')
    cur = cnx.cursor(buffered=True)
    cur.execute("use " + PB_DB_NAME)

    # Current timestamp (ending time of the execution)
    end_time = time.time()

    # Getting a list of all schools
    cur.execute(" SELECT school, start_time FROM School WHERE year = %s", (YEAR,))
    school_list = cur.fetchall()

    # Summarizing data for all schools
    for school in school_list:
        school_name = school[0]
        start_time = school[1].strftime("%Y-%m-%d %H:%M:%S")
        day_start = time.mktime(datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S").timetuple())
        day_start_string = datetime.datetime.fromtimestamp(day_start).strftime('%Y-%m-%d %H:%M:%S')
        print school_name

        # Getting a list of all groups
        cur.execute("SELECT wiki_id FROM Wiki WHERE year = %s AND school = %s", (YEAR, school_name,))
        group_list = cur.fetchall()

        # Getting a list of all students
        cur.execute(""" SELECT u.full_name, uw.wiki_id
                        FROM User_wiki AS uw
                        LEFT OUTER JOIN User AS u ON u.user_id = uw.uid
                        INNER JOIN pbworks_db.Wiki As w ON w.wiki_id = uw.wiki_id
                        WHERE w.year = %s AND w.school = %s AND u.perm = 'write' AND u.username IS NOT NULL
                        ORDER BY uw.wiki_id,u.username""", (YEAR, school_name,))
        student_list = cur.fetchall()

        # Summarizing data in daily basis
        while day_start < end_time:
            day_start_string = datetime.datetime.fromtimestamp(day_start).strftime('%Y-%m-%d %H:%M:%S')
            day_end = day_start + 86399
            day_end_string = datetime.datetime.fromtimestamp(day_end).strftime('%Y-%m-%d %H:%M:%S')

            # Revision count region
            if True:
                # Getting revision count (number of revision counts of each group)
                cur.execute(""" SELECT w.wiki_id, COUNT(*)
                                FROM Revision_Stats AS r, Wiki AS w
                                WHERE r.page_id LIKE CONCAT(w.wiki_id,'\_%') AND w.year = %s AND w.school = %s
                                AND r.Revision_creation_time BETWEEN %s AND %s
                                GROUP BY w.wiki_id""", (YEAR, school_name, day_start, day_end,))
                rev_data = cur.fetchall()

                # Inserting rev_data of revision count (If a group does not have any revision counts,
                # set 0 to the revision_count)
                group_id = []
                rev_count = []
                for row in rev_data:
                    wiki_id = row[0]
                    count = row[1]
                    group_id.append(wiki_id)
                    rev_count.append(count)
                    cur.execute(""" INSERT INTO Daily_revision_count (group_id, revision_count, ts_day_start)
                                    VALUES (%s,%s,%s) ON DUPLICATE KEY UPDATE
                                    revision_count = if( revision_count <> values(revision_count),
                                    values(revision_count), revision_count )""",
                                (wiki_id, count, day_start_string))
                cnx.commit()
                for group in group_list:
                    if group[0] not in group_id:
                        cur.execute(""" INSERT INTO Daily_revision_count (group_id,revision_count,ts_day_start)
                                        VALUES (%s,%s,%s) ON DUPLICATE KEY UPDATE
                                        revision_count = if( revision_count <> values(revision_count),
                                        values(revision_count), revision_count )""",
                                    (group[0], 0, day_start_string))
                cnx.commit()

            # Word count region
            if True:
                # Getting word count (number of word counts of each group)
                cur.execute(""" SELECT w.wiki_id, SUM(r.Words_change)
                                FROM Revision_Stats AS r, Wiki AS w
                                WHERE r.page_id LIKE CONCAT(w.wiki_id,'\_%') AND w.year = %s AND w.school = %s
                                AND r.Revision_creation_time BETWEEN %s AND %s
                                GROUP BY w.wiki_id""", (YEAR, school_name, day_start, day_end,))
                rev_data = cur.fetchall()

                # Inserting rev_data of word count (If a group does not have any word counts, set 0 to the word_count)
                group_id = []
                word_count = []
                for row in rev_data:
                    wiki_id = row[0]
                    count = row[1]
                    group_id.append(wiki_id)
                    word_count.append(count)
                    cur.execute(""" INSERT INTO Daily_word_count (group_id,word_count,ts_day_start)
                                    VALUES (%s,%s,%s) ON DUPLICATE KEY UPDATE
                                    word_count = if( word_count <> values(word_count), values(word_count), word_count )""",
                                (wiki_id, count, day_start_string))
                cnx.commit()
                for group in group_list:
                    if group[0] not in group_id:
                        cur.execute(""" INSERT INTO Daily_word_count (group_id,word_count,ts_day_start)
                                        VALUES (%s,%s,%s) ON DUPLICATE KEY UPDATE
                                        word_count = if( word_count <> values(word_count), values(word_count), word_count )""",
                                    (group[0], 0, day_start_string))
                cnx.commit()

            # Word amendment region
            if True:
                # Getting word amendment (number of word changes of each student)
                cur.execute(""" SELECT w.wiki_id, r.User_name, SUM(r.Words_change)
                                FROM Revision_Stats AS r, Wiki AS w
                                WHERE r.page_id LIKE CONCAT(w.wiki_id,'\_%') AND w.year = %s AND w.school = %s
                                AND r.Revision_creation_time BETWEEN %s AND %s AND r.User_perm = 'write'
                                GROUP BY r.User_id
                                ORDER BY w.wiki_id, r.User_id""", (YEAR, school_name, day_start, day_end,))
                rev_data = cur.fetchall()

                # Inserting rev_data of word amendment (If a student does not have any word changes,
                # set 0 to the word_amendment_count)
                group_id = []
                student_name = []
                word_amendment_count = []
                for row in rev_data:
                    wiki_id = row[0]
                    student = row[1]
                    count = row[2]
                    group_id.append(wiki_id)
                    student_name.append(student)
                    word_amendment_count.append(count)
                    cur.execute(""" INSERT INTO Daily_word_amendment (group_id,student_name,word_amendment_count,ts_day_start)
                                    VALUES (%s,%s,%s,%s) ON DUPLICATE KEY UPDATE
                                    word_amendment_count = if( word_amendment_count <> values(word_amendment_count),
                                    values(word_amendment_count), word_amendment_count )""",
                                    (wiki_id, student, count, day_start_string))
                cnx.commit()
                for student in student_list:
                    if student[0] not in student_name:
                        cur.execute(""" INSERT INTO Daily_word_amendment (group_id,student_name,word_amendment_count,ts_day_start)
                                        VALUES (%s,%s,%s,%s) ON DUPLICATE KEY UPDATE
                                        word_amendment_count = if( word_amendment_count <> values(word_amendment_count),
                                        values(word_amendment_count), word_amendment_count )""",
                                        (student[1], student[0], 0, day_start_string))
                cnx.commit()

            # Sentence level count region
            if True:
                low_lvl_dict, high_lvl_dict, user_group_dict, user_name_dict = {}, {}, {}, {}

                cur.execute(""" SELECT ug.User_id, ug.User_name, ug.Group_id
                                FROM User_stats_by_group AS ug, Wiki AS w
                                WHERE ug.Group_id = w.wiki_id AND ug.User_perm = 'write'
                                AND w.year = %s AND w.school = %s""", (YEAR, school_name,))
                user_info_list = cur.fetchall()

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
                cur.execute(""" SELECT user_name, user_id, level
                                FROM Sentence_quality
                                WHERE timestamp BETWEEN %s AND %s""", (day_start, day_end))
                sentence_info_list = cur.fetchall()

                logging.debug("[tbl_daily_sentence_lvl_count] " + day_start_string + " -> " + day_end_string
                              + " : " + str(len(sentence_info_list)))

                # extract useful information from SQL query result
                for sentence in sentence_info_list:
                    sentence_user_name = sentence[0]
                    sentence_user_id = sentence[1]
                    sentence_level = sentence[2]

                    if sentence_user_id in low_lvl_dict.keys() and sentence_level == "level 1":
                        low_lvl_dict[sentence_user_id] += 1
                    if sentence_user_id in high_lvl_dict.keys() and sentence_level == "level 3":
                        high_lvl_dict[sentence_user_id] += 1

                # summarize data and insert into Daily_sentence_level_stats table
                for sum_user_id in user_group_dict.keys():
                    sum_group_id = user_group_dict[sum_user_id]
                    sum_user_name = user_name_dict[sum_user_id]
                    sum_low_thinking_cnt = low_lvl_dict[sum_user_id]
                    sum_high_thinking_cnt = high_lvl_dict[sum_user_id]

                    cur.execute(""" INSERT INTO Daily_sentence_level_stats (group_id, student_name,
                                    high_thinking_count, low_thinking_count, ts_day_start, ts)
                                    VALUES (%s, %s, %s, %s, %s, %s) ON duplicate key UPDATE
                                    high_thinking_count = if ( high_thinking_count <> values(high_thinking_count),
                                    values(high_thinking_count), high_thinking_count ),
                                    low_thinking_count = if ( low_thinking_count <> values(low_thinking_count),
                                    values(low_thinking_count), low_thinking_count )""",
                                (sum_group_id, sum_user_name, sum_high_thinking_cnt, sum_low_thinking_cnt,
                                 day_start_string, day_end_string))
                    cnx.commit()

            # revision_relation_stats region
            if True:
                cur.execute(""" SELECT DISTINCT(rr.group_id)
                                    FROM Revision_relation AS rr
                                    WHERE rr.revision_id IN
                                    (
                                        SELECT r.revision_id
                                        FROM Revision AS r
                                        WHERE r.timestamp BETWEEN %s AND %s
                                    )""", (day_start, day_end))

                group_id_list = cur.fetchall()
                logging.debug("[tbl_revision_relation_stats_daily] " + day_start_string
                              + " ~ " + day_end_string + ": " + str(len(group_id_list)))

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

                    cur.execute("""SELECT rr.user_from_id, rr.user_from_name, rr.user_to_id, rr.user_to_name
                                    FROM Revision_relation AS rr
                                    WHERE rr.group_id = %s AND rr.revision_id IN
                                    (   SELECT r.revision_id
                                        FROM Revision AS r
                                        WHERE r.timestamp BETWEEN %s AND %s
                                    )""", (group_id[0], day_start, day_end))
                    group_revision_list = cur.fetchall()

                    for group_revision in group_revision_list:
                        this_key = group_revision[0] + "&^&" + group_revision[1] + "->" + \
                                   group_revision[2] + "&^&" + group_revision[3]
                        if this_key not in group_stats_dict.keys():
                            continue
                        else:
                            group_stats_dict[this_key] += 1

                    # analyze the result in group_stats_dict
                    for key in group_stats_dict.keys():
                        user_from, user_to = key.split("->")
                        user_from_id, user_from_name = user_from.split("&^&")
                        user_to_id, user_to_name = user_to.split("&^&")
                        logging.debug("Insert Revision_relation_stats_daily "
                                      + user_from_id + " (" + user_from_name + ") -> "
                                      + user_to_id + " (" + user_to_name + ") [" + str(group_stats_dict[key]) + "]")

                        cur.execute(""" INSERT INTO Revision_relation_stats_daily (user_from_id, user_from_name,
                                        user_to_id, user_to_name, group_id, total_revision_count, time_stamp)
                                        VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                                    (user_from_id, user_from_name, user_to_id, user_to_name, group_id[0],
                                     group_stats_dict[key], day_start_string))
                        cnx.commit()

            # Next day
            day_start += 86400

    cur.close()


except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Error: Wrong name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("database doesn't exist")
    else:
        print(err)
else:
    cnx.close()
