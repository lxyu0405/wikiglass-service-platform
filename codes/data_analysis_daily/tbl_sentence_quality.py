#!/usr/bin/python
# -*- coding: utf-8 -*-
import mysql.connector
import logging
import datetime
import time
import ConfigParser
from mysql.connector import errorcode

# diy library
import sys
sys.path.append('/home/oper/wikiglass-data-service/wikiglass-service-platform/codes/common')
sys.path.append('/home/oper/wikiglass-data-service/wikiglass-service-platform/codes/models')
from char_utils import CharTools
from RevisionModel import RevisionModel
from UserModel import UserModel
from SentenceModel import SentenceQualityModel

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
# in 'daily' mode, only recent data need to be analyzed.
# else, whole year's data
RUN_MODE = 2 if CONFIG.get("run_mode", "mode") == 'daily' else 366
# NLP prepare file path
NLP_PREPARE = CONFIG.get("nlp_conf", "prepare")


try:
    logging.basicConfig(filename=LOGFILE, level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')

    # Connect to pbworks_db database
    cnx = mysql.connector.connect(user=PB_DB_USERNAME, password=PB_DB_PWD, host=PB_DB_HOST, database=PB_DB_NAME,
                                  charset='utf8mb4')
    cur = cnx.cursor(buffered=True)
    cur.execute("use " + PB_DB_NAME)

    # prepare tools
    execfile(NLP_PREPARE)
    nlp_kit = pyltpAdapter()
    classifier = TextClassificationFacet(nlp_kit)

    end_date = datetime.datetime.now()
    end_unix_timestamp = time.mktime(end_date.timetuple())
    end_date_string = end_date.strftime("%y-%m-%d %H:%M:%S")
    start_date = datetime.datetime.now() - datetime.timedelta(days=RUN_MODE)
    start_unix_timestamp = time.mktime(start_date.timetuple())
    start_date_string = start_date.strftime("%y-%m-%d %H:%M:%S")

    cur.execute(""" SELECT Revision.page_id, User.user_id, User.full_name, Revision.revision_id,
                    Revision.version, Revision.content, Revision.timestamp
                    FROM Revision
                    INNER JOIN User
                    ON Revision.user_id = User.user_id
                    WHERE User.perm = 'write' AND Revision.timestamp BETWEEN %s AND %s
                    ORDER BY Revision.version""", (start_unix_timestamp, end_unix_timestamp))
    revision_user_list = cur.fetchall()

    logging.debug("[tbl_sentence_quality] " + start_date_string + " ~ " + end_date_string + ": " + str(len(revision_user_list)))

    # extract useful information from sql result
    for revision_user in revision_user_list:
        revision_model = RevisionModel()
        revision_model.page_id = revision_user[0]
        revision_model.user_id = revision_user[1]
        revision_model.revision_id = revision_user[3]
        revision_model.version = revision_user[4]
        revision_model.content = revision_user[5].decode('unicode_escape')
        revision_model.timestamp = revision_user[6]

        user_model = UserModel()
        user_model.user_id = revision_user[1]
        user_model.full_name = revision_user[2]

        # empty content
        if revision_model.content is None or len(revision_model.content) == 0:
            continue

        # remove html tags, nbsp, \n and whitespaces from raw content
        revision_model.content = CharTools.clean_nbsp(CharTools.clean_meaningless_symbol(
                CharTools.clean_spaces(CharTools.clean_html_tags(revision_model.content))))

        # non-chinese content OR too short content
        if not CharTools.check_contain_chinese(revision_model.content.encode('utf-8')) or len(revision_model.content) == 0:
            continue

        # set limit to average length of sentence
        # too long sentence may cause nlp module crash
        average_sentence_length = CharTools.average_sentences_length(revision_model.content)
        if average_sentence_length > 2000:
            continue

        logging.debug("[tbl_sentence_quality] revision_id: " + str(revision_model.revision_id)
                      + " page_id: " + revision_model.page_id
                      + " version: " + str(revision_model.version)
                      + " AVERAGE(len): " + str(average_sentence_length))
        # call quality evaluation function
        nlp_res = classifier.classify_text(revision_model.content)

        analyze_dic = {}
        for t2l in nlp_res:
            nlp_res_content, nlp_res_quality = t2l[0], t2l[1]
            nlp_res_content = CharTools.clean_alph(nlp_res_content)
            if not CharTools.check_contain_chinese(nlp_res_content.encode('utf-8')):
                continue
            analyze_dic[nlp_res_content] = nlp_res_quality

        cur.execute(""" SELECT sentence_id, content, current_version, current_revision_id
                        FROM Sentence_quality
                        WHERE sentence_id LIKE '{0}\_%'""".format(revision_model.page_id))
        page_sentences_list = cur.fetchall()

        page_sentence_cnt = len(page_sentences_list)
        for page_sentence in page_sentences_list:
            sentence_model = SentenceQualityModel()
            sentence_model.sentence_id = page_sentence[0]
            sentence_model.content = page_sentence[1]
            sentence_model.current_version = page_sentence[2]
            sentence_model.current_revision_id = page_sentence[3]
            # if already contains the sentence
            # update the version
            # remove it from analyze_dic
            if sentence_model.content in analyze_dic.keys():
                cur.execute(""" UPDATE Sentence_quality
                                SET current_version = %s, current_revision_id = %s, timestamp = %s, update_time = %s
                                WHERE sentence_id = %s""",
                            (revision_model.version, revision_model.revision_id, revision_model.timestamp,
                             end_date_string, sentence_model.sentence_id))
                cnx.commit()
                del analyze_dic[sentence_model.content]
                logging.debug("[tbl_sentence_quality] Update Sentence(" + sentence_model.sentence_id + ") Version, "
                              + " from " + str(sentence_model.current_version) + " to " + str(revision_model.version))

        # rest of analyze_dic should be newly added sentences
        # insert into the database
        for key in analyze_dic.keys():
            page_sentence_cnt += 1
            new_sentence_id = revision_model.page_id + "_" + str(page_sentence_cnt)
            try:
                cur.execute(""" INSERT INTO Sentence_quality (sentence_id, page_id, user_name, user_id,
                                initial_revision_id, current_revision_id, initial_version, current_version,
                                timestamp, level, content, create_time, update_time)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                            (new_sentence_id, revision_model.page_id, user_model.full_name, user_model.user_id,
                             revision_model.revision_id, revision_model.revision_id, revision_model.version,
                             revision_model.version, revision_model.timestamp, analyze_dic[key],
                                key, end_date_string, end_date_string))
                cnx.commit()
            except mysql.connector.Error as err:
                print("[tbl_sentence_quality] Insert exception, content: " + key + ", level:" + analyze_dic[key])
            logging.debug("[tbl_sentence_quality] Insert into Sentence_quality, sentence_id: " + new_sentence_id
                            + " , content: " + key + ", level: " + analyze_dic[key])

    # release memory after the task
    del classifier
    del nlp_kit

    # close mysql database connection
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
