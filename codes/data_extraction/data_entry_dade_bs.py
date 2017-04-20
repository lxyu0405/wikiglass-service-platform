#!/usr/bin/python
import mysql.connector
import logging
import ConfigParser
from mysql.connector import errorcode
import datetime
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
# bluespice_db config
BS_DB_USERNAME = CONFIG.get("bluespice_db_conf", "username")
BS_DB_PWD = CONFIG.get("bluespice_db_conf", "password")
BS_DB_HOST = CONFIG.get("bluespice_db_conf", "db_host")
BS_DB_NAME = CONFIG.get("bluespice_db_conf", "db_name")
# log file path
LOGFILE = CONFIG.get("logs_conf", "common_log")


try:
    logging.basicConfig(filename=LOGFILE, level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')

    # Connect to pbworks_db database
    cnx_pbworks = mysql.connector.connect(user=PB_DB_USERNAME, password=PB_DB_PWD, host=PB_DB_HOST, database=PB_DB_NAME)
    cur_pbworks = cnx_pbworks.cursor(buffered=True)
    cur_pbworks.execute("use " + PB_DB_NAME)
    # Connect to bluespice_db database
    cnx_bs = mysql.connector.connect(user=BS_DB_USERNAME, password=BS_DB_PWD, host=BS_DB_HOST, database=BS_DB_NAME)
    cur_bs = cnx_bs.cursor(buffered=True)
    cur_bs.execute("use " + BS_DB_NAME)

    school_name = 'dade'

    prefix_class_dic = {
        'chn1b': 'chn',
        'chn2b': 'chn',
        'chn3b': 'chn',
        'chn4b': 'chn',
        'chn5b': 'chn',
        'chn6b': 'chn',
        'math1b': 'math',
        'math2b': 'math',
        'math3b': 'math',
        'math4b': 'math',
        'math5b': 'math',
        'math6b': 'math',
        'chn1a': 'chn',
        'chn2a': 'chn',
        'chn3a': 'chn',
        'chn4a': 'chn',
        'chn5a': 'chn',
        'chn6a': 'chn',
        'math1a': 'math',
        'math2a': 'math',
        'math3a': 'math',
        'math4a': 'math',
        'math5a': 'math',
        'math6a': 'math',
    }

    for prefix in prefix_class_dic.keys():
        class_name = prefix_class_dic[prefix]

        user_table = prefix + '_user'
        user_group_table = prefix + '_user_groups'
        page_table = prefix + '_page'
        revision_table = prefix + '_revision'
        revision_text_table = prefix + '_text'

        wiki_id = '2016' + school_name + class_name + '_' + prefix[-1] + 'gp' + prefix[-2]
        print(wiki_id)

        useless_pages_id = []

        # [Page BlueSpice] select page information from bluespice
        cur_bs.execute("SELECT page_id, page_title FROM " + page_table)
        page_info_list = cur_bs.fetchall()
        print("wiki_id: " + wiki_id + " pages: " + str(len(page_info_list)))
        # [Page PBworks] update page data into pbworks_db
        for page_info in page_info_list:
            page_id = wiki_id + '_' + str(page_info[0])
            page_name = page_info[1]
            if 'Sidebar' in page_name:
                useless_pages_id.append(page_info[0])
                continue
            logging.debug('[dataEntry_bs_chn] page_id: ' + page_id + ' page_name: ' + page_name)
            cur_pbworks.execute(""" INSERT INTO Page (page_id, wiki_id, page_name, page_url)
                                    VALUES (%s, %s, %s, %s)
                                    ON DUPLICATE KEY UPDATE page_name = if( page_name <> values(page_name),
                                    values(page_name), page_name )""",(page_id, wiki_id, page_name, ''))
            cnx_pbworks.commit()

        # [User BlueSpice] select user information from bluespice
        cur_bs.execute("""	SELECT user.user_id, user.user_name, user_groups.ug_group
                            FROM """ + user_table + """ AS user, """ + user_group_table + """ AS user_groups
                            WHERE user.user_id = user_groups.ug_user""")
        user_info_list = cur_bs.fetchall()

        # [User PBworks] update user data into pbworks_db
        for user_info in user_info_list:
            user_id = wiki_id + 'user' + str(user_info[0])
            user_name = user_info[1]
            user_group = user_info[2]
            user_perm = 'admin' if ( user_group == 'sysop' or user_group == 'bureaucrat' or user_group == 'teacher') else 'write'

            logging.debug('[dataEntry_bs_chn] user_id: ' + user_id + ' user_name: ' + user_name + ' user_group: ' + user_group + ' user_perm: ' + user_perm)

            cur_pbworks.execute("""	INSERT INTO User (user_id, full_name, username, perm)
                                    VALUES (%s, %s, %s, %s)
                                    ON DUPLICATE KEY UPDATE
                                    full_name = if( full_name <> values(full_name), values(full_name), full_name ),
                                    username = if( username <> values(username), values(username), username ),
                                    perm = if( perm <> values(perm), values(perm), perm ) """,
                                (user_id, user_name, user_name, user_perm))
            cnx_pbworks.commit()

            if user_perm == 'write':
                cur_pbworks.execute("""	INSERT INTO User_wiki (user_id, uid, wiki_id)
                                        VALUES (%s, %s, %s)
                                        ON DUPLICATE KEY UPDATE
                                        user_id = if( user_id <> values(user_id), values(user_id), user_id )""",
                                    (user_id, user_id, wiki_id))
                cnx_pbworks.commit()

        # [Revision BlueSpice] select revision information from bluespice
        cur_bs.execute("""	SELECT rev.rev_id, rev.rev_page, rev.rev_timestamp, rev.rev_user, revtext.old_text, rev.rev_len
                            FROM """ + revision_table + """ AS rev, """ + revision_text_table + """ AS revtext
                            WHERE rev.rev_text_id = revtext.old_id
                            ORDER BY rev.rev_id """)
        rev_info_list = cur_bs.fetchall()

        # [Revision PBworks] update revision data into pbworks_db
        rev_page_dict = {}
        for rev_info in rev_info_list:
            rev_id = str(rev_info[0])
            rev_page_id = wiki_id + '_' + str(rev_info[1])
            rev_timestamp = time.mktime(datetime.datetime.strptime(str(rev_info[2]), '%Y%m%d%H%M%S').timetuple())
            rev_user = wiki_id + 'user' + str(rev_info[3])
            rev_content_raw = rev_info[4]
            rev_content = rev_info[4].decode("utf-8").encode('unicode_escape')
            rev_len = rev_info[5]

            if rev_info[1] in useless_pages_id:
                continue

            if rev_page_id in rev_page_dict.keys():
                rev_page_dict[rev_page_id] += 1
            else:
                rev_page_dict[rev_page_id] = 1
            rev_version = rev_page_dict[rev_page_id]

            # logging.debug('[dataEntry_bs_chn] rev_page_id: ' + rev_page_id + ' rev_timestamp: ' + rev_timestamp + ' rev_user: ' + rev_user + ' rev_len: ' + str(rev_len))
            # logging.debug('[dataEntry_bs_chn] rev_version: ' + str(rev_version) + ' rev_content: ' + rev_content_raw)

            cur_pbworks.execute("""	INSERT INTO Revision (page_id, timestamp, version, oid, user_id, content)
                                    SELECT * FROM (
                                        SELECT %s, %s, %s, %s, %s, %s) AS tmp
                                        WHERE NOT EXISTS (
                                            SELECT *
                                            FROM Revision
                                            WHERE page_id = %s AND version = %s
                                            ) LIMIT 1 """, (rev_page_id, rev_timestamp, rev_version, 0, rev_user, rev_content, rev_page_id, rev_version))
            cnx_pbworks.commit()

    # Close	mysql database connection
    cur_bs.close()
    cur_pbworks.close()

except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Error: Wrong name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database doesn't exist")
    else:
        print(err)
else:
    cnx_pbworks.close()
    cnx_bs.close()


