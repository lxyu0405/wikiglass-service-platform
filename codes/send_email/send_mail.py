#!/usr/bin/python
# -*- coding: utf-8 -*-
import mysql.connector
import time
import datetime
import math
import logging
import ConfigParser
from mysql.connector import errorcode
from datetime import timedelta


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
# email txt files directory
EMAIL_DIR = CONFIG.get("email_text", "text_directory")


def mean(values):
    return sum(values)/len(values)


# Standard deviation is not outputted to the email, for reference only
def stdev(values):
    length = len(values)
    m = mean(values)
    total_sum = 0
    for i in range(length):
        total_sum += (values[i]-m)**2
    return int(math.sqrt(total_sum/(length-1)))


try:
    logging.basicConfig(filename=LOGFILE, level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')

    # Connect to pbworks_db database
    cnx = mysql.connector.connect(user=PB_DB_USERNAME, password=PB_DB_PWD, host=PB_DB_HOST, database=PB_DB_NAME,
                                  charset='utf8mb4')
    cur = cnx.cursor(buffered=True)
    cur.execute("use " + PB_DB_NAME)

    # Get a teacher list
    cur.execute("""	SELECT DISTINCT(name), full_name
                    FROM Class_user,loginUser
                    WHERE Class_user.class_id LIKE '""" + YEAR +"""%'
                    AND Class_user.name = loginUser.user
                    AND Class_user.role = 'teacher'""")
    teacher_list = cur.fetchall()

    # name is email, and full_name is real name.
    teacher_email=[]
    teacher_name=[]
    for row in teacher_list:
        teacher_email.append(row[0])
        teacher_name.append(row[1])

    week_start = datetime.datetime.now().date() - timedelta(days=7)
    week_start = time.mktime(week_start.timetuple())
    week_end = datetime.datetime.now().date()
    week_end = time.mktime(week_end.timetuple())-1
    year = YEAR

    # Loop through all teacher
    for num in range(len(teacher_email)):
        teacher_email_i = teacher_email[num]
        teacher_name_i = teacher_name[num]
        print("teacher name: " + teacher_name_i + ", teacher email: " + teacher_email_i)

        class_name=[]
        # fetch class_id
        cur.execute("""	SELECT DISTINCT(class_id)
                        FROM Class_user
                        WHERE name = %s
                        AND Class_user.class_id LIKE '""" + YEAR +"""%'
                        AND Class_user.role = 'teacher'
                        AND Class_user.active = 1""", (teacher_email_i,))
        teacher_class_list = cur.fetchall()

        for row in teacher_class_list:
            class_name.append(row[0])
            print("class_id: " + row[0])

        # Loop through all classes
        for class_name_i in class_name:
            # Arrays storing revision counts of all groups
            group_rev_count=[]

            # Arrays storing best 3 and worst 3 groups in revision counts
            best_group_no=[]
            best_group_rev_count=[]
            worst_group_no=[]
            worst_group_rev_count=[]

            # Arrays storing best 5 and worst 5 students in word changes
            best_stu_name=[]
            best_stu_add=[]
            best_stu_del=[]
            worst_stu_name=[]
            worst_stu_add=[]
            worst_stu_del=[]

            # This part calculats the average value of the revision counts of each group in a class
            # Get a list of group number
            cur.execute(" SELECT group_no FROM Wiki WHERE class_name = %s",(class_name_i,))
            class_group_list = cur.fetchall()

            no_of_groups = len(class_group_list)
            # Get a list of revision counts of each group
            cur.execute("""	SELECT COUNT(t.group_no) AS count
                            FROM (
                                SELECT Wiki.group_no AS group_no
                                FROM Revision, User, Page, Wiki
                                WHERE Revision.user_id = User.user_id
                                AND Revision.page_id = Page.Page_id
                                AND Page.wiki_id = Wiki.wiki_id
                                AND Wiki.class_name = %s
                                AND perm = 'write'
                                AND Revision.timestamp BETWEEN %s AND %s) t
                                GROUP BY group_no""",(class_name_i, week_start, week_end,))
            group_rev_cnt_list = cur.fetchall()

            count = no_of_groups

            # Groups with revision counts
            for row in group_rev_cnt_list:
                group_rev_count.append(row[0])
                count = count-1

            # Check if the group made any revisions; If made no revision, isNull=True
            if count == no_of_groups:
                is_null = True
            else:
                is_null = False

            # Append zero to the remaining group
            while count > 0:
                group_rev_count.append(0)
                count = count-1

            # Output for this part
            if is_null:
                stat="Not a single group made any revisions this week. Please consider encouraging students to contribute more actively.</p>"
            else:
                avg = mean(group_rev_count)
                sd = stdev(group_rev_count)
                stat = "In this class, the average number of revisions per group is " + str(avg) + ". Following is a brief analysis of weekly performance of the class.</p>"
                print(str(sum(group_rev_count)) + " " + str(avg) + " " + str(sd))


            # This part is to get the Best 3 and Worst 3 groups in a class by comparing their revision counts
            # Get a list of the best 3 groups
            cur.execute("""SELECT t.group_no, COUNT(t.group_no) AS count
                                FROM (SELECT Wiki.group_no AS group_no
                                    FROM Revision, User, Page, Wiki
                                    WHERE Revision.user_id = User.user_id
                                    AND Revision.page_id = Page.Page_id
                                    AND Page.wiki_id = Wiki.wiki_id
									AND Wiki.class_name=%s
									AND perm = 'write'
									AND Revision.timestamp BETWEEN %s AND %s
									) t
							GROUP BY t.group_no
							ORDER BY count
							DESC LIMIT 3""",(class_name_i, week_start, week_end,))
            best_group = cur.fetchall()

            # Get a list of the worst 3 groups
            cur.execute("""SELECT t.group_no, COUNT(t.group_no) AS count
							FROM (SELECT Wiki.group_no AS group_no
									FROM Revision, User, Page, Wiki
									WHERE Revision.user_id = User.user_id
									AND Revision.page_id = Page.Page_id
									AND Page.wiki_id = Wiki.wiki_id
									AND Wiki.class_name=%s
									AND perm = 'write'
									AND Revision.timestamp BETWEEN %s AND %s
									) t
							GROUP BY t.group_no
							ORDER BY count
							ASC LIMIT 3""",(class_name_i, week_start, week_end,))
            worst_group = cur.fetchall()

            # Output Best 3 groups
            count = 0
            for row in best_group:
                best_group_no.append(row[0])
                best_group_rev_count.append(row[1])
                count += 1
            if count==3:
                best_group_comp = "<u><b>Group comparsion</u><br>Top 3 groups with the best performance:<p style=\"padding-left:3em;margin:2px;\">1. Group "+str(best_group_no[0]) \
                            +" ("+str(best_group_rev_count[0])+" revisions)<br>2. Group "+str(best_group_no[1])+" ("+str(best_group_rev_count[1]) \
                            +" revisions)<br>3. Group "+str(best_group_no[2])+" ("+str(best_group_rev_count[2])+" revisions)</p>"
            elif count==2:
                best_group_comp = "<u><b>Group comparsion</u><br>Top 2 groups with the best performance:<p style=\"padding-left:3em;margin:2px;\">1. Group "+str(best_group_no[0]) \
                            +" ("+str(best_group_rev_count[0])+" revisions)<br>2. Group "+str(best_group_no[1])+" ("+str(best_group_rev_count[1]) \
                            +" revisions)</p>"
            elif count==1:
                best_group_comp = "<u><b>Group comparsion</u><br>The only group made revision:<p style=\"padding-left:3em;margin:2px;\">1. Group "+str(best_group_no[0]) \
                            +" ("+str(best_group_rev_count[0])+" revisions)</p>"
            else:
                best_group_comp = ""

            # Output Worst 3 groups
            zero = []
            for row in worst_group:
                worst_group_no.append(row[0])
                worst_group_rev_count.append(row[1])
            count = no_of_groups - len(worst_group_no)	# count: number of groups with zero revision counts

            for group in class_group_list:
                if (group[0] not in worst_group_no):
                    zero.append(group[0])
            if count==0:
                worst_group_comp = "Top 3 groups with the worst performance:<p style=\"padding-left:3em;margin:2px;\">1. Group "+str(worst_group_no[0]) \
					+" ("+str(worst_group_rev_count[0])+" revisions)<br>2. Group "+str(worst_group_no[1])+" ("+str(worst_group_rev_count[1]) \
					+" revisions)<br>3. Group "+str(worst_group_no[2])+" ("+str(worst_group_rev_count[2])+" revisions)</p><br>"
            elif count==1:
                worst_group_comp = "Top 3 groups with the worst performance:<p style=\"padding-left:3em;margin:2px;\">1. Group "+str(zero[0]) \
					+" (0 revisions)<br>2. Group "+str(worst_group_no[0])+" ("+str(worst_group_rev_count[0]) \
					+" revisions)<br>3. Group "+str(worst_group_no[1])+" ("+str(worst_group_rev_count[1])+" revisions)</p><br>"
            elif count==2:
                worst_group_comp = "Top 3 groups with the worst performance:<p style=\"padding-left:3em;margin:2px;\">1. Group "+str(zero[0]) \
					+" (0 revisions)<br>2. Group "+str(zero[1])+" (0 revisions)<br>3. Group " \
					+str(worst_group_no[0])+" ("+str(worst_group_rev_count[0])+" revisions)</p><br>"
            elif count==no_of_groups:
                worst_group_comp = ""
            else:
                worst_group_comp = "Groups without making any revisions:<p style=\"padding-left:3em;margin:2px;\">"
                for i in range(1,count-1):
                    worst_group_comp = worst_group_comp+str(i)+". Group "+str(zero[i-1])+"<br>"
                worst_group_comp = worst_group_comp+str(count-1)+". Group "+str(zero[count-2])+"<br>"
                worst_group_comp = worst_group_comp+str(count)+". Group "+str(zero[count-1])+"</p><br>"


            # This part is to get the Best 5 and Worst 5 students in a class by comparing their word changes
            # Get a list all students
            cur.execute("""SELECT u.user_id, u.full_name, u.username, u.perm
							FROM User_wiki AS uw
							LEFT OUTER JOIN User AS u
								ON u.user_id = uw.uid
							INNER JOIN Wiki AS w
								ON uw.wiki_id = w.wiki_id
							WHERE w.class_name = %s
							AND u.perm = 'write'
							AND u.username IS NOT NULL
							ORDER BY u.perm""",(class_name_i ,))
            user_list = cur.fetchall()
            no_of_students = len(user_list)

            # Get a list of the Best 5 students
            cur.execute("""SELECT User_id, User_name, User_no, User_perm, COUNT(*), SUM(Words_addition), SUM(Words_deletion), SUM(Words_change)
							FROM Revision_Stats, Page, Wiki
							WHERE Revision_Stats.page_id = Page.Page_id
							AND Page.wiki_id = Wiki.wiki_id
							AND Wiki.class_name = %s
							AND Revision_creation_time BETWEEN %s AND %s
							AND User_perm = 'write'
							GROUP BY User_id
							ORDER BY SUM(Words_change) DESC LIMIT 5""",(class_name_i, week_start, week_end,))
            best = cur.fetchall()

            # Get a list of the Worst 5 students
            cur.execute("""SELECT User_id, User_name, User_no, User_perm, COUNT(*), SUM(Words_addition), SUM(Words_deletion), SUM(Words_change)
							FROM Revision_Stats, Page, Wiki
							WHERE Revision_Stats.page_id = Page.Page_id
							AND Page.wiki_id = Wiki.wiki_id
							AND Wiki.class_name = %s
							AND Revision_creation_time BETWEEN %s AND %s
							AND User_perm = 'write'
							GROUP BY User_id
							ORDER BY SUM(Words_change) ASC LIMIT 5""",(class_name_i, week_start, week_end,))
            worst = cur.fetchall()

            # Output Best 5 students
            user_of_page = []
            count = 0
            for row in best:
                user_name = row[1].encode('utf-8')
                total_a = row[5]
                total_d = row[6]
                best_stu_name.append(user_name)
                best_stu_add.append(total_a)
                best_stu_del.append(total_d)
                count = count + 1

            if count>=2:
                best_indiv_comp = u"\n<u><b>Individual performance</u><br>Top "+str(count)+" students in contribution:<p style=\"padding-left:3em;margin:2px;\">"
                for i in range(1,count-1):
                    best_indiv_comp = best_indiv_comp+str(i)+". "+str(best_stu_name[i-1]).title()+" ("+str(best_stu_add[i-1])+" words added, "+str(best_stu_del[i-1])+" words deleted)<br>"
                best_indiv_comp = best_indiv_comp+str(count-1)+". "+str(best_stu_name[count-2]).title()+" ("+str(best_stu_add[count-2])+" words added, "+str(best_stu_del[count-2])+" words deleted)<br>"
                best_indiv_comp = best_indiv_comp+str(count)+". "+str(best_stu_name[count-1]).title()+" ("+str(best_stu_add[count-1])+" words added, "+str(best_stu_del[count-1])+" words deleted)<br></p>"
            elif count==1:
                best_indiv_comp = "\n<u><b>Individual performance</u><br>The top student in contribution is<p style=\"padding-left:3em;margin:2px;\">"
                best_indiv_comp = best_indiv_comp+"1. "+str(best_stu_name[0]).title()+" ("+str(best_stu_add[0])+" words added, "+str(best_stu_del[0])+" words deleted).<br></p>"
            else:
                best_indiv_comp = u""

            # Output Worst 5 students
            user_of_page = []
            zero = []
            for row in worst:
                user_of_page.append(row[0])
                worst_stu_name.append(row[1].encode('utf-8'))
                worst_stu_add.append(row[5])
                worst_stu_del.append(row[6])
            count = no_of_students - len(user_of_page)		# count: number of students with zero revision counts
            print(str(no_of_students) + " === " + str(len(user_of_page)))
            for user in user_list:
                if (user[0] not in user_of_page):
                    user_name = user[1].encode('utf-8')
                    total_a = 0
                    total_d = 0
                    zero.append(user_name)
            if count==0:
                worst_indiv_comp = u"\nTop 5 inactive students:<p style=\"padding-left:3em;margin:2px;\">1. "+str(worst_stu_name[0]).title() \
                    +" ("+str(worst_stu_add[0])+" words added, "+str(worst_stu_del[0])+" words deleted)<br>2. "+str(worst_stu_name[1]).title()+" ("+str(worst_stu_add[1]) \
                    +" words added, "+str(worst_stu_del[1])+" words deleted)<br>3. "+str(worst_stu_name[2]).title()+" ("+str(worst_stu_add[2])+" words added, " \
                    +str(worst_stu_del[2])+" words deleted)<br>4. "+str(worst_stu_name[3]).title()+" ("+str(worst_stu_add[3])+" words added, "+str(worst_stu_del[3]) \
                    +" words deleted)<br>5. "+str(worst_stu_name[4]).title()+" ("+str(worst_stu_add[4])+" words added, "+str(worst_stu_del[4])+" words deleted)</p><br>"
            elif count==1:
                worst_indiv_comp = u"\nTop 5 inactive students:<p style=\"padding-left:3em;margin:2px;\">1. "+str(zero[0]).title() \
                    +" (0 words added, 0 words deleted)<br>2. "+str(worst_stu_name[0]).title()+" ("+str(worst_stu_add[0]) \
                    +" words added, "+str(worst_stu_del[0])+" words deleted)<br>3. "+str(worst_stu_name[1]).title()+" ("+str(worst_stu_add[1])+" words added, " \
                    +str(worst_stu_del[1])+" words deleted)<br>4. "+str(worst_stu_name[2]).title()+" ("+str(worst_stu_add[2])+" words added, "+str(worst_stu_del[2]) \
                    +" words deleted)<br>5. "+str(worst_stu_name[3]).title()+" ("+str(worst_stu_add[3])+" words added, "+str(worst_stu_del[3])+" words deleted)</p><br>"
            elif count==2:
                worst_indiv_comp = u"\nTop 5 inactive students:<p style=\"padding-left:3em;margin:2px;\">1. "+str(zero[0]).title() \
                    +" (0 words added, 0 words deleted)<br>2. "+str(zero[1]).title()+" (0 words added, 0 words deleted)<br>3. "+str(worst_stu_name[0]).title() \
                    +" ("+str(worst_stu_add[0])+" words added, "+str(worst_stu_del[0])+" words deleted)<br>4. "+str(worst_stu_name[1]).title()+" ("+str(worst_stu_add[1]) \
                    +" words added, "+str(worst_stu_del[1])+" words deleted)<br>5. "+str(worst_stu_name[2]).title()+" ("+str(worst_stu_add[2])+" words added, " \
                    +str(worst_stu_del[2])+" words deleted)</p><br>"
            elif count==3:
                worst_indiv_comp = u"\nTop 5 inactive students:<p style=\"padding-left:3em;margin:2px;\">1. "+str(zero[0]).title()\
                    +" (0 words added, 0 words deleted)<br>2. "+str(zero[1]).title()+" (0 words added, 0 words deleted)<br>3. "+str(zero[2]).title()+" (0 words added, " \
                    +"0 words deleted)<br>4. "+str(worst_stu_name[0]).title()+" ("+str(worst_stu_add[0])+" words added, "+str(worst_stu_del[0]) \
                    +" words deleted)<br>5. "+str(worst_stu_name[1]).title()+" ("+str(worst_stu_add[1])+" words added, "+str(worst_stu_del[1])+" words deleted)</p><br>"
            elif count==4 and no_of_students > 4:
                worst_indiv_comp = u"\nTop 5 inactive students:<p style=\"padding-left:3em;margin:2px;\">1. "+str(zero[0]).title() \
                    +" (0 words added, 0 words deleted)<br>2. "+str(zero[1]).title()+" (0 words added, 0 words deleted)<br>3. "+str(zero[2]).title()+" (0 words added, " \
                    +"0 words deleted)<br>4. "+str(zero[3]).title()+" (0 words added, 0 words deleted)<br>5. " \
                    +str(worst_stu_name[0]).title()+" ("+str(worst_stu_add[0])+" words added, "+str(worst_stu_del[0])+" words deleted)</p><br>"
            elif count == no_of_students:
                worst_indiv_comp = u""
            else:
                worst_indiv_comp = u"\nStudents without making any revisions:<p style=\"padding-left:3em;margin:2px;\">"
                for i in range(1,count-1):
                    worst_indiv_comp = worst_indiv_comp+str(i)+". "+str(zero[i-1]).title()+"<br>"
                worst_indiv_comp = worst_indiv_comp+"\n"+str(count-1)+". "+str(zero[count-2]).title()+"<br>"
                worst_indiv_comp = worst_indiv_comp+str(count)+". "+str(zero[count-1]).title()+"</p><br>"

            # Adding text up
            text = "From: wikiglass@ccmir.cite.hku.hk\nTo: " \
                    + teacher_email_i \
                    + "\nBcc: ecswikis@gmail.com, xiaoxhu@hku.hk, xh.gslis@gmail.com, xiaoyu.lu0405@gmail.com" + "\nContent-Type: text/html\nSubject: Weekly Summary - " + class_name_i + "\n\n" \
                    + "<html><head><title>Weekly Summary</title></head>" + "<body>Dear "+teacher_name_i+",<p><br>This is a weekly summary (" + datetime.datetime.fromtimestamp(week_start).strftime('%Y/%m/%d') \
                    + " - "+datetime.datetime.fromtimestamp(week_end).strftime('%Y/%m/%d') + ") of students' performance in Class " \
                    + class_name_i[-2:] + ". " + stat + best_group_comp + worst_group_comp + best_indiv_comp + best_indiv_comp + worst_indiv_comp \
                    + "Please log in to <a href='http://ccmir.cite.hku.hk/wikiglass/'>Wikiglass Site</a> for more details at any time. Data on Wikiglass are updated everyday.<p><br>" \
                    + "Yours sincerely,<br>Wikiglass</body></html>\n\n"
            with open(EMAIL_DIR + class_name_i + "_" + teacher_email_i+".txt", "w") as text_file:
                text_file.write(text)
            text_file.close()

    # Close	mysql database connection
    cur.close()

    # Close	mysql database connection
    cur.close()


except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    else:
        print(err)
else:
    cnx.close()

