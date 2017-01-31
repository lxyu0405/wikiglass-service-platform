#!/bin/bash
# daily Database Update
# The execute order is important

dailyPyDir="/home/oper/wikiglass-data-service/codes/data_analysis_daily/"

now=$(date +"%T")

# tbl_sentence_quality.py
echo "$now Start tbl_sentence_quality.py"
/usr/local/bin/python ${dailyPyDir}tbl_sentence_quality.py
now1=$(date +"%T")
echo "$now1 Complete tbl_sentence_quality.py"

# tbl_revision_stats.py
echo "$now1 Start tbl_revision_stats.py"
/usr/local/bin/python ${dailyPyDir}tbl_revision_stats.py
now2=$(date +"%T")
echo "$now2 Complete tbl_revision_stats.py"

# tbl_page_stats.py
echo "$now2 Start tbl_page_stats.py"
/usr/local/bin/python ${dailyPyDir}tbl_page_stats.py
now3=$(date +"%T")
echo "$now3 Complete tbl_page_stats.py"

# tbl_group_stats.py
echo "$now3 Start tbl_group_stats.py"
/usr/local/bin/python ${dailyPyDir}tbl_group_stats.py
now4=$(date +"%T")
echo "$now4 Complete tbl_group_stats.py"

# tbl_user_page.py
echo "$now4 Start tbl_user_page.py"
/usr/local/bin/python ${dailyPyDir}tbl_user_page.py
now5=$(date +"%T")
echo "$now5 Complete tbl_user_page.py"

# tbl_user_group.py
echo "$now5 Start tbl_user_group.py"
/usr/local/bin/python ${dailyPyDir}tbl_user_group.py
now6=$(date +"%T")
echo "$now6 Complete tbl_user_group.py"

# tbl_revision_relation.py
echo "$now6 Start tbl_revision_relation.py"
/usr/local/bin/python ${dailyPyDir}tbl_revision_relation.py
now7=$(date +"%T")
echo "$now7 Complete tbl_revision_relation.py"

# tbl_revision_relation_stats.py
echo "$now7 Start tbl_revision_relation_stats.py"
/usr/local/bin/python ${dailyPyDir}tbl_revision_relation_stats.py
now8=$(date +"%T")
echo "$now8 Complete tbl_revision_relation_stats.py"

# Daily_revision_count, Daily_word_amendment, Daily_word_count, Daily_sentence_level, Daily_revision_relation
# daily_timeline.py
echo "$now8 Start daily_timeline.py"
/usr/local/bin/python ${dailyPyDir}daily_timeline.py
now9=$(date +"%T")
echo "$now9 Complete daily_timeline.py"
