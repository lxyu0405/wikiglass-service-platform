#!/bin/bash
# Weekly Database Update

weeklyPyDir="/home/oper/wikiglass-data-service/codes/data_analysis_weekly/"

now=$(date +"%T")

# Weekly_revision_count, Weekly_word_amendment, Weekly_word_count, Weekly_sentence_lvl
# Prepare data for timeline view in weekly basis
echo "$now Start weekly_timeline.py"
/usr/local/bin/python ${weeklyPyDir}weekly_timeline.py
now1=$(date +"%T")
echo "$now1 Complete weekly_timeline.py"