#!/bin/bash

logDir="/home/oper/wikiglass-data-service/log/"
pbworks_log=${logDir}pbworks.log
pb_DataExt_log=${logDir}pb_wikiDataExtraction.log
pb_DailyDB_log=${logDir}pb_dailyDBUpdate.log

cp ${pbworks_log} ${logDir}log_normal/$(date +"%Y%m%d").log
echo "" > ${pbworks_log}

cp ${pb_DataExt_log} ${logDir}log_wikiDataExtraction/$(date +"%Y%m%d").log
echo "" > ${pb_DataExt_log}

cp ${pb_DailyDB_log} ${logDir}log_dailyDBUpdate/$(date +"%Y%m%d").log
echo "" > ${pb_DailyDB_log}

