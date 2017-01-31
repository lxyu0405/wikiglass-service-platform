#!/bin/bash

dbBackupDir="~/wikiglass-data-service/resources/pbworks_db-backup/"

now=$(date +"%Y%m%d")
old=$(date +"%Y%m%d" -d "5 days ago")
newFile=pbworks_$now.sql
oldFile=${dbBackupDir}pbworks_$old.sql

if [ -f $oldfile ] ; then
	rm $oldfile
	echo "Removed $oldfile"
else
	echo "No file is removed"
fi

echo "Start back up"
mysqldump -upbworks_usr -ppbworks_usr pbworks_db > ${dbBackupDir}$newfile
echo "Complete back up"
