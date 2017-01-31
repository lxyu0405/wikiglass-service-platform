#!/bin/bash

now=$(date +"%Y%m%d")
old=$(date +"%Y%m%d" -d "5 days ago")
newfile=pbworks_$now.sql
oldfile=/home/oper/wikiglass-data-service/resources/pbworks_db-backup/pbworks_$old.sql

if [ -f $oldfile ] ; then
	rm $oldfile
	echo "Removed $oldfile"
else
	echo "No file is removed"
fi

echo "Start back up"
mysqldump -upbworks_usr -ppbworks_usr pbworks_db > /home/oper/wikiglass-data-service/resources/pbworks_db-backup/$newfile
echo "Complete back up"
