#!/bin/bash
sendMailDir="/home/oper/wikiglass-data-service/wikiglass-service-platform/codes/send_email/"

now=$(date +"%T")

echo "$now Start weekly_email.py"
/usr/local/bin/python ${sendMailDir}send_mail.py
now1=$(date +"%T")
echo "$now1 Complete weekly_email.py"
echo " "


