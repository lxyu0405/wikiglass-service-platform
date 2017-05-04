#!/bin/bash
sendMailDir="/home/oper/wikiglass-data-service/wikiglass-service-platform/codes/send_email/"

now=$(date +"%T")

echo "$now Start weekly_email.py"
/usr/local/bin/python ${sendMailDir}send_mail.py
now1=$(date +"%T")
echo "$now1 Complete weekly_email.py"
echo " "

echo "$now Start weekly_email-dade.py"
/usr/local/bin/python ${sendMailDir}send_mail-dade.py
now2=$(date +"%T")
echo "$now2 Complete weekly_email-dade.py"
echo " "

echo "$now1 Start sendEmail"
for txt in /home/oper/wikiglass-data-service/resources/email-txt/*
do
	if [ -f "$txt" ];then
		/usr/sbin/sendmail -t < $txt
	fi
done
now3=$(date +"%T")
echo "$now3 Complete sendEmail"