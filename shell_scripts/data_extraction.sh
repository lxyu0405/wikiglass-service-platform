#!/bin/bash
# Wiki Data Extraction

extractPyDir="~/wikiglass-data-service/codes/data_extraction/"

now=$(date +"%T")

#Data Extraction from PBworks
echo "$now Start data_entry_twgss.py"
/usr/local/bin/python ${extractPyDir}data_entry_twgss.py
now1=$(date +"%T")
echo "$now1 Complete data_entry_twgss.py"
echo " "

echo "$now1 Start data_entry_kyc.py"
/usr/local/bin/python ${extractPyDir}data_entry_kyc.py
now2=$(date +"%T")
echo "$now2 Complete data_entry_kyc.py"
echo " "

#Data Extraction from BlueSpice
echo "$now Start data_entry_bs.py"
/usr/local/bin/python ${extractPyDir}data_entry_bs.py
now3=$(date +"%T")
echo "$now1 Complete data_entry_bs.py"


