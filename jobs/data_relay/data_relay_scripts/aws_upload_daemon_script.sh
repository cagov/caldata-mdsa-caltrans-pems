#!/bin/bash
# aws_upload_daemon_script.sh
# 
# This Bash script is a handy utility wrapper for the topic_aws_uploader.py Python script, 
# which operates on a single specified data table name (or topic). The script takes a list of topics (data table name) 
# as input, breaks them into individual entries, and iterates through the list in a loop. For each topic (data table name), 
# it invokes topic_aws_uploader.py, passing one topic at a time as an argument.
#
# To use this script, you would run it from the command line with the list of topics as the first argument.
# For example:
# ```
# ./aws_upload_daemon_script.sh "D3.VDS30SEC,D4.VDS30SEC,D5.VDS30SEC"
# ```
# 
# Ensure that the topic_aws_uploader.py script is present and accessible for this Bash script to 
# function correctly.
# 
# This script will be used by a cron command, please review crontab.txt for examples on how to use this script.
# 
# 


cd /nfsdata/dataop/uploader # enter into the same folder as the table_aws_uploader.py script deployed to

# Assuming the TOPICs you want to copy are passed in as the first command line argument. 
# Eg: "topic1,topic2,topic3"
topics=$1  

IFS=',' read -ra ADDR <<< "$topics"

for topic in "${ADDR[@]}"; do
  echo "Now start uploading $topic"
  echo python3 table_aws_uploader.py --topic $topic --snow_env PROD
  python3.9 table_aws_uploader.py --topic $topic --snow_env PROD 

done

