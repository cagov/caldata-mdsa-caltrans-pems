#!/bin/bash
# aws_upload_daemon_script.sh
# This bash script is designed to upload data from multiple kafka topics to AWS using a Python script
#   (`topic_aws_uploader.py`).
#
# The script takes the list of topics as a command line argument, which is then split into individual topics
#  using the ',' delimiter. It enters an infinite loop where it iterates through each topic and executes
#  the Python script `topic_aws_uploader.py` with the respective topic as an argument.
#
# After processing all the topics, it waits for 10 minutes before starting the next iteration.
#
# To use this script, you would run it from the command line with the list of topics as the first argument.
# For example:
# ```
# ./aws_upload_daemon_script.sh "topic1,topic2,topic3"
# ./aws_upload_daemon_script.sh "D3.VDS30SEC,D4.VDS30SEC"
# ```
# Make sure that `topic_aws_uploader.py` is available for the script to run successfully.
#
# In production, this script is used as a daemon process.
#
#

# Assuming the TOPICs you want to copy are passed in as the first command line argument. 
# Eg: "topic1,topic2,topic3"
topics=$1  

IFS=',' read -ra ADDR <<< "$topics"

# Start an infinite loop
while true; do
  # Traverse through each TOPIC and run the copy command
  for topic in "${ADDR[@]}"; do
    echo "Now start uploading $topic"
    echo "Executing script below:"
    echo python3 topic_aws_uploader.py --topic $topic --date_time default
    python3 topic_aws_uploader.py --topic $topic --date_time default
  done

  # Wait for 10 minutes before the next iteration
  echo "Waiting for 10 minutes before the next iteration..."
  sleep 10m
done
