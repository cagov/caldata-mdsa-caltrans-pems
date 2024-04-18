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
# ./aws_upload_daemon_script.sh "D3.VDS30SEC,D4.VDS30SEC,D5.VDS30SEC,D6.VDS30SEC,D7.VDS30SEC,D8.VDS30SEC,D10.VDS30SEC,D11.VDS30SEC,D12.VDS30SEC"
#
# ```
# Make sure that `topic_aws_uploader.py` is available for the script to run successfully.
#
# In production, this script is used as a daemon process.
#
#

cd /nfsdata/dataop/uploader
# Assuming the TOPICs you want to copy are passed in as the first command line argument. 
# Eg: "topic1,topic2,topic3"
topics=$1  

IFS=',' read -ra ADDR <<< "$topics"

# Start an infinite loop
# Traverse through each TOPIC and run the copy command
for topic in "${ADDR[@]}"; do
echo "Now start uploading $topic"
echo "Executing script below:"
echo python3 table_aws_uploader.py --topic $topic --date_time default --window 21600
python3 table_aws_uploader.py --topic $topic --date_time default --window 21600
done


/tmp/table_aws_uploader.log:2024-03-20T17:33:59.874139-0700 INFO table_aws_uploader.py:1062 Exe
cuting command/usr/local/bin/aws s3 cp /nfsdata/dataop/uploader/tmp/D4.VDS30SEC/D4.VDS30SEC_dum
p_20240320T234819Z_21b93a5fbcdfea92.parquet s3://caltrans-pems-dev-us-west-2-raw/db96_export_st
aging_area/tables/VDS30SEC/district=D4/year=2024/month=3/day=14/ --ca-bundle /etc/pki/ca-trust/
extracted/openssl/ca-bundle.trust.crt



