

## Big picture of data relay
The data relay process employs a task queue model, wherein a specific crawl task 
is outlined as an entry in the task queue. This model enables the system to continuously 
poll for new tasks, executing each crawl task as it appears. Intermediate data from the 
crawl tasks is saved into the Kafka. Another function, known as the topic_uploader, polls 
the Kafka for this intermediate data, uploading it to AWS.


The adoption of Kafka offers numerous advantages. Primarily, it improves latency as the 
crawling service operates on a continual basis rather than at scheduled intervals. 
It also promotes robustness, allowing the system to restart and resume from the previous 
point in case of any disruptions. Additionally, Kafka eases the process of extending 
new functionality, meaning the incorporation of new data sources does not disrupt 
the existing processes.

```mermaid
flowchart TB
subgraph Recurrent Batch Process
  append[append_to_crawl_queue.py\n_]
end
subgraph Crawling Service
  oracleP[oracle_puller_daemon.py\nruns continuously\n_]
  awsUpload[aws_upload_daemon_script.sh\nruns continuously\n_]
  tableAWS[table_aws_uploader.py\n_]
end
airflowCron[Airflow/Cron]
airflowCron --> append
append -- Generates --> CTQ((Crawl Task Queue))
CTQ --> oracleP 
oracleP --> TIK((Buffered Data in Kafka))
TIK --> awsUpload
awsUpload --> tableAWS
tableAWS --> AWSB((AWS Bucket))
tableAWS -- Subroutine return --> awsUpload
```




