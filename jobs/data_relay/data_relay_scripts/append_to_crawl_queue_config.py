#!/usr/bin/env python3
"""
append_to_crawl_queue_config.py

This script generates crawl tasks and publishes them to the crawl queue (Kafka service).

Related script: 
* append_to_crawl_queue.py  -  It is for the crawl task of VDS30SEC data, as explained in crontab.txt


Requirements:
* Internal Kafka service must be set up for the crawl queue.
* Kafka Schema Registry must be configured for managing crawl queue metadata.

Purpose:
The main purpose of this script is to generate a list of crawl tasks with consecutive, equal time window intervals. This ensures that the agent responsible for handling the crawl tasks (`oracle_puller_daemon.py`) can reliably retrieve the required database records with minimal impact on the database and efficient memory resource consumption.

Key Concepts:
1. **Time Window Partitioning**:
   - The script takes an argument `day_partition_number`, which specifies the number of partitions in a day.
   - For example, if `day_partition_number=4`, the total time window per partition is 24/4 = 6 hours.

2. **Chunking**:
   - Each partition is further divided into smaller chunks defined by the constant `CHUNK_DURATION_10_MIN`, which represents 10-minute intervals.
   - For a 6-hour partition, there are 6 x 6 = 36 chunks.

3. **Memory Optimization**:
   - Dividing tasks into chunks reduces memory usage for `oracle_puller_daemon.py`. For instance:
     - A 6-hour window typically consumes about 5 GB of memory when using the PyArrow backend for Pandas.
     - Breaking it into 36 chunks reduces the memory consumption to approximately 1/36 of the original (i.e., ~139 MB per chunk).
   - This memory optimization is crucial for stabilizing the `oracle_puller_daemon.py` to function reliably without resource bottlenecks.

"""




# Callouts:
# - **Kafka as a Multipurpose Service**:
#   The Kafka service is used as a multipurpose message queue in Data Relay, 
#   Within this script's scope, we leverage Kafka for task queue management. This script
#   enqueue crawl tasks, and `oracle_puller_daemon.py` dequeue crawl tasks, all
#   through the `oracle_crawl_queue_topic`.
#
# - **Enqueueing Crawl Tasks**:
#   - Crawl tasks are enqueued by publishing events to the Kafka topic `oracle_crawl_queue_topic`.
#   - Each event encapsulates the details of a crawl task, including the database, table, 
#     and time window to be processed.
#
# - **Dequeuing Crawl Tasks**:
#   - The same topic is consumed by downstream components, such as the `oracle_puller_daemon.py` script.
#   - This script dequeues the events and processes the crawl tasks by executing parameterized 
#     SQL operations based on the task details.
#
# - **Lightweight Alternative to Airflow**:
#   - This approach provides a lightweight alternative to using Airflow's dynamic DAGs for 
#     task generation and parameterized SQL operations.
#   - By chunking crawl tasks into manageable units and publishing them as Kafka events, 
#     we achieve efficient task distribution and processing without the overhead of a full 
#     orchestration framework like Airflow.
# 
# 

KAFKA_SERVERS = (
    "PLAINTEXT://svgcmdl03:9092, PLAINTEXT://svgcmdl04:9092, PLAINTEXT://svgcmdl05:9092"
)
KAFKA_SCHEMA_REGISTRY_SERVER = "http://svgcmdl04:8092"
KAFKA_CLIENT_CONFIG = {
    "bootstrap.servers": KAFKA_SERVERS,
    "schema.registry.url": KAFKA_SCHEMA_REGISTRY_SERVER,
}

# Inline FAQ and Context:
# 
# **What's the major challenging part of establishing a Kafka service?**
# - Through previous exploration of both Airflow and Kafka on the same three machines (`svgcmdl03`, 
#   `svgcmdl04`, and `svgcmdl05`), I identified key challenges:
#
#   1. **Kafka Setup**:
#      - Kafka's setup across the three machines is straightforward and uniform, relying on local storage 
#        for data retention. This minimizes dependencies and keeps configuration manageable.
# 
#   2. **Airflow Setup**:
#      - Airflow, on the other hand, requires a dedicated database backend to store metadata such as 
#        DAG definitions, task states, and execution logs.
#      - Unlike Kafka, Airflow cannot operate effectively with just local storage options like SQLite 
#        in a production environment due to concurrency and scalability limitations.
#
# **Why is a dedicated database backend a challenge for Airflow?**
# - Our internal databases are already dedicated to specific purposes:
#   - **MariaDB**: Used exclusively for Tableau.
#   - **Oracle**: Dedicated to PeMS.
# - Setting up a new database service (e.g., PostgreSQL or MySQL) on the three machines for Airflow 
#   requires significant resources, time, and expertise, which is beyond the capacity of a single person.
# - Resorting to SQLite for metadata storage in Airflow is not viable for production use due to 
#   its limitations in handling concurrent access and large-scale workloads.
#
# **Why choose Kafka over Airflow in this case?**
# - Kafka provides a lightweight, efficient queue management solution without requiring a separate 
#   database backend for metadata storage.
# - Its simplicity in setup and operation makes it a practical choice for managing crawl tasks 
#   on our existing infrastructure.
# - By leveraging Kafka's event-driven architecture, we can efficiently enqueue and dequeue tasks 
#   without the overhead of a more complex system like Airflow.
# 
# **How reliable is using self-managed Kafka?**
# - On the three dedicated Linux machines (`svgcmdl03.dot.ca.gov`, `svgcmdl04.dot.ca.gov`, 
#   and `svgcmdl05.dot.ca.gov`), the Kafka service ran without a single incident 
#   from April 2024 through November 2024. This uptime included heavy usage for 
#   crawling tasks and sustained reliability until we decided to reprovision the cluster.   
# 
# 


CRAWL_QUEUE_KAFKA_TOPIC = "oracle_crawl_queue_topic"
# Example Command to Create the Kafka Topic:
# Use the following CLI command to create the `oracle_crawl_queue_topic` on your Kafka cluster:
#
# kafka-topics.sh --create \
#   --bootstrap-server svgcmdl03:9092,svgcmdl04:9092,svgcmdl05:9092 \
#   --replication-factor 3 \
#   --partitions 3 \
#   --topic oracle_crawl_queue_topic
# 
# Callout:
# - **Why use multiple partitions (e.g., 3)?**
#   - A single queue (partition) can be highly inconvenient for regular operations, especially during backfill scenarios.
#   - With multiple partitions, we can designate specific partitions for different tasks:
#     * For example, use partition 0 for the primary queue and partitions 1 or 2 for backfill tasks.
#     * This setup ensures the main queue remains operational and uninterrupted while handling backfill tasks.
#   - Without multiple partitions, the alternative workflow would involve:
#     1. Stopping both the producer and consumer operations on the main queue.
#     2. Backing up the main queue data.
#     3. Clearing the queue and publishing backfill tasks.
#     4. Crawling the backfill tasks.
#     5. Recovering the previous main queue state.
#   - This alternative workflow is cumbersome and operationally inefficient.
#   - Multi-partitioning significantly simplifies operations, improves flexibility, and minimizes disruption to the primary queue.
# 
#
#

CRAWL_QUEUE_TOPIC_SCHEMA = """ 
{
  "name": "PEMS_Oracle_Crawl_Queue",
  "namespace": "com.caltrans.bia",
  "type": "record",
  "doc": "This schema defines the structure of task events used by the PEMS Oracle crawling script `oracle_puller_daemon.py`. It specifies the database, table, and time range to be pulled. The associated Kafka topic is 'oracle_crawl_queue_topic'. The producer for this schema is the `append_to_crawl_queue*.py` scripts, which are executed periodically to publish tasks with updated time windows based on the execution time. The consumer for this schema is the `oracle_puller_daemon.py` script, which processes these events by constructing SQL commands using the specified database, table, and time window as filters."
  "fields": [
    {
      "name": "Database",
      "type": "string",
      "default": "PEMS_DB96",
      "doc": "Internal database name. This field is cruciel for identifing the right crawler / connector for pulling data. Before v5, we have PEMS, and since v5, we have TIM, and since v6, we have PEMS_DB96"
    },
    {
      "name": "Date",
      "type": "string",
      "doc": "Example: 2023-11-20 (yyyy-MM-dd), or 2023-11-20 13:20:00 (yyyy-MM-dd hh:mm:ss)"
    },
    {
      "name": "Table",
      "doc": "Examples: DETECTOR_STATUS, D10.VDS30SEC ",
      "type": "string"
    },
    {
      "name": "Sql",
      "type": "string",
      "default": "SELECT * FROM (SELECT * FROM PEMS.TABLE_PLACEHOLDER) WHERE ROWNUM < END_PLACEHOLDER and ROWNUM >= START_PLACEHOLDER  ",
      "doc": "SELECT * FROM (SELECT * FROM PEMS.TABLE_PLACEHOLDER) WHERE ROWNUM < END_PLACEHOLDER and ROWNUM >= START_PLACEHOLDER  , or SELECT * FROM (SELECT * FROM PEMS.TABLE_PLACEHOLDER WHERE TIME_ID = TO_DATE(\\\"DATE_PLACEHOLDER\\\", \\\"YYYY-MM-DD\\\")) WHERE ROWNUM < END_PLACEHOLDER and ROWNUM >= START_PLACEHOLDER"
    },
    {
      "name": "Doc",
      "type": "string",
      "doc": "helper docs",
      "default": " "
    },
    {
      "name": "Purpose",
      "type": "string",
      "doc": "We have the Purpose identifier. If the query is not for crawling purpose (i.e. analytics), this field helps filter them out",
      "default": "Crawl"
    }
  ]
}
"""

# How to Publish the Schema to the Schema Registry via HTTP:
#
# 1. Save the schema as a JSON file, e.g., `PEMS_Oracle_Crawl_Queue.json`.
#    Example:
#    ```
#    echo "$CRAWL_QUEUE_TOPIC_SCHEMA" > PEMS_Oracle_Crawl_Queue.json
#    ```
#
# 2. Use the following HTTP POST request to publish the schema:
#
#    ```
#    curl -X POST \
#      -H "Content-Type: application/vnd.schemaregistry.v1+json" \
#      --data '{"schema": "$(cat PEMS_Oracle_Crawl_Queue.json)"}' \
#      http://svgcmdl04:8092/subjects/oracle_crawl_queue_topic-value/versions
#    ```
#
# Explanation:
# - `http://svgcmdl04:8092/subjects/oracle_crawl_queue_topic-value/versions`: The URL endpoint to register the schema. Replace `oracle_crawl_queue_topic-value` with the appropriate subject name.
# - `"Content-Type: application/vnd.schemaregistry.v1+json"`: Specifies the request format for the schema registry API.
# - `{"schema": "$(cat PEMS_Oracle_Crawl_Queue.json)"}`: Includes the schema as a JSON string in the request body.
#
# 3. Verify the schema registration by checking the registered schemas:
#    ```
#    curl -X GET http://svgcmdl04:8092/subjects/oracle_crawl_queue_topic-value/versions
#    ```
#
# This process registers the schema with the schema registry and makes it available for producing and consuming messages for the topic `oracle_crawl_queue_topic`.


PARTITION_NUMBER_FOR_THE_CONFIG_TABLE_CRAWL_TASKS = 1

# Following the partitioning strategy outlined earlier:
# - The designated crawling queue for the current script's CONFIG table crawling tasks
#   is partition 1.
# - This ensures that when `oracle_puller_daemon.py` is consuming from the `oracle_crawl_queue_topic`,
#   tasks for CONFIG table crawling are isolated to partition 1, independent of other partitions.
#


# Use loguru for logging.
# loguru produces developer friendly format such as coloring, highlighting etc.
from loguru import logger
import json



logger.add(
    f"/tmp/append_to_crawl_queue.log",
    level="INFO",
    format="{time} {level} {file}:{line} {message}",
    rotation="500 MB",
    retention="10 days",
)

CHUNK_DURATION_10_MIN = 10


def publish_to_crawl_queue(queue_df):
    """
    This function publishes the data in the DataFrame `queue_df` to the crawl queue Kafka topic.
    It first checks if the schema for the topic is up to date in the schema registry, and updates
    it if necessary. Then, it iterates through the rows of the DataFrame and sends each row to
    the Kafka topic. Finally, it flushes the producer and logging handlers.

    :param queue_df:
    :return:
    """

    from confluent_kafka import avro
    from confluent_kafka.avro import AvroProducer

    cproducer = AvroProducer(
        KAFKA_CLIENT_CONFIG,
        default_value_schema=avro.loads(CRAWL_QUEUE_TOPIC_SCHEMA),
    )
    logger.info(f"Head: {queue_df.head()}")
    logger.info("Start publishing:")
    table = None

    # Iterate through the task list and send to crawl queue
    for idx, row in queue_df.iterrows():
        logger.info("New row:" + str(idx))
        logger.info(f"row table: {row['Table']}")
        if "Table" in queue_df.columns and row["Table"]:
            table = row["Table"]
            logger.info(f"table name is {table}")
        else:
            logger.info("warn, no table col")

        cproducer.produce(
            topic="oracle_crawl_queue_topic",
            value={
                "Database": row["Database"],  # Internal database name, e.g., "PEMS_DB96"
                "Date": row["Date"],  # Date in format yyyy-MM-dd or yyyy-MM-dd hh:mm:ss
                "Table": row["Table"],  # Table name, e.g., "DETECTOR_STATUS" or "D10.VDS30SEC"
                "Sql": row.get("Sql", "SELECT * FROM (SELECT * FROM PEMS.TABLE_PLACEHOLDER) WHERE ROWNUM < END_PLACEHOLDER and ROWNUM >= START_PLACEHOLDER"),  
                "Doc": row.get("Doc", " "),  # Optional helper docs, default to a single space
                "Purpose": row.get("Purpose", "Crawl"),  # Purpose identifier, default to "Crawl"
            },
            partition=PARTITION_NUMBER_FOR_THE_CONFIG_TABLE_CRAWL_TASKS
        )

    cproducer.flush()
    logger.info("Publishing to the crawl queue successfully ends")


from datetime import datetime, timedelta
import pandas as pd



def generate_crawl_tasks():
    """
    Generate crawl tasks for crawling the latest CONFIG tables from districts D3 through D12.
    Returns:
    DataFrame: A DataFrame containing the crawl tasks 
    """

    # Prepare data for DataFrame
    data = []

    database = "PEMS"
    
    purpose = "Crawl"
    doc = "This task is intended for polling the CONTROLLER_CONFIG, CONTROLLER_CONFIG_LOG, DETECTOR_CONFIG, DETECTOR_CONFIG_LOG, STATION_CONFIG, and STATION_CONFIG_LOG"
    
    # Per intro from crontab.txt, config table is full table download, hence using SELECT * without a WHERE clause for SQL template.
    sql = f"SELECT * FROM PEMS.TABLE_PLACEHOLDER"
   
    current_time = datetime.now()

    for table in ['CONTROLLER_CONFIG', 'CONTROLLER_CONFIG_LOG', 'DETECTOR_CONFIG', 'DETECTOR_CONFIG_LOG', 'STATION_CONFIG', 'STATION_CONFIG_LOG']:
        sql_template = sql.replace(
            "DATE_PLACEHOLDER", current_time.strftime("%Y-%m-%d %H:%M:%S")
        )
        data.append(
            [
                database,
                current_time.strftime("%Y-%m-%d %H:%M:%S"),
                table,
                sql_template,
                doc,
                purpose,
            ]
        )
    # Create DataFrame
    df = pd.DataFrame(
        data,
        columns=[
            "Schema",
            "Database",
            "Date",
            "Table",
            "Sql",
            "Doc",
            "Purpose",
        ],
    )
    return df




import argparse

# Initialize parser
parser = argparse.ArgumentParser(
    description=(
        "This script generate crawl tasks for the PEMS Oracle database, for CONFIG tables. "
        "It supports three key options for flexibility in task generation and execution: "
        "\n\n"
        "1. **output_csv_file**: "
        "Use this option if you are new or experimenting. It generates a CSV file containing the crawl tasks "
        "without publishing them to the Kafka queue. This allows you to review and validate tasks before committing. "
        "\n\n"
        "2. **crawl_window_start**: "
        "Specify a custom start time for the crawl window. If not provided, the script automatically assigns a "
        "start time of 24 hours ago. This option is useful for backfills or specific time adjustments. "
        "\n\n"
        "3. **append_to_queue**: "
        "When you are fully satisfied with the generated tasks, use this option to enqueue them into the Kafka topic "
        "'oracle_crawl_queue_topic'. This publishes the tasks for downstream processing by the crawling agent."
        "\n\n"
        "Recommended Workflow: "
        "Start with `output_csv_file` for validation, use `crawl_window_start` for custom windows, and finalize with "
        "`append_to_queue` for production execution."
    )
)

# Adding optional argument
parser.add_argument(
    "--append_to_queue",
    action="store_true",
    help="Make the generated crawl tasks published to the crawl service",
)
parser.add_argument(
    "--output_csv_file",
    dest="output_csv_file",
    type=str,
    help="Set the output CSV file for the crawl tasks generated",
)
parser.add_argument(
    "--crawl_window_start",
    type=str,
    default=None,
    help="""A timestamp, if specified, is in string format of yyyy-MM-dd hh:mm:ss) for the start of the timewindow. 
          For example, if `2024-03-01 18:00:00` is specified, then the script will generate crawl tasks with time 
          starting from 2024-03-01 18:00:00 PST.
            If it is unspecified (default to None), then 
             it will be overriden with 24 hours ago
     """,
)

parser.add_argument(
    "--day_partition_number",
    type=int,
    default=1,
    help=""" 
      Default is 1.  
        If 2 is specified, then the time window will be 1/2 of a day
        If 3 is specified, the time window will be 1/3 of a day, so on so forth...
     """,
)


# Read arguments from command line
args = parser.parse_args()
if args.append_to_queue:
    logger.info("The generated csv will be published to the crawl service")
else:
    logger.info("The generated csv will *not* be published to the crawl service")

if __name__ == "__main__":
    df = generate_crawl_tasks()
    if args.output_csv_file:
        df.to_csv(args.output_csv_file, sep="\t")
        logger.info(
            "Inspect the output_csv_file (any append_to_queue request is paused)"
        )
    else:
        logger.info(
            "Did not specify the output_csv_file so that the task list is not written to a csv file"
        )
        if args.append_to_queue:
            logger.info(
                "append_to_queue is specified. Therefore, do the publish (irreversible). "
            )
            publish_to_crawl_queue(df)






import argparse

# Initialize parser
parser = argparse.ArgumentParser()

# Adding optional argument
parser.add_argument(
    "--append_to_queue",
    action="store_true",
    help="Make the generated crawl tasks published to the crawl service",
)
parser.add_argument(
    "--output_csv_file",
    dest="output_csv_file",
    type=str,
    help="Set the output CSV file for the crawl tasks generated",
)
parser.add_argument(
    "--githash",
    type=str,
    required=True,
    help="""Specify the Git hash associated with the generation script. 
    The githash will be included as a data column in all crawled tables for diagnostic purposes.""",
)


# Read arguments from command line
args = parser.parse_args()
if args.append_to_queue:
    logger.info("The generated csv will be published to the crawl service")
else:
    logger.info("The generated csv will *not* be published to the crawl service")

if __name__ == "__main__":
    df = generate_crawl_tasks()
    if args.output_csv_file:
        df.to_csv(args.output_csv_file, sep="\t")
        logger.info(
            "Inspect the output_csv_file (any append_to_queue request is paused)"
        )
    else:
        logger.info(
            "Did not specify the output_csv_file so that the task list is not written to a csv file"
        )
        if args.append_to_queue:
            logger.info(
                "append_to_queue is specified. Therefore, do the publish (irreversible). "
            )
            publish_to_crawl_queue(df)
