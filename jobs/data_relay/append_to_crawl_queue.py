#!/usr/bin/env python3
""" append_to_crawl_queue.py
This job performs the task of generate crawl tasks and publish to crawl queue (kafka service).
Requirements:
* Setup internal kafka service (for crawl queue)
* Setup the kafka schema registry (for metadata of crawl queue)
* Setup the elastic search (for an alternative schema registry)
"""


KAFKA_SERVERS = (
    "PLAINTEXT://svgcmdl03:9092, PLAINTEXT://svgcmdl04:9092, PLAINTEXT://svgcmdl05:9092"
)
KAFKA_SCHEMA_REGISTRY_SERVER = "http://svgcmdl04:8092"
ELASTIC_SEARCH_SERVER = "http://svgcmdl02:9200"
KAFKA_CLIENT_CONFIG = {
    "bootstrap.servers": KAFKA_SERVERS,
    "schema.registry.url": KAFKA_SCHEMA_REGISTRY_SERVER,
}
CRAWL_QUEUE_KAFKA_TOPIC = "oracle_crawl_queue_topic"

CRAWL_QUEUE_TOPIC_CURRENT_SCHEMA = """
{
  "name": "PEMS_Oracle_Crawl_Queue",
  "namespace": "com.caltrans.bia",
  "type": "record",
  "doc": "This is a central schema for crawling service. PEMS_Oracle_Crawl_Queue V6  for oracle_crawl_queue_topic . Usage: the agent on jupyter@svgcmdl01 (D1) will take those events as instructions for crawling PEMS Oracle (and MySql) tables. ",
  "fields": [
    {
      "name": "Schema",
      "type": "string",
      "doc": "Schema version identifier",
      "default": "v6"
    },
    {
      "name": "GitHash",
      "type": "string",
      "doc": "7-character length git hash, associated with the data gen logic"
    },
    {
      "name": "v5_Database",
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
      "name": "v1_Start_Index",
      "type": "int",
      "default": 0,
      "doc": "A chunk's start index - Note that, we employ the chunking strategies to break down big volume data migration into smaller chunks."
    },
    {
      "name": "v1_End_Index",
      "type": "int",
      "default": 30000,
      "doc": "A chunk's end index - See the field v1_Start_Index"
    },
    {
      "name": "v6_Window_Size",
      "type": "int",
      "default": -1,
      "doc": "Starting v6, we have this field. MySQL tends to use Window Size rather than End_Idx. This field default is -1. If larger than -1 value, then we will replace WINDOW_SIZE in the template with this value."
    },
    {
      "name": "Table",
      "doc": "Examples: DETECTOR_STATUS, D10.VDS30SEC ",
      "type": "string"
    },
    {
      "name": "v2_Sql",
      "type": "string",
      "doc" : "no longer needed. (backward compatibility)",
      "default": ""
    },
    {
      "name": "v3_Sql",
      "type": "string",
      "default": "SELECT * FROM (SELECT * FROM PEMS.TABLE_PLACEHOLDER) WHERE ROWNUM < END_PLACEHOLDER and ROWNUM >= START_PLACEHOLDER  ",
      "doc": "SELECT * FROM (SELECT * FROM PEMS.TABLE_PLACEHOLDER) WHERE ROWNUM < END_PLACEHOLDER and ROWNUM >= START_PLACEHOLDER  , or SELECT * FROM (SELECT * FROM PEMS.TABLE_PLACEHOLDER WHERE TIME_ID = TO_DATE(\\\"DATE_PLACEHOLDER\\\", \\\"YYYY-MM-DD\\\")) WHERE ROWNUM < END_PLACEHOLDER and ROWNUM >= START_PLACEHOLDER"
    },
    {
      "name": "v4_Doc",
      "type": "string",
      "doc": "Since v4, we have the helper docs",
      "default": " "
    },
    {
      "name": "v4_Purpose",
      "type": "string",
      "doc": "Since v4, we have the Purpose identifier. If the query is not for crawling purpose (i.e. analytics), this field helps filter them out",
      "default": "Crawl"
    }
  ]
}
"""

# Use loguru for logging.
# loguru produces developer friendly format such as coloring, highlighting etc.
import loguru
from loguru import logger
import json


class loguru_custom_encoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, int):
            return str(obj)
        elif isinstance(obj, str):
            return obj
        elif isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, timedelta):
            return str(obj)
        elif isinstance(obj, loguru._recattrs.RecordFile):
            return json.dumps({"name": obj.name, "path": obj.path})
        elif isinstance(obj, loguru._recattrs.RecordLevel):
            return json.dumps({"name": obj.name, "no": obj.no, "icon": obj.icon})
        elif isinstance(obj, loguru._recattrs.RecordProcess):
            return json.dumps({"id": obj.id, "name": obj.name})
        elif isinstance(obj, loguru._recattrs.RecordThread):
            return json.dumps({"id": obj.id, "name": obj.name})
        elif isinstance(obj, type(None)):
            return None
        else:
            return str(obj)


class log_message_producer:
    def __init__(self, topic, bootstrap_servers):
        from confluent_kafka import Producer
        import socket

        self.producer = Producer(**{"bootstrap.servers": bootstrap_servers})
        self.topic = topic
        self.hostname = socket.gethostname()

    def write(self, message):
        obj = message.record
        obj["hosthame"] = self.hostname
        self.producer.produce(
            self.topic, value=json.dumps(obj, cls=loguru_custom_encoder)
        )

    def flush(self):
        self.producer.flush()


logger.add(
    f"/tmp/append_to_crawl_queue.log",
    level="INFO",
    format="{time} {level} {file}:{line} {message}",
    rotation="500 MB",
    retention="10 days",
)

kafka_log_handler = log_message_producer("dataop_log", KAFKA_SERVERS)
logger.add(
    kafka_log_handler.write,
    level="DEBUG",
    format="{time} {level} {file}:{line} {message}",
)


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
        default_value_schema=avro.loads(CRAWL_QUEUE_TOPIC_CURRENT_SCHEMA),
    )
    logger.info(f"Head: {queue_df.head()}")
    logger.info("Start publishing:")
    table = None

    import requests
    import json

    # If the schema registry fail to have the corresponding schema,
    #  we will use this script's hard-coded schema for backup, and
    url = f"http://svgcmdl04:8092/subjects/{CRAWL_QUEUE_KAFKA_TOPIC}-value/versions/latest"
    response = requests.get(url)
    logger.info(f"returned: {response.json()}")
    target_schema = CRAWL_QUEUE_TOPIC_CURRENT_SCHEMA
    response_obj = response.json()
    if "error_code" in response_obj and response_obj["error_code"] == 40401:
        logger.info(
            f"At this points, topic {CRAWL_QUEUE_KAFKA_TOPIC} does not exist. So, decide to change schema later"
        )
    else:
        latest_schema = response_obj["schema"]
        from deepdiff import DeepDiff

        diff = DeepDiff(json.loads(latest_schema), json.loads(target_schema))

        if diff != {}:
            logger.info(f"Old schema {latest_schema} new schema {target_schema}")
            url = f"{KAFKA_SCHEMA_REGISTRY_SERVER}/subjects/{CRAWL_QUEUE_KAFKA_TOPIC}-value/versions"
            headers = {"Content-Type": "application/vnd.schemaregistry.v1+json"}
            payload = {"schema": target_schema}
            response = requests.post(url, headers=headers, data=json.dumps(payload))
            if response.status_code == 200:
                logger.info(
                    f"Schema {CRAWL_QUEUE_KAFKA_TOPIC} has been successfully registered with schema {target_schema}"
                )
            else:
                logger.info("Error registering the schema: ", response.json())
                raise ValueError("schema update failed")

        else:
            logger.info(f"Schema {CRAWL_QUEUE_KAFKA_TOPIC } is up to date")

        import requests
        import json

        headers = {"Content-Type": "application/json"}

        dockey = f"kafka_{CRAWL_QUEUE_KAFKA_TOPIC}-value-schema"
        data = {"document_key": dockey, "document_value": target_schema}
        es_url = f"{ELASTIC_SEARCH_SERVER}/data_dict/_doc/{dockey}"
        response = requests.post(es_url, headers=headers, data=json.dumps(data))
        logger.info("Registered ES index with response " + response.text)

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
                "Schema": row["Schema"],
                "v5_Database": row["v5_Database"],
                "GitHash": str(row["GitHash"]),
                "Date": row["Date"],
                "v1_Start_Index": int(row["v1_Start_Index"]),
                "v1_End_Index": int(row["v1_End_Index"]),
                "v6_Window_Size": int(row["v6_Window_Size"]),
                "Table": table,
                "v2_Sql": "",
                "v3_Sql": row["v3_Sql"],
                "v4_Doc": row["v4_Doc"],
                "v4_Purpose": row["v4_Purpose"],
            },
        )
    cproducer.flush()
    kafka_log_handler.flush()
    logger.info("Publishing to the crawl queue successfully ends")


from datetime import datetime, timedelta
import pandas as pd


def generate_crawl_tasks():
    """
    Generate crawl tasks for crawling the latest VDS30SEC tables from districts D3 through D12.
    Returns:
    DataFrame: A DataFrame containing the crawl tasks with information including
    Schema, Database, GitHash, Date, Start Index, End Index, Window Size, Table, SQL query, Doc, and Purpose.
    """

    # Prepare data for DataFrame
    districts = ["D3", "D4", "D5", "D6", "D7", "D8", "D10", "D11", "D12"]
    data = []

    # If day_partition_number is 1, then crawl full day 24 hours, devided by 10-min window size, would be 144 chunks.
    number_of_chunks_per_district = 24 * 60 // (10 * args.day_partition_number)
    schema = "v6"
    database = "PEMS_DB96"
    githash = args.githash
    if args.crawl_window_start:
        starting_datetime = args.crawl_window_start
    else:
        starting_datetime = (datetime.now() - timedelta(1)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )

    purpose = "Crawl"
    doc = "This task is intended for crawling the latest VDS30SEC tables from D3 through D12, on the\
 Datetime window that is 10 min long, starting time is DATE_PLACEHOLDER and format example is 2024-03-04 13:00:20"
    sql = f"SELECT * FROM TABLE_PLACEHOLDER WHERE SAMPLE_TIME BETWEEN\
 to_date('DATE_PLACEHOLDER', 'YYYY-MM-DD HH24:MI:SS') AND (to_date('DATE_PLACEHOLDER', 'YYYY-MM-DD HH24:MI:SS') + INTERVAL '10' MINUTE)"
    for d in districts:
        for i in range(number_of_chunks_per_district):
            current_time = datetime.strptime(
                starting_datetime, "%Y-%m-%d %H:%M:%S"
            ) + timedelta(minutes=10 * i)
            next_time = current_time + timedelta(minutes=10)
            table = f"{d}.VDS30SEC"
            sql_template = sql.replace(
                "DATE_PLACEHOLDER", current_time.strftime("%Y-%m-%d %H:%M:%S")
            )
            data.append(
                [
                    schema,
                    database,
                    githash,
                    current_time.strftime("%Y-%m-%d %H:%M:%S"),
                    -1,
                    -1,
                    -1,
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
            "v5_Database",
            "GitHash",
            "Date",
            "v1_Start_Index",
            "v1_End_Index",
            "v6_Window_Size",
            "Table",
            "v3_Sql",
            "v4_Doc",
            "v4_Purpose",
        ],
    )
    return df


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
parser.add_argument(
    "--crawl_window_start",
    type=str,
    default=None,
    help="""A timestamp in string format of yyyy-MM-dd hh:mm:ss for the start of the timewindow.
          For example, if `2024-03-01 18:00:00` is specified, then the script will generate crawl tasks with time
          starting from 2024-03-01 18:00:00 PST.
            If it is unspecified (default to None), then
             it will be overriden with the yesterday's starting time: 00:00:00.001
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

kafka_log_handler.flush()
