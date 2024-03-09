#!/usr/bin/env python3
""" table_aws_uploader.py
This utility retrieves all the remaining contents from a Kafka topic
that hosts temporary data from a database table and uploads it to AWS
under a specific date_time folder in a bucket.
"""

import loguru._recattrs
import subprocess

import requests

from datetime import datetime
from datetime import timedelta

KAFKA_SERVERS = (
    "PLAINTEXT://svgcmdl03:9092, PLAINTEXT://svgcmdl04:9092, PLAINTEXT://svgcmdl05:9092"
)
KAFKA_SCHEMA_REGISTRY_SERVER = "http://svgcmdl04:8092"
ELASTIC_SEARCH_SERVER = "http://svgcmdl02:9200"

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


import argparse
import json

config = {
    "output_path": "/nfsdata/dataop/uploader/tmp",
    "checkpoint_prefix": "_checkpoint",
    "json_draft_for_checkpoint": "draft.json",
}

parser = argparse.ArgumentParser()
parser.add_argument("--topic", help="Topic i.e. D3.VDS30SEC", required=True)
parser.add_argument(
    "--date_time",
    help=" Date time for the output bucket, default to now, "
    "in the format of yyyy-MM-dd HH:hh:ss 2024-03-01 12:21:21",
)
parser.add_argument(
    "--override_checkpoint",
    action="store_true",
    help="If set, this will load the file in yml json_draft_for_checkpoint instead of ES"
    "Cautious, this will override & overwrite the existing stored values for the checkpoint location. ",
)
args = parser.parse_args()

ES_KEY = f"_{config['checkpoint_prefix']}_{args.topic}_offsets.json"

delta_upload_seconds = 20


def get_partition_num_for_initialization(topic_name):
    """
     This function takes a topic name as input and returns
     the number of partitions for that topic.

    Parameters:
    topic_name (str): The name of the topic for which to get the partition count.
    Returns:
    int: The number of partitions for the specified topic.
    """

    from confluent_kafka.admin import AdminClient

    # Configure the AdminClient with your broker endpoints here
    conf = {"bootstrap.servers": KAFKA_SERVERS}
    # Create a new AdminClient
    admin_client = AdminClient(conf)
    # Get metadata for all topics
    metadata = admin_client.list_topics()
    # Get the specific TopicMetadata for your-topic-name
    topic = metadata.topics[topic_name]
    # Now you can access partition count for your topic
    partition_count = len(topic.partitions)
    print(f'The topic "{topic_name}" has {partition_count} partitions.')
    return partition_count


def load_previous_offset_info(topic) -> dict:
    """
    Load the previous offset information from Elasticsearch for the specified topic.

    Args:
    - topic (str): The topic for which offset information needs to be loaded.
    Returns:
    - dict: A dictionary containing the offset information for the specified topic.
    This function connects to the Elasticsearch instance and retrieves the offset
     information for the specified topic.

     If the 'override_checkpoint' flag is set, the existing offset
       information is ignored. Two subcases:
         1. If no previous offset information file is provided, it generates
           default offset information based on the topic's partition number.
         2. If previous offset information is available, then use it.
    Note: This function relies on external configurations like 'json_draft_for_checkpoint',
      'ELASTIC_SEARCH_SERVER', and 'ES_KEY'.
    :param topic:
    :return:
    """

    # connect to the Elasticsearch instance
    if args.override_checkpoint:
        logger.warning("Note: you are going to override (and update) the existing ES")
        # From file to ES
        # From file to downstream
        with open(config["json_draft_for_checkpoint"], "r") as json_file:
            offsets_json = json.load(json_file)
            if "num_partitions" not in offsets_json:
                logger.error(
                    f"Wrong file format: {config['json_draft_for_checkpoint']} {json.dumps(offsets_json)}"
                )
                num_partitions = get_partition_num_for_initialization(topic)
                partitions = {str(i): -1 for i in range(num_partitions)}
                offsets_json = {
                    "topic": topic,
                    "num_partitions": num_partitions,
                    "partition_offsets": partitions,
                }
                logger.error(f"Override with default {json.dumps(offsets_json)}")

            logger.info(
                f"The current offset json content is {json.dumps(offsets_json)}"
            )
            CONFIG_KEY = "json_draft_for_checkpoint"
            logger.info(
                f"And the file is specified as key {CONFIG_KEY} at location {config[CONFIG_KEY]}"
            )
            # Check if the document exists
            headers = {"Content-Type": "application/json"}

            doc_key = ES_KEY
            data = {"document_key": doc_key, "document_value": json.dumps(offsets_json)}
            es_url = f"{ELASTIC_SEARCH_SERVER}/data_dict/_doc/{doc_key}"
            response = requests.get(es_url, headers=headers)

            if "found" in response.json() and not response.json()["found"]:
                requests.post(
                    f"{ELASTIC_SEARCH_SERVER}/data_dict/_doc/{doc_key}",
                    headers=headers,
                    data={"dictionary_value": json.dumps(data)},
                )
            else:
                requests.post(
                    f"{ELASTIC_SEARCH_SERVER}/data_dict/_update/{doc_key}",
                    headers=headers,
                    data={"doc": {"dictionary_value": json.dumps(data)}},
                )

            return offsets_json
    else:
        # From ES to file
        # From ES to downstream
        logger.info(f"Normal path, try to load ES config at {ES_KEY}")
        headers = {"Content-Type": "application/json"}

        doc_key = ES_KEY

        es_url = f"{ELASTIC_SEARCH_SERVER}/data_dict/_doc/{doc_key}"
        response = requests.get(es_url, headers=headers)
        if "found" in response.json() and response.json()["found"]:
            logger.info(
                f"Returned from the found response: {json.dumps(response.json())}"
            )
            offsets_json_str = response.json()["_source"]["dictionary_value"]
            offsets_json = json.loads(offsets_json_str)
            logger.info(
                f"The extracted json from {ES_KEY} is {json.dumps(offsets_json)}"
            )

            if "num_partitions" not in offsets_json:
                logger.error(
                    f"Wrong file format: {config['json_draft_for_checkpoint']} {json.dumps(offsets_json)}"
                )
                num_partitions = get_partition_num_for_initialization(topic)
                partitions = {str(i): -1 for i in range(num_partitions)}
                offsets_json = {
                    "topic": topic,
                    "num_partitions": num_partitions,
                    "partition_offsets": partitions,
                }
                logger.error(f"Override with default {json.dumps(offsets_json)}")

            logger.info(f"Also saved to {config['json_draft_for_checkpoint']}")
            with open(config["json_draft_for_checkpoint"], "w") as json_file:
                json_file.write(json.dumps(offsets_json))
            logger.info(f"The extracted content from ES is {offsets_json}")
            return offsets_json
        else:
            logger.info(" ES no results, so just use default offsets.")
            num_partitions = get_partition_num_for_initialization(topic)
            partitions = {str(i): -1 for i in range(num_partitions)}
            offset_json = {
                "topic": topic,
                "num_partitions": num_partitions,
                "partition_offsets": partitions,
            }
            logger.info(f"Default is {json.dumps(offset_json)}")
            return offset_json


def force_start_at_offset(consumer, offset_info):
    if offset_info is not None:
        # Seek to the latest offset for specific partition
        logger.info(
            f"Seek to the latest offset for specific partition {json.dumps(offset_info)}"
        )
        from confluent_kafka import TopicPartition

        if "num_partitions" not in offset_info:
            raise ValueError("should have ensured the new offset schema by this point")

        topic_parts = []
        logger.debug(f"the offset_info is {str(offset_info)}")
        logger.debug(
            f"the offset_details partition_offsets is {str(offset_info['partition_offsets'])}"
        )
        for k, v in offset_info.get("partition_offsets").items():
            topic_parts.append(TopicPartition(offset_info.get("topic"), int(k), v))
        consumer.assign(topic_parts)
    else:
        raise ValueError("should have eliminated the None branch")


def save_offset_details_as_json(message, offset_details):
    logger.info(
        f"The offset details going to save to ES is {json.dumps(offset_details)}"
    )
    # Check if the message is null, a null message may mean no new messages to consume
    if message is None:
        logger.info("No message received by consumer")
    elif message.error():
        logger.info(f"Error in message consumption: {message.error()}")
    else:
        logger.info(
            f"Successfully consumed message {message.value()} at topic={message.topic()}, partition={message.partition()}, offset: {message.offset()}"
        )

    # Check if the message is null, a null message may mean no new messages to consume

    # Create connection to the Elasticsearch instance
    headers = {"Content-Type": "application/json"}

    dockey = ES_KEY
    logger.info(f"dockey {dockey}")
    es_url = f"{ELASTIC_SEARCH_SERVER}/data_dict/_doc/{dockey}"
    response = requests.get(es_url, headers=headers)

    if "found" in response.json() and not response.json()["found"]:
        response = requests.post(
            f"{ELASTIC_SEARCH_SERVER}/data_dict/_doc/{dockey}",
            headers=headers,
            data=json.dumps({"dictionary_value": json.dumps(offset_details)}),
        )
        logger.info(f"Returned {json.dumps(response.json())}")
    else:
        response = requests.post(
            f"{ELASTIC_SEARCH_SERVER}/data_dict/_update/{dockey}",
            headers=headers,
            data=json.dumps({"doc": {"dictionary_value": json.dumps(offset_details)}}),
        )
        logger.info(f"Returned {json.dumps(response.json())}")

    logger.info("Offset details saved in Elasticsearch.")


def ensure_dir(file_path):
    import os

    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        os.makedirs(directory)


def create_timestamp():
    now = datetime.utcnow()
    return now.strftime("%Y%m%dT%H%M%SZ")


def get_output_path():
    import secrets

    random_string = secrets.token_hex(
        8
    )  # generates a random string of length 16 characters
    timestamp = create_timestamp()
    return f"{config['output_path']}/{args.topic}/{args.topic}_dump_{timestamp}_{random_string}.json"


def pull():
    """
    This function sets up an Avro Kafka Consumer subscribes to the specified topic
    (for example, D3.VDS30SEC)
    and polls messages from it. There are two scenarios:
      1. Before the end: if a new message is found, it appends the message
     into an output file.
      2. Reaching the end: If the polling returns error or None
      (indicating no new message),
     the script will exit.

    Messages are also written into an output file which is uploaded periodically
    if a certain number of seconds have passed. The consumer also resets offset
    details by loading them from a saved JSON file, ensuring continuity of
    message consumption.

    :return:
    """

    from confluent_kafka.avro import AvroConsumer

    output_path = get_output_path()
    ensure_dir(output_path)
    move_to_aws_consumer = AvroConsumer(
        {
            "bootstrap.servers": KAFKA_SERVERS,
            "group.id": "puller_" + args.topic,
            "schema.registry.url": KAFKA_SCHEMA_REGISTRY_SERVER,
            "auto.offset.reset": "earliest",
        }
    )

    move_to_aws_consumer.subscribe([args.topic])

    offset_info = load_previous_offset_info(args.topic)
    force_start_at_offset(move_to_aws_consumer, offset_info)
    new_offset_details = None
    import time

    last_saved_time = time.time()
    while True:
        current_time = time.time()
        try:
            # Attempt to pull a new message from the Kafka topic
            message = move_to_aws_consumer.poll(10)
            if message is None:
                logger.info("Message is None, break")
                time.sleep(0)
                if new_offset_details:
                    upload(output_path, args.date_time, args.topic)
                    output_path = get_output_path()
                    ensure_dir(output_path)
                    save_offset_details_as_json(message, offset_info)
                # Note: this is *the one and only one* exit of the while loop.
                break
            elif message.error():
                logger.info("AvroConsumer error: {}".format(message.error()))
                time.sleep(3)
                continue
            else:
                offset_info["partition_offsets"][message.partition()] = message.offset()
                new_offset_details = True
                with open(output_path, "a") as f:
                    f.write(json.dumps(message.value()) + "\n")
                if (
                    current_time - last_saved_time >= delta_upload_seconds
                ):  # if `delta_upload_seconds` or more seconds have passed
                    last_saved_time = current_time
                    # Perform the action here. For this example, we're just printing a statement
                    logger.info(f"{delta_upload_seconds} seconds have passed, save.")
                    upload(output_path, args.date_time, args.topic)
                    output_path = get_output_path()
                    ensure_dir(output_path)
                    save_offset_details_as_json(message, offset_info)
                else:
                    # Continue to accumulate before sending
                    pass

        except StopIteration:
            # If there's no new message, wait for a bit before retrying
            logger.info("no message")
            time.sleep(5)
            continue
        except json.JSONDecodeError:
            # If we fail to parse the message as JSON, just continue to the next iteration
            logger.info("json error")
            time.sleep(3)
            continue


def upload(output_path, date_string, topic):
    """
     This function uploads files located in an output path (with parquet format) to the AWS
     bucket 'caltrans-pems-dev-us-west-2-raw/db96_export_staging_area'. The directory
     location to upload to within the bucket is determined by the date_string
     and the topic passed to the function.

    Parameters:
    :param output_path: A string denoting the file or directory path to be uploaded.
    :param date_string: A string in the format of "YYYY-MM-DD HH:MM:SS" which sets
      a folder path structure of "year=year/month=month/day=day/".
    :param topic: A string containing information about the relevant topic of the
      data being uploaded. If the topic string contains 'VDS30SEC', it refers to a
      district which sets a folder path structure of "district=district_num/".
    Returns:
      None
    """
    date_folder_substring = ""
    if date_string:
        from datetime import datetime

        date = datetime.strptime(date_string, "%Y-%m-%d %H:%M:%S")
        year = date.year
        month = date.month
        day = date.day
        date_folder_substring = f"year={year}/month={month}/day={day}/"
    district_folder_substring = ""

    if topic and "VDS30SEC" in topic:
        # topic that pass the check would take the form (for example)
        #  D3.VDS30SEC

        district_map = {
            "D3": 3,
            "D4": 4,
            "D5": 5,
            "D6": 6,
            "D7": 7,
            "D8": 8,
            "D9": None,
            "D10": 10,
            "D11": 11,
            "D12": 12,
        }
        district = topic.split(".")[0]
        if district_map[district]:
            district_folder_substring = f"district={district}/"

    final_topic = topic
    if "VDS30SEC" in final_topic:
        final_topic = "VDS30SEC"
        import pandas as pd
        import pyarrow as pa

        df = pd.read_json(output_path)
        table = pa.Table.from_pandas(df)
        parquet_output = f"{output_path}.parquet"
        pa.parquet.write_table(table, parquet_output)
        logger.info(
            f"Json format {output_path} is converted into parquet format {parquet_output}"
        )
        ssh_command = (
            f"/usr/local/bin/aws s3 cp {parquet_output} "
            "s3://caltrans-pems-dev-us-west-2-raw/db96_export_staging_area"
            f"/tables/{final_topic}/{date_folder_substring}{district_folder_substring}"
            f" --ca-bundle /etc/pki/ca-trust/extracted/openssl/ca-bundle.trust.crt"
        )
    else:

        ssh_command = (
            f"/usr/local/bin/aws s3 cp {output_path} "
            "s3://caltrans-pems-dev-us-west-2-raw/db96_export_staging_area"
            f"/tables/{final_topic}/{date_folder_substring}{district_folder_substring}"
            f" --ca-bundle /etc/pki/ca-trust/extracted/openssl/ca-bundle.trust.crt"
        )
    try:
        logger.info("Executing command" + ssh_command)
        completed_process = subprocess.run(ssh_command, check=True, shell=True)
        stdout = completed_process.stdout
        stderr = completed_process.stderr
        logger.debug(stdout)
        logger.debug(stderr)
        logger.info("Command executed successfully")
    except subprocess.CalledProcessError as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    pull()

kafka_log_handler.flush()
