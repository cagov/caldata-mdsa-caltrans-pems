#!/usr/bin/env python3
""" table_aws_uploader.py
This utility retrieves all the remaining contents from a Kafka topic
that hosts temporary data from a database table and uploads it to AWS
under a specific date_time folder in a bucket.
"""
from time import sleep

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
    required=True,
    help=" Date time for the output bucket, a mandatory param, "
    "in the format of (a) `default`, representing today's start 00:00.001, (also can be considered as yesterday's ending 23:59.999) "
    ' or (b) `yyyy-MM-dd HH:mm:ss`, something like "2024-03-01 12:30"',
)
parser.add_argument(
    "--override_checkpoint",
    action="store_true",
    help=f"If set, this will load the file in json_draft_for_checkpoint {config['json_draft_for_checkpoint']} instead of ES "
    "Cautious, this will override & overwrite the existing stored values for the checkpoint location. ",
)
parser.add_argument(
    "--parquet_schema_registry_only",
    action="store_true",
    help=f"If set, the parquet schema, when publishing to AWS, will strictly use schema_registry ",
)
parser.add_argument(
    "--parquet_schema_file_only",
    action="store_true",
    help=f"If set, the parquet schema, when publishing to AWS, will strictly use file ",
)
parser.add_argument(
    "--parquet_schema_logic_only",
    action="store_true",
    help=f"If set, the parquet schema, when publishing to AWS, will strictly use custom logic in this file ",
)
parser.add_argument(
    "--window",
    default=600,
    type=int,
    help=f" The seconds the uploader is spending for finishing each chunking. ",
)
args = parser.parse_args()

ES_KEY = f"_{config['checkpoint_prefix']}_{args.topic}_offsets.json"

delta_upload_seconds = args.window


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


def table_specific_schema_reconstruction(df, topic):
    """
    The function `table_specific_schema_reconstruction` takes a DataFrame `df`
     and a `topic` as input parameters. It reconstructs a specific schema
     based on the `topic` provided.

     For example, if the `topic` contains the string 'VDS30SEC',
        we apply the business knowledge to read the data, which has certain columns
           like "SAMPLE_TIME", "RECV_TIME",
           And expecting the rest columns starting with "Loop" and assigns them a float64 data type.

    The function returns a schema object constructed in the above mentioned example.
           If no branch captures that topic, it simply return None.

    :param df:
    :param topic:
    :return:
        schema_option  return a schema object if there is a defined
               branch that handles the topic, or None if no such business logic
    """
    import pyarrow as pa
    import pandas as pd

    if "VDS30SEC" in topic:
        logger.debug(f"Peek the df for schema construct (before flatten): {df.head()}")
        df["serialized_json"] = df["serialized_json"].apply(
            json.loads
        )  # Convert the column to json
        # Flatten the column
        flattened = pd.json_normalize(df["serialized_json"])
        # Join back to the original dataset
        df = df.drop("serialized_json", axis=1).join(flattened)
        pd.set_option("display.max_columns", None)
        logger.debug(f"Peek the df after flatten: {df.head()}")
        logger.debug(f"Columns after flatten: {','.join(df.columns)}")

        specified_cols = ["SAMPLE_TIME", "RECV_TIME", "meta", "VDS_ID"]
        specified_cols_types = ["timestamp", "timestamp", "string", "int"]
        # Rest of the columns starting with "Loop"
        other_cols = [
            col
            for col in df.columns
            if col not in specified_cols and col.lower().startswith("loop")
        ]

        def extract_digits(string) -> int:
            import re

            return int("0" + "".join(re.findall("\d+", string)))

        lookup = {extract_digits(o): True for o in other_cols}
        for d in range(2, 15):
            if not lookup.get(d):
                other_cols.extend([f"LOOP{d}_VOL", f"LOOP{d}_OCC"])
        other_cols = sorted(other_cols)

        # Ensure no column starts with "Loop" among the first three
        assert all(col.lower().startswith("loop") for col in other_cols)
        # Compile the schema_option
        list_columns_types = []
        list_columns_types.extend(
            [
                (
                    (col, pa.timestamp("ms"))
                    if typ == "timestamp"
                    else (
                        (col, pa.string())
                        if col == "meta"
                        else (col, pa.int32()) if col == "VDS_ID" else None
                    )
                )
                for col, typ in zip(specified_cols, specified_cols_types)
            ]
        )
        list_columns_types.extend(
            [
                (col, pa.float64()) if "_vol" in col.lower() else (col, pa.float64())
                for col in other_cols
            ]
        )

        schema = pa.schema(list_columns_types)
        return schema


def update_schema_with_parquet_schema(schema_option, topic):
    """
    Updates an existing Avro schema in the Kafka schema registry by
     adding a new field to it for parquet schema.
     Note that this parquet schema is for information purpose only,
     and it does not play active role on Avro typing that Schema Registry
     originally purpose.

    In this function, we first fetch the latest version of the Avro schema
    for the relevant topic. To the obtained schema, we add new field named
    'pickle_schema_for_parquet' which
    will hold the Parquet schema
      (Python schema option object in pickled and base64 encoded form).

     The updated Avro schema is then published back to the Kafka schema registry.

    Args:
        schema_option (object): Python object representing Parquet
          schema which will be pickled and base64 encoded.
        topic (str): Kafka topic name for which the schema needs to be updated.

    Raises:
        Exception: Raises an exception if there is any error in the process
         of updating the schema.
    """

    import requests
    import json
    import pickle
    import base64

    # First, get the latest version data
    response = requests.get(
        f"{KAFKA_SCHEMA_REGISTRY_SERVER}/subjects/{topic}-value/versions/latest"
    )
    data = response.json()  # parse JSON response to Python dict
    # Next, get the schema portion
    schema_str = data["schema"]
    schema = json.loads(schema_str)  # parse schema JSON string to dict
    # Then, assign the pickle_shcme field with an input param
    schema["pickle_schema_for_parquet"] = base64.b64encode(
        pickle.dumps(schema_option)
    ).decode("utf-8")
    # Finally, publish to the same topic with the new schema
    new_schema_str = json.dumps(schema)  # convert schema dict back to JSON string
    data = {"schema": new_schema_str}

    post_response = requests.post(
        f"{KAFKA_SCHEMA_REGISTRY_SERVER}/subjects/{topic}-value/versions/",
        data=json.dumps(data),
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
    )
    # Check if the POST request was successful
    if post_response.status_code == 200:
        logger.info("Schema published successfully.")
    else:
        logger.error("Failed to publish Schema.")


def fetch_parquet_schema_from_registry(topic):
    """
    The `fetch_parquet_schema_from_registry` function is a utility function
    used to retrieve the corresponding Parquet schema from the Kafka schema registry
    for a given topic. The function performs an HTTP GET request to the
    schema registry to locate the required schema (a custom field `pickle_schema_for_parquet`)
    based on the specific topic provided as a parameter.

    Parameters:
    - `topic` : required, a string that represents the name of the topic for which the Parquet schema is needed.

    Return:
    - The function will return the Parquet schema related to the topic if it exists in schema registry.
       We will base64 decode the pickle schema for parquet from registry and then unpickle it to extract
       the schema.
    - If the schema doesn't exist, the function return None.

    This function will raise an error if it fails to connect with the schema registry server,
      or if the schema returned by the registry is malformed or not found.

    """

    import requests
    import json
    import pickle
    import base64

    url = f"{KAFKA_SCHEMA_REGISTRY_SERVER}/subjects/{topic}-value/versions/latest"
    response = requests.get(url)
    data = response.json()
    # Task 2: Extract the schema
    schema = json.loads(data["schema"])
    # Task 3: Get the keys
    keys = schema.keys()
    # Task 4: Check for 'pickle_schema_for_parquet' field
    if "pickle_schema_for_parquet" in keys:
        # If exists, encode it in utf-8, base64 decode it and then unpickle it
        pickle_schema_for_parquet = schema["pickle_schema_for_parquet"]
        pickle_schema_for_parquet = pickle_schema_for_parquet.encode("utf-8")
        base64_decoded = base64.b64decode(pickle_schema_for_parquet)
        schema_option = pickle.loads(base64_decoded)
        logger.info(
            f"fetch_parquet_schema_from_registry found the existing schema_option"
        )
        return schema_option
    else:
        logger.info(
            f"fetch_parquet_schema_from_registry does not find any parquet related schema"
        )
        return


def fetch_parquet_schema_from_local_file(topic):
    """Function that fetches the schema_option from a .pkl file"""

    import pickle

    try:
        with open(f"schema_option_{topic}.pkl", "rb") as f:
            schema_option = pickle.load(f)
        return schema_option
    except FileNotFoundError:
        logger.info(f"No file found for topic: {topic}")
        return None


def transform_table_to_schema(topic, df, schema_option):
    """
    This function converts a pandas dataframe to a specified schema.

    Parameters:
        topic (string): Represents the topic that schema_option is associated with.
        df (pandas.DataFrame): Original DataFrame that needs to be converted.
        schema_option (pyarrow.Schema): A PyArrow schema object specifying the format and datatypes
            to comply with.

    Returns:
         df (pandas.DataFrame): Dataframe that has been transformed to comply with
         the specified schema.

    Raises:
        KeyError: In case when column in the actual dataframe does not exist
         in the proposed schema.

    Notes:
        * For the 'VDS30SEC' topic, the 'serialized_json' field is converted into json and then flattened.
        * The provided schema should dictate the output dataframe columns.
        * Each column in the dataframe is type casted according to the data type specified
           in the schema.
    """

    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq

    if not schema_option:
        logger.info(f"No schema specified so return original df")
        return df

    # Depending on `topic`, the custom conversion logic goes here
    if "VDS30SEC" in topic:
        # Align dataframe to this schema
        try:
            df["serialized_json"] = df["serialized_json"].apply(
                json.loads
            )  # Convert the column to json
        except:
            logger.warning(
                f"Tried to json parse the field serialized_json but it turned out already parsed. "
                "This is not necessarily an error, but indicating the data might be slightly varied "
                "in terms of being processed by json.dumps() or not."
            )
        # Flatten the column
        flattened = pd.json_normalize(df["serialized_json"])
        # Join back to the original dataset
        df = df.drop("serialized_json", axis=1).join(flattened)

    # Column wise type casting
    type_lookup = {
        column: dtype for column, dtype in zip(schema_option.names, schema_option.types)
    }
    import numpy as np

    df = df.replace(np.nan, np.nan)

    for column in schema_option.names:
        if column not in df.columns:
            logger.warning(
                f"df.columns: {','.join(df.columns)} does not match schema_option {schema_option} but I assign None"
            )
            import numpy as np

            df[column] = np.nan

    df = df[schema_option.names]
    return df


def read_raw_data_with_schema_parsing_attempt(topic, raw_data_in_json):
    """
    This function reads raw data from a given topic and parses
      the schema of that data. The Schema comes from the difference sources
      with a priority order:
      (1) Check the schema registry, on whether there is an aws_schema or like in the doc.
      (2) Check the local file, whether there is a schema_option.pkl, and if so,
              then overwrite the schema registry and continue to use that schema instead
      (3) Use the local defined custom logic to assign schema,
              if there is such a custom logic, then first overwrite the schema_override.txt
              and the schema_registry, and then continue to use that schema
      (4) If none of the above, then pass along the raw dataframe, with schema_option assigned
              to a None value.

    Parameters:
    topic (str): The topic to read data from. (Corresponding to the Kafka topic)
    raw_data_in_json (str): The raw_data_in_json to read the raw file from.

    Returns:
       dataframe: the parsed data from the raw_data_in_json
       schema_option: if None, this is raw dataframe with no schema
                      Otherwise, this is a schema object that is used
                       for governing the downstream (like parquet output)
    """
    import pandas as pd

    df = pd.read_json(raw_data_in_json, lines=True)
    logger.debug(f"type df {type(df)}")

    schema_option = fetch_parquet_schema_from_registry(topic)

    if (
        schema_option
        and not args.parquet_schema_file_only
        and not args.parquet_schema_logic_only
    ):
        logger.info(
            "Since this step (registry) has non null schema, we overrite local pkl file "
        )
        import pickle

        with open(f"schema_option_{topic}.pkl", "wb") as f:
            pickle.dump(schema_option, f)

        logger.info(
            "Also convert the raw dataframe into typed version, schema from registry"
        )
        df = transform_table_to_schema(topic, df, schema_option)
        return df, schema_option
    elif args.parquet_schema_registry_only:
        logger.error("parquet_schema_registry_only specified but fail to get schema")
        raise ValueError("no schema")
    else:
        logger.info("Fallback to the next available option")
        pass

    schema_option = fetch_parquet_schema_from_local_file(topic)
    if schema_option and not args.parquet_schema_logic_only:
        logger.info(
            "Since this step (local) has non null schema, we overrite registry "
        )
        update_schema_with_parquet_schema(schema_option, topic)

        logger.info(
            "Also convert the raw dataframe into typed version , schema from local file"
        )

        df = transform_table_to_schema(topic, df, schema_option)
        return df, schema_option
    elif args.parquet_schema_file_only:
        logger.error("parquet_schema_file_only specified but fail to get schema")
        raise ValueError("no schema")
    else:
        logger.info("Fallback to the next available option")
        pass

    schema_option = table_specific_schema_reconstruction(df, topic)
    if schema_option:
        logger.info(
            "Since this step (custom logic) has non null schema, "
            "we overrite both registry and local file. Registry is updated with extra checks "
            " to avoid the accident overwrite"
        )

        import pickle

        with open(f"schema_option_{topic}.pkl", "wb") as f:
            pickle.dump(schema_option, f)
        import base64
        import requests
        import json
        from deepdiff import DeepDiff

        # Task 1: Make the request
        url = f"{KAFKA_SCHEMA_REGISTRY_SERVER}/subjects/{topic}-value/versions/latest"
        response = requests.get(url)
        data = response.json()
        # Task 2: Extract the schema
        schema = json.loads(data["schema"])
        # Task 3: Get the keys
        keys = schema.keys()
        # Task 4: Check for 'pickle_schema_for_parquet' field
        if "pickle_schema_for_parquet" in keys:
            # Task 5: If it exists, decode the value
            decoded_value = base64.b64decode(
                schema["pickle_schema_for_parquet"].encode("utf-8")
            )
            decoded_obj = pickle.loads(decoded_value)
            # Task 6: Compare with schema_option
            diff = DeepDiff(decoded_obj, schema_option, ignore_order=True)
            # Task 7: If there's a difference, publish to schema registry
            if diff:
                logger.info(
                    f"Detected difference then we need to update the schema registry with pickle_schema_for_parquet"
                )
                # Publish the new schema to schema registry
                # Assuming you have a function called publish_schema
                update_schema_with_parquet_schema(schema_option, topic)
            else:
                logger.info(
                    "No change on the schema because it is up to date on the parquet format"
                )
        else:
            logger.info(
                f"There is no prior `pickle_schema_for_parquet`. Then we update schema to include this"
            )
            update_schema_with_parquet_schema(schema_option, topic)

        logger.info(
            "Also convert the raw dataframe into typed version, schema from custom logic"
        )

        df = transform_table_to_schema(topic, df, schema_option)
        return df, schema_option
    elif args.parquet_schema_logic_only:
        logger.error("parquet_schema_logic_only specified but fail to get schema")
        raise ValueError("no schema")
    else:
        logger.info("Fallback to the next available option")
        pass

    logger.info(
        "No schema detected, then this function `read_raw_data_with_schema_parsing_attempt` return original data"
    )
    return df, None


def upload(output_path, date_string, topic):
    """
     This function uploads files located in an output path (with parquet format) to the AWS
     bucket 'caltrans-pems-dev-us-west-2-raw/db96_export_staging_area'. The directory
     location to upload to within the bucket is determined by the date_string
     and the topic passed to the function.

    Parameters:
    :param output_path: A string denoting the file or directory path to be uploaded.
    :param date_string: A string in the format of "YYYY-MM-DD HH:MM" which sets
      a folder path structure of "year=year/month=month/day=day/".
    :param topic: A string containing information about the relevant topic of the
      data being uploaded. If the topic string contains 'VDS30SEC', it refers to a
      district which sets a folder path structure of "district=district_num/".
    Returns:
      None
    """
    if date_string == "default":
        import datetime

        def get_today_start():
            # Get the datetime of now
            now = datetime.datetime.now()
            # Get only the date part and manually set the time part to 00:00
            yesterday_midnight = datetime.datetime(now.year, now.month, now.day, 0, 0)
            # Format the datetime into desired format
            formatted_datetime = yesterday_midnight.strftime("%Y-%m-%d %H:%M")
            return formatted_datetime

        actual_date_time = get_today_start()
        logger.debug(f"default to actual_date_time {actual_date_time}")
    else:
        logger.debug(f"assign to actual_date_time {date_string}")
        actual_date_time = date_string
        from datetime import datetime

        def validate_date_format(actual_date_time):
            if isinstance(actual_date_time, str):
                try:
                    datetime.strptime(actual_date_time, "%Y-%m-%d %H:%M")
                    return True
                except ValueError:
                    return False
            else:
                return False

        assert validate_date_format(
            actual_date_time
        ), "Should follow the format like `2023-10-22 11:33`"

    from datetime import datetime

    date = datetime.strptime(actual_date_time, "%Y-%m-%d %H:%M")
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
        import pyarrow.parquet as pq

        logger.debug(f"output_path {output_path}")

        df, schema_option = read_raw_data_with_schema_parsing_attempt(
            topic, output_path
        )
        logger.debug(f"df before the schema-ed pyarrow table conversion {df.head()}")
        table = pa.Table.from_pandas(df, schema=schema_option, preserve_index=False)
        parquet_output = f"{output_path.replace('.json', '')}.parquet"
        pq.write_table(table, parquet_output)
        logger.info(
            f"Json format {output_path} is converted into parquet format {parquet_output}"
        )
        ssh_command = (
            f"/usr/local/bin/aws s3 cp {parquet_output} "
            "s3://caltrans-pems-dev-us-west-2-raw/db96_export_staging_area"
            f"/tables/{final_topic}/{district_folder_substring}{date_folder_substring}"
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


from kazoo.client import KazooClient

ZOOKEEPER_CLUSTER = (
    "svgcmdl03:2181,svgcmdl04:2181,svgcmdl05:2181,svgcmdl01:2181,svgcmdl02:2181"
)


def start_zoo_client_with_retry(zk, retries=3, delay=5):
    """
    This function starts a Zookeeper client with the given number of retries and delay in between retries. It will try to start the client 'retries' number of times, with a 'delay' in seconds between each retry. If the client is successfully started, it will return True. If it fails to start after all retries, it will return False.

    Parameters:
    - zk: Zookeeper client object
    - retries: Number of times to retry starting the client (default is 3)
    - delay: Delay in seconds between retries (default is 5)

    Returns:
    - True if the client is successfully started
    - False if the client fails to start after all retries
    """

    for i in range(retries):
        try:
            zk.start()
            logger.info("Zookeeper client started successfully")
            return True
        except Exception as e:  # adjust this with the right exception type
            logger.error(f"{str(e)}")
            if i < retries - 1:  # if it's not the last retry
                sleep(delay)  # wait for a bit before retrying
                logger.error("continuing retry")
                continue
            else:
                return False
    return False


if __name__ == "__main__":
    zk = KazooClient(hosts=ZOOKEEPER_CLUSTER)
    if not start_zoo_client_with_retry(zk):
        logger.error("Unable to connect after multiple retries")
        kafka_log_handler.flush()
        raise ValueError("cannot start zookeeper")
    from kazoo.recipe.lock import Lock

    lock_path = "/var/coordinator/my_instance_lock_for_aws_uploader"
    logger.info(f"I'm about to enter an section of lock ({lock_path}) ")
    kafka_log_handler.flush()
    lock = Lock(zk, lock_path)
    with lock:
        logger.info(
            f"I'm the only one running table_aws_uploader.py under lock path {lock_path}"
        )
        pull()

kafka_log_handler.flush()
