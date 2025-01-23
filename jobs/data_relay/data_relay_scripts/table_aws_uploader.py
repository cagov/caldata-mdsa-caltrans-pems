#!/usr/bin/env python3

"""
table_aws_uploader.py
This utility retrieves all the remaining contents from a Kafka topic
that hosts temporary data from a database table and uploads it to AWS
under a specific date_time folder in a bucket.
"""
import argparse

# Use loguru for logging.
# loguru produces developer friendly format such as coloring, highlighting etc.
import json
import subprocess
from datetime import datetime, timedelta
from time import sleep
import os

import loguru
import loguru._recattrs
import requests
from kazoo.client import KazooClient
from loguru import logger

KAFKA_SERVERS = (
    "PLAINTEXT://svgcmdl03:9092, PLAINTEXT://svgcmdl04:9092, PLAINTEXT://svgcmdl05:9092"
)
KAFKA_SCHEMA_REGISTRY_SERVER = "http://svgcmdl04:8092"
ELASTIC_SEARCH_SERVER = "https://svgcmdl02.dot.ca.gov:9200"




logger.add(
    "/tmp/table_aws_uploader.log",
    level="DEBUG",
    format="{time} {level} {file}:{line} {message}",
    rotation="500 MB",
    retention="10 days",
)



config = {
    "output_path": "/nfsdata/dataop/uploader/tmp",
    "checkpoint_prefix": "_checkpoint",
    "json_draft_for_checkpoint": "draft.json",
}

parser = argparse.ArgumentParser()
parser.add_argument("--topic", help="Topic i.e. D3.VDS30SEC", required=True)
parser.add_argument("--debug", \
                    help="Debugger config string, if specified, should take the form like key=value,key1=value1. Example key includes `signature` ")
parser.add_argument(
    "--limit_rows",
    type=int,
    help=" if specified, will regulate the maximal size of rows the whole script can handle before exiting. ",
)
parser.add_argument(
    "--date_time",
    required=True,
    help=" Date time for the output bucket, a mandatory param, "
    "in the format of (a) `default`, representing today's start 00:00.001, (also can be considered as yesterday's ending 23:59.999) "
    ' or (b) `yyyy-MM-dd HH:mm:ss`, something like "2024-03-01 12:30"',
)
parser.add_argument(
        "--snow_env", 
        help= " Snowflake environment. Values can be DEV or PROD , default is DEV", default="DEV")
parser.add_argument(
    "--override_checkpoint",
    action="store_true",
    help=f"If set, this will load the file in json_draft_for_checkpoint {config["json_draft_for_checkpoint"]} instead of ES "
    "Cautious, this will override & overwrite the existing stored values for the checkpoint location. ",
)
parser.add_argument(
    "--parquet_schema_registry_only",
    action="store_true",
    help="If set, the parquet schema, when publishing to AWS, will strictly use schema_registry ",
)
parser.add_argument(
    "--parquet_schema_file_only",
    action="store_true",
    help="If set, the parquet schema, when publishing to AWS, will strictly use file ",
)
parser.add_argument(
    "--parquet_schema_logic_only",
    action="store_true",
    help="If set, the parquet schema, when publishing to AWS, will strictly use custom logic in this file ",
)
parser.add_argument(
    "--window",
    default=600,
    type=int,
    help=" The seconds the uploader is spending for finishing each chunking. Default to 600 sec. ",
)
args = parser.parse_args()

ES_KEY = f"_{config["checkpoint_prefix"]}_{args.topic}_offsets.json"

delta_upload_seconds = args.window


def get_partition_num_for_initialization(topic_name):
    """
     Takes a topic name as input and returns
     the number of partitions for that topic.

    Parameters
    ----------
    topic_name (str): The name of the topic for which to get the partition count.

    Returns
    -------
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
    print(f"The topic "{topic_name}" has {partition_count} partitions.")
    return partition_count


def load_previous_offset_info(topic) -> dict:
    """
    Load the previous offset information from Elasticsearch for the specified topic.

    Args:
    ----
    - topic (str): The topic for which offset information needs to be loaded.

    Returns:
    -------
    - dict: A dictionary containing the offset information for the specified topic.
    This function connects to the Elasticsearch instance and retrieves the offset
     information for the specified topic.

     If the "override_checkpoint" flag is set, the existing offset
       information is ignored. Two subcases:
         1. If no previous offset information file is provided, it generates
           default offset information based on the topic"s partition number.
         2. If previous offset information is available, then use it.
    Note: This function relies on external configurations like "json_draft_for_checkpoint",
      "ELASTIC_SEARCH_SERVER", and "ES_KEY".
    :param topic:
    :return:

    """
    # connect to the Elasticsearch instance
    if args.override_checkpoint:
        logger.warning("Note: you are going to override (and update) the existing ES")
        # From file to ES
        # From file to downstream
        with open(config["json_draft_for_checkpoint"]) as json_file:
            offsets_json = json.load(json_file)
            if "num_partitions" not in offsets_json:
                logger.error(
                    f"Wrong file format: {config["json_draft_for_checkpoint"]} {json.dumps(offsets_json)}"
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
            response = requests.get(es_url, headers=headers, auth=("XXX","XXXX"), verify="/nfsdata/dataop/ca_ca.crt")

            if "found" in response.json() and not response.json()["found"]:
                requests.post(
                    f"{ELASTIC_SEARCH_SERVER}/data_dict/_doc/{doc_key}",
                    headers=headers,
                    data={"dictionary_value": json.dumps(data)},
                    auth=("XXXXX", "XXXXX"),
                    verify="/nfsdata/dataop/ca_ca.crt"
                )
            else:
                requests.post(
                    f"{ELASTIC_SEARCH_SERVER}/data_dict/_update/{doc_key}",
                    headers=headers,
                    data={"doc": {"dictionary_value": json.dumps(data)}},
                    auth=("XXXXX", "XXXXX"),
                    verify="/nfsdata/dataop/ca_ca.crt"
                )

            return offsets_json
    else:
        # From ES to file
        # From ES to downstream
        logger.info(f"Normal path, try to load ES config at {ES_KEY}")
        headers = {"Content-Type": "application/json"}

        doc_key = ES_KEY

        es_url = f"{ELASTIC_SEARCH_SERVER}/data_dict/_doc/{doc_key}"
        response = requests.get(es_url, headers=headers, auth=("XXXXX","XXXXX"),verify="/nfsdata/dataop/ca_ca.crt")
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
                    f"Wrong file format: {config["json_draft_for_checkpoint"]} {json.dumps(offsets_json)}"
                )
                num_partitions = get_partition_num_for_initialization(topic)
                partitions = {str(i): -1 for i in range(num_partitions)}
                offsets_json = {
                    "topic": topic,
                    "num_partitions": num_partitions,
                    "partition_offsets": partitions,
                }
                logger.error(f"Override with default {json.dumps(offsets_json)}")

            logger.info(f"Also saved to {config["json_draft_for_checkpoint"]}")
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
        logger.debug(f"the offset_info is {offset_info!s}")
        logger.debug(
            f"the offset_details partition_offsets is {offset_info["partition_offsets"]!s}"
        )
        for k, v in offset_info.get("partition_offsets").items():
            topic_parts.append(TopicPartition(offset_info.get("topic"), int(k), v + 1))
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
    response = requests.get(es_url, headers=headers, auth=("XXX","XXXXX"),verify="/nfsdata/dataop/ca_ca.crt")

    if "found" in response.json() and not response.json()["found"]:
        response = requests.post(
            f"{ELASTIC_SEARCH_SERVER}/data_dict/_doc/{dockey}",
            headers=headers,
            data=json.dumps({"dictionary_value": json.dumps(offset_details)}),
            auth=("XXX","XXXXXX"),
            verify="/nfsdata/dataop/ca_ca.crt"
        )
        logger.info(f"Returned {json.dumps(response.json())}")
    else:
        response = requests.post(
            f"{ELASTIC_SEARCH_SERVER}/data_dict/_update/{dockey}",
            headers=headers,
            data=json.dumps({"doc": {"dictionary_value": json.dumps(offset_details)}}),
            auth=("XXX", "XXXXX"),
            verify="/nfsdata/dataop/ca_ca.crt"
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

    random_string = secrets.token_hex(8)  # generates a random string of length 16 characters
    timestamp = create_timestamp()
    return f"{config["output_path"]}/{args.topic}/{args.topic}_dump_{timestamp}_{random_string}.json"


def pull():
    """
    Sets up an Avro Kafka Consumer subscribes to the specified topic
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
    if args.topic in ["CONTROLLER_CONFIG", 
                      "CONTROLLER_CONFIG_LOG", 
                      "DETECTOR_CONFIG", 
                      "DETECTOR_CONFIG_LOG", 
                      "STATION_CONFIG", 
                      "STATION_CONFIG_LOG", ]:
        logger.info(f"Topic {args.topic} will directly upload from static files rather than pull kafka.")
        upload("","2012-11-11 11:11", args.topic)
        return
    logger.info("Topic {args.topic} will be pulled from kafka and upload.")

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
    if False:
        force_start_at_offset(move_to_aws_consumer, offset_info)
    else:
        logger.info("Force start at offset skipped.")
    new_offset_details = None
    import time

    last_saved_time = time.time()
    messages_in_batch = 0
    buffer = []
    while True:
        current_time = time.time()
        try:
            # Attempt to pull a new message from the Kafka topic
            message = move_to_aws_consumer.poll(10)
            messages_in_batch = messages_in_batch + 1
            if message is None:
                logger.info("Message is None, break")
                time.sleep(0)
                if new_offset_details:
                    with open(output_path, "a") as f:
                        for row in buffer:
                            f.write(json.dumps(row) + "\n")
                    buffer.clear()
                    upload(output_path, args.date_time, args.topic)
                    output_path = get_output_path()
                    ensure_dir(output_path)
                    save_offset_details_as_json(message, offset_info)
                # Note: this is *the one and only one* exit of the while loop.
                break
            elif message.error():
                logger.info(f"AvroConsumer error: {message.error()}")
                time.sleep(3)
                continue
            else:

                offset_info["partition_offsets"][message.partition()] = message.offset()
                new_offset_details = True
                if (
                    current_time - last_saved_time >= delta_upload_seconds \
                        or (args.limit_rows and messages_in_batch > int(args.limit_rows))
                ):  # if `delta_upload_seconds` or more seconds have passed
                    buffer.append(message.value())
                    # dump the remaining backlog buffer to the file
                    with open(output_path, "a") as f:
                        for row in buffer:
                            f.write(json.dumps(row) + "\n")
                    buffer.clear()
                    messages_in_batch = len(buffer)
                    last_saved_time = current_time
                    # Perform the action here. For this example, we"re just printing a statement
                    logger.info(f"{delta_upload_seconds} seconds have passed, save.")
                    upload(output_path, args.date_time, args.topic)
                    output_path = get_output_path()
                    ensure_dir(output_path)
                    save_offset_details_as_json(message, offset_info)
                else:
                    buffer.append(message.value())

                    # Access to the IO only reaches certain batch
                    if len(buffer) > 1000:
                        with open(output_path, "a") as f:
                            for row in buffer:
                                f.write(json.dumps(row) + "\n")
                        buffer.clear()
                    # Continue to accumulate before sending
                    pass

        except StopIteration:
            # If there"s no new message, wait for a bit before retrying
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
    Takes a DataFrame `df`
     and a `topic` as input parameters, reconstructs a specific schema
     based on the `topic` provided.

     For example, if the `topic` contains the string "VDS30SEC",
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
    import pandas as pd
    import pyarrow as pa

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
        logger.debug(f"Columns after flatten: {",".join(df.columns)}")

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

            return int("0" + "".join(re.findall(r"\d+", string)))

        lookup = {extract_digits(o): True for o in other_cols}
        for d in range(1, 15):
            if not lookup.get(d):
                other_cols.extend([f"LOOP{d}_VOL", f"LOOP{d}_OCC", f"LOOP{d}_SPD"])
        other_cols = sorted(set(other_cols))
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
                for col, typ in zip(specified_cols, specified_cols_types, strict=False)
            ]
        )
        list_columns_types.extend(
            [
                (col, pa.float64()) if "_vol" in col.lower() else (col, pa.float64())
                for col in other_cols
            ]
        )

        return pa.schema(list_columns_types)

    return None


def update_schema_with_parquet_schema(schema_option, topic):
    """
    Updates an existing Avro schema in the Kafka schema registry by
     adding a new field to it for parquet schema.

    Note that this parquet schema is for information purpose only,
     and it does not play active role on Avro typing that Schema Registry
     originally purpose.

    In this function, we first fetch the latest version of the Avro schema
    for the relevant topic. To the obtained schema, we add new field named
    "pickle_schema_for_parquet" which
    will hold the Parquet schema
      (Python schema option object in pickled and base64 encoded form).

     The updated Avro schema is then published back to the Kafka schema registry.

    Args:
    ----
        schema_option (object): Python object representing Parquet
          schema which will be pickled and base64 encoded.
        topic (str): Kafka topic name for which the schema needs to be updated.

    Raises:
    ------
        Exception: Raises an exception if there is any error in the process
         of updating the schema.

    """
    import base64
    import json
    import pickle

    import requests

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
    Retrieve the corresponding Parquet schema from the Kafka schema registry
    for a given topic. The function performs an HTTP GET request to the
    schema registry to locate the required schema (a custom field `pickle_schema_for_parquet`)
    based on the specific topic provided as a parameter.

    Parameters
    ----------
    - `topic` : required, a string that represents the name of the topic for which the Parquet schema is needed.

    Return:
    - The function will return the Parquet schema related to the topic if it exists in schema registry.
       We will base64 decode the pickle schema for parquet from registry and then unpickle it to extract
       the schema.
    - If the schema doesn"t exist, the function return None.

    This function will raise an error if it fails to connect with the schema registry server,
      or if the schema returned by the registry is malformed or not found.

    """
    import base64
    import json
    import pickle

    import requests

    url = f"{KAFKA_SCHEMA_REGISTRY_SERVER}/subjects/{topic}-value/versions/latest"
    response = requests.get(url)
    data = response.json()
    # Task 2: Extract the schema
    schema = json.loads(data["schema"])
    # Task 3: Get the keys
    keys = schema.keys()
    # Task 4: Check for "pickle_schema_for_parquet" field
    if "pickle_schema_for_parquet" in keys:
        # If exists, encode it in utf-8, base64 decode it and then unpickle it
        pickle_schema_for_parquet = schema["pickle_schema_for_parquet"]
        pickle_schema_for_parquet = pickle_schema_for_parquet.encode("utf-8")
        base64_decoded = base64.b64decode(pickle_schema_for_parquet)
        schema_option = pickle.loads(base64_decoded)
        logger.info(
            "fetch_parquet_schema_from_registry found the existing schema_option"
        )
        return schema_option
    else:
        logger.info(
            "fetch_parquet_schema_from_registry does not find any parquet related schema"
        )
        return None


def fetch_parquet_schema_from_local_file(topic):
    """Fetches the schema_option from a .pkl file."""
    import pickle

    try:
        with open(f"schema_option_{topic}.pkl", "rb") as f:
            return pickle.load(f)
    except FileNotFoundError:
        logger.info(f"No file found for topic: {topic}")
        return None


def transform_table_to_schema(topic, df, schema_option):
    """
    Converts a pandas dataframe to a specified schema,
      and split into dates specified in the meta.Date field.

    Parameters
    ----------
        topic (string): Represents the topic that schema_option is associated with.
        df (pandas.DataFrame): Original DataFrame that needs to be converted.
        schema_option (pyarrow.Schema): A PyArrow schema object specifying the format and datatypes
            to comply with.

    Returns
    -------
         df_dates (list[tuple(pandas.DataFrame, datetime)]): Dataframe that has been transformed to comply with
         the specified schema.

    Raises
    ------
        KeyError: In case when column in the actual dataframe does not exist
         in the proposed schema.

    Notes
    -----
        * For the "VDS30SEC" topic, the "serialized_json" field is converted into json and then flattened.
        * The provided schema should dictate the output dataframe columns.
        * Each column in the dataframe is type casted according to the data type specified
           in the schema.

    """
    import pandas as pd

    if not schema_option:
        logger.info("No schema specified so return original df")
        return df

    # Depending on `topic`, the custom conversion logic goes here
    # Align dataframe to this schema
    try:
        df["serialized_json"] = df["serialized_json"].apply(
            json.loads
        )  # Convert the column to json
    except Exception:
        logger.trace(
            "Tried to json parse the field serialized_json but it turned out already parsed. "
            "This is not necessarily an error, but indicating the data might be slightly varied "
            "in terms of being processed by json.dumps() or not."
        )
    # Flatten the column
    flattened = pd.json_normalize(df["serialized_json"])
    pd.set_option("display.max_columns", None)
    logger.debug(f"flattened: {flattened.head()}")
    # Join back to the original dataset
    df = flattened
    # Flatten the column


    logger.info("DF Successfully converted to numpy_nullable as memory backend")

    import numpy as np


    for column in schema_option.names:
        if column not in df.columns:
            logger.trace(
                f"column {column} not in df.columns: {",".join(df.columns)} does not match schema_option {schema_option} but I assign None"
            )
            import numpy as np

            df[column] = np.nan

    df["meta"] = ""
    df["SAMPLE_TIME"] = pd.to_datetime(df["SAMPLE_TIME"], unit="ms")
    df["Date_internal"] = df["SAMPLE_TIME"].dt.date
    df = df[[*schema_option.names, "Date_internal"]]


    logger.info("Again, successfully converted to pyarrow as memory backend")

    if "VDS30SEC" in topic:
        df["SAMPLE_TIME"] = pd.to_datetime(df["SAMPLE_TIME"], unit="ms")
        df["RECV_TIME"] = pd.to_datetime(df["RECV_TIME"], unit="ms")
        for col in df.columns:
            if col.startswith("LOOP"):
                df[col] = df[col].astype(float)
            if col.endswith("VOL"):
                df[col] = df[col].round().astype("Int64")


        # Initialize an empty list to store the tuples
        df_segments_by_date = []

        # Group the DataFrame by "Date_internal" and iterate over the groups
        for date, group in df.groupby("Date_internal"):
            logger.debug(f"date {date} types: {group.dtypes} dataframe: {group.head()}")
            # Append a tuple of the group (df_segment) and the date to the list
            df_segments_by_date.append((group.drop("Date_internal", axis=1), date))

        return df_segments_by_date

    else:
        logger.info("return df without a date indicator")
        return [(df.drop("Date_internal", axis=1), None)]


def split_json_file(raw_data_path, output_dir, lines_per_file=100000):
    """Split a large JSON file with newline-separated JSON objects into multiple smaller files."""

    from itertools import islice
    from pathlib import Path
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    with open(raw_data_path, "r") as f:
        for file_count, chunk in enumerate(iter(lambda: list(islice(f, lines_per_file)), [])):
            output_file = output_dir / f"split_{file_count}.json"
            with open(output_file, "w") as split_file:
                split_file.writelines(chunk)


def pd_read_json_safe(raw_json_data_path):
    """
    Read a JSON file into a DataFrame, without running into the memory reservation failure. 
    
    It splitted the input json into smaller ones, each parsed into a dataframe, then concatenate those into the final data frame. 
    
    Parameters
    ----------
    raw_json_data_path(str): The json file to read from.

    Returns
    ----------
       df : pandas.DataFrame the dataframe read from the input json, using pyarrow as backend for performance.
    """
    import pandas as pd
    
    import tempfile
    with tempfile.TemporaryDirectory() as temp_dir:
        # Split large JSON file into smaller chunks
        split_json_file(raw_json_data_path, temp_dir)

        # Read each split JSON into a DataFrame and append to a list
        dfs = []
        for file in os.listdir(temp_dir):
            file_path = os.path.join(temp_dir, file)
            try:
                df_chunk = pd.read_json(file_path, lines=True, dtype_backend="pyarrow")
                dfs.append(df_chunk)
            except ValueError as e:
                logger.error(f"Error loading {file_path}: {str(e)}")

        # Concatenate all DataFrames
        df = pd.concat(dfs, ignore_index=True)
        return df


def read_raw_data_with_schema_parsing_attempt(topic, raw_data_path_in_json):
    """
    Reads raw data from a given topic and parses
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

    Parameters
    ----------
    topic (str): The topic to read data from. (Corresponding to the Kafka topic)
    raw_data_in_json (str): The raw_data_in_json to read the raw file from.

    Returns
    -------
       dataframe_dates: (list[tuple(pandas.DataFrame, datetime)]) the parsed data from the
                        raw_data_in_json,
                        splitted into multiple dataframes each corresponding to a date,
                        (the date is from the row"s meta field"s Date (dt) field)
       schema_option: if None, this is raw dataframe with no schema
                      Otherwise, this is a schema object that is used
                       for governing the downstream (like parquet output)

    """
    import pandas as pd

    try:
        df = pd_read_json_safe(raw_data_path_in_json)
    except ValueError as e:
        logger.error(f"Error message (cannot reserve memory): {str(e)}")
        df = None
    logger.debug(f"df.dtypes {df.dtypes}")
    logger.debug(f"df.memory_usage {df.memory_usage()}")


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
        df_dates = transform_table_to_schema(topic, df, schema_option)
        logger.debug(f"df_dates: {df_dates}")
        return df_dates, schema_option
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

        df_dates = transform_table_to_schema(topic, df, schema_option)
        return df_dates, schema_option
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
        import json

        import requests
        from deepdiff import DeepDiff

        # Task 1: Make the request
        url = f"{KAFKA_SCHEMA_REGISTRY_SERVER}/subjects/{topic}-value/versions/latest"
        response = requests.get(url)
        data = response.json()
        # Task 2: Extract the schema
        schema = json.loads(data["schema"])
        # Task 3: Get the keys
        keys = schema.keys()
        # Task 4: Check for "pickle_schema_for_parquet" field
        if "pickle_schema_for_parquet" in keys:
            # Task 5: If it exists, decode the value
            decoded_value = base64.b64decode(
                schema["pickle_schema_for_parquet"].encode("utf-8")
            )
            decoded_obj = pickle.loads(decoded_value)
            # Task 6: Compare with schema_option
            diff = DeepDiff(decoded_obj, schema_option, ignore_order=True)
            # Task 7: If there"s a difference, publish to schema registry
            if diff:
                logger.info(
                    "Detected difference then we need to update the schema registry with pickle_schema_for_parquet"
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
                "There is no prior `pickle_schema_for_parquet`. Then we update schema to include this"
            )
            update_schema_with_parquet_schema(schema_option, topic)

        logger.info(
            "Also convert the raw dataframe into typed version, schema from custom logic"
        )

        df_dates = transform_table_to_schema(topic, df, schema_option)
        return df_dates, schema_option
    elif args.parquet_schema_logic_only:
        logger.error("parquet_schema_logic_only specified but fail to get schema")
        raise ValueError("no schema")
    else:
        logger.info("Fallback to the next available option")
        pass

    logger.info(
        "No schema detected, then this function `read_raw_data_with_schema_parsing_attempt` return original data"
    )
    return [(df, None)], None


def upload(output_path, date_string, topic):
    """
     Uploads files located in an output path (with parquet format) to the AWS
     bucket "caltrans-pems-dev-us-west-2-raw/db96_export_staging_area". The directory
     location to upload to within the bucket is determined by the Date.
      By default the date is from the date_string passed in,
      However, for custom logic, date maybe overriden.
      For example, in VDS30SEC, the Date is actually coming from the meta field dt field
        of each row. (Therefore, for VDS30SEC, there may be multiple files uploaded due
        to the fact that there may be different meta dates (dt) among rows).

    Parameters
    ----------
    :param output_path: A string denoting the file or directory path to be uploaded.
    :param date_string: A string in the format of "YYYY-MM-DD HH:MM" which sets
      a folder path structure of "year=year/month=month/day=day/".
    :param topic: A string containing information about the relevant topic of the
      data being uploaded. If the topic string contains "VDS30SEC", it refers to a
      district which sets a folder path structure of "district=district_num/".

    Returns
    -------
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
            return yesterday_midnight.strftime("%Y-%m-%d %H:%M")

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

        logger.debug(f"output_path {output_path}")

        df_dates, schema_option = read_raw_data_with_schema_parsing_attempt(
            topic, output_path
        )
        logger.debug(
            f"df before the schema-ed pyarrow table conversion {df_dates[0][0].head()}"
        )

        logger.info(f"There are {len(df_dates)} dates in the output results")
        for df, date in df_dates:
            if date:
                year = date.year
                month = date.month
                day = date.day
                date_folder_substring = f"year={year}/month={month}/day={day}/"

            logger.info(f"Parquet file row size: {len(df)} at Year({year}) Month({month}) Day ({day}) Date({date}) District({topic.split(".")[0]})")


            # table = pa.Table.from_pandas(df, schema=schema_option, preserve_index=False)
            parquet_output = f"{output_path.replace(".json", "")}.parquet"
            df.to_parquet(parquet_output)
            logger.info(
                f"Json format {output_path} is converted into parquet format {parquet_output}"
            )
            logger.info(
                f"Current date is {date} and df first row"s date is {df["SAMPLE_TIME"].iloc[0]} and df last row"s date is {df["SAMPLE_TIME"].iloc[-1]} and total size is {len(df)}"
            )

            shell_cp = (
                f"/usr/local/bin/aws s3 cp {parquet_output} "
                "s3://caltrans-pems-dev-us-west-2-raw/db96_export_staging_area"
                f"/tables/{final_topic}/{district_folder_substring}{date_folder_substring}"
                f" --ca-bundle /etc/pki/ca-trust/extracted/openssl/ca-bundle.trust.crt"
            )
            try:
                logger.info("Executing command" + shell_cp)
                completed_process = subprocess.run(shell_cp, check=True, shell=True)
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

            shell_cp = (
                f"/usr/local/bin/aws s3 cp --profile prod {parquet_output} "
                "s3://caltrans-pems-prd-us-west-2-raw/db96_export_staging_area"
                f"/tables/{final_topic}/{district_folder_substring}{date_folder_substring}"
                f" --ca-bundle /etc/pki/ca-trust/extracted/openssl/ca-bundle.trust.crt"
            )
            try:
                logger.info("Executing command" + shell_cp)
                completed_process = subprocess.run(shell_cp, check=True, shell=True)
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
    elif final_topic in ["CONTROLLER_CONFIG", "CONTROLLER_CONFIG_LOG", "DETECTOR_CONFIG", \
                         "DETECTOR_CONFIG_LOG", "STATION_CONFIG", "STATION_CONFIG_LOG", ]:
        logger.info(f"Final topic is {final_topic}")
        if args.snow_env == "DEV":
            logger.info("Chosen the DEV environment for the SNOWFLAKE destination")
            SNOWFLAKE_PASSWORD = "XXXXXXXXX"
            ENVIRONMENT = "RAW_DEV"
            USERNAME = "MWAA_SVC_USER_DEV"
        else:
            logger.info("Chosen the PROD environment for the SNOWFLAKE destination")
            SNOWFLAKE_PASSWORD = "XXXXXXXXX"
            ENVIRONMENT = "RAW_PRD"
            USERNAME = "MWAA_SVC_USER_PRD"

        SQL_FILE = f"/nfsdata/dataop/uploader/snowsql/snowsql_load_{final_topic}.sql"

        shell_cp = f"SNOWSQL_PWD=\"{SNOWFLAKE_PASSWORD}\" REQUESTS_CA_BUNDLE=/etc/pki/ca-trust/extracted/openssl/ca-bundle.trust.crt snowsql -a NGB13288 -u {USERNAME} -o insecure_mode=True -f {SQL_FILE} -d {ENVIRONMENT}"
        try:
            logger.info("Executing command " + shell_cp)
            completed_process = subprocess.run(shell_cp, check=True, shell=True)
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
        logger.warning("Now, I need to further execute the uploading script")
    elif final_topic in ["HOV.DETECTOR_STATUS", "HOV.station_hour_summary"]:
        logger.debug("final topic in HOV")
        logger.debug(f"output_path {output_path}")

        df_dates, schema_option = read_raw_data_with_schema_parsing_attempt(
            topic, output_path
        )
        logger.debug(
            f"df before the schema-ed pyarrow table conversion {df_dates[0][0].head()}"
        )

        logger.info(f"There are {len(df_dates)} dates in the output results")
        df, date = df_dates[0]

        logger.info(f"The number of entries in the df is {len(df)}")

        if len(df) < 2:
            logger.warning(f"This is almost empty batch. It typically indicates the cron job schedule messed up. Therefore, do not execute the uploading.")
        else:
            logger.info(
                f"This batch is reasonablly large, which indicates a full batch. Then, execute the uploading to Snowflake operation. ")
            # table = pa.Table.from_pandas(df, schema=schema_option, preserve_index=False)
            parquet_output = f"{output_path.replace(".json", "")}.parquet"
            df.to_parquet(parquet_output)
            logger.info(
                f"Json format {output_path} is converted into parquet format {parquet_output}"
            )

            shell_cp = (
                f"cp {parquet_output} "
                f"/nfsdata/dataop/uploader/tmp/{final_topic}/{final_topic}_dump_static.parquet"
            )
            try:
                logger.info("Executing command" + shell_cp)
                completed_process = subprocess.run(shell_cp, check=True, shell=True)
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

            if args.snow_env == "DEV": 
                logger.info("Chosen the DEV environment for the SNOWFLAKE destination")
                SNOWFLAKE_PASSWORD = "XXXXXX"
                ENVIRONMENT = "RAW_DEV"
                USERNAME="MWAA_SVC_USER_DEV"
            else:
                logger.info("Chosen the PROD environment for the SNOWFLAKE destination")
                SNOWFLAKE_PASSWORD = "XXXXXX"
                ENVIRONMENT = "RAW_PRD"
                USERNAME="MWAA_SVC_USER_PRD"
                
            SQL_FILE = f"/nfsdata/dataop/uploader/snowsql/snowsql_load_{final_topic}.sql"
            
            shell_cp = f"SNOWSQL_PWD=\"{SNOWFLAKE_PASSWORD}\" REQUESTS_CA_BUNDLE=/etc/pki/ca-trust/extracted/openssl/ca-bundle.trust.crt snowsql -a NGB13288 -u {USERNAME} -o insecure_mode=True -f {SQL_FILE} -d {ENVIRONMENT}"
            try:
                logger.info("Executing command " + shell_cp)
                completed_process = subprocess.run(shell_cp, check=True, shell=True)
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
            logger.warning("Now, I need to further execute the uploading script")
    else:

        shell_cp = (
            f"/usr/local/bin/aws s3 cp {output_path} "
            "s3://caltrans-pems-dev-us-west-2-raw/db96_export_staging_area"
            f"/tables/{final_topic}/{date_folder_substring}{district_folder_substring}"
            f" --ca-bundle /etc/pki/ca-trust/extracted/openssl/ca-bundle.trust.crt"
        )
        try:
            logger.info("Executing command" + shell_cp)
            completed_process = subprocess.run(shell_cp, check=True, shell=True)
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

        shell_cp = (
            f"/usr/local/bin/aws s3 cp --profile prod {output_path} "
            "s3://caltrans-pems-prd-us-west-2-raw/db96_export_staging_area"
            f"/tables/{final_topic}/{date_folder_substring}{district_folder_substring}"
            f" --ca-bundle /etc/pki/ca-trust/extracted/openssl/ca-bundle.trust.crt"
        )
        try:
            logger.info("Executing command" + shell_cp)
            completed_process = subprocess.run(shell_cp, check=True, shell=True)
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


ZOOKEEPER_CLUSTER = (
    "svgcmdl03:2181,svgcmdl04:2181,svgcmdl05:2181,svgcmdl01:2181,svgcmdl02:2181"
)


def start_zoo_client_with_retry(zk, retries=3, delay=5):
    """
    Starts a Zookeeper client with the given number of retries and delay in between retries.

    Parameters
    ----------
    - zk: Zookeeper client object
    - retries: Number of times to retry starting the client (default is 3)
    - delay: Delay in seconds between retries (default is 5)

    Returns
    -------
    - True if the client is successfully started
    - False if the client fails to start after all retries

    """
    for i in range(retries):
        try:
            zk.start()
            logger.info("Zookeeper client started successfully")
            return True
        except Exception as e:  # adjust this with the right exception type
            logger.error(f"{e!s}")
            if i < retries - 1:  # if it"s not the last retry
                sleep(delay)  # wait for a bit before retrying
                logger.error("continuing retry")
            else:
                return False
    return False


if __name__ == "__main__":
    if not args.debug:
        zk = KazooClient(hosts=ZOOKEEPER_CLUSTER)
        if not start_zoo_client_with_retry(zk):
            logger.error("Unable to connect after multiple retries")
            raise ValueError("cannot start zookeeper")
        from kazoo.recipe.lock import Lock
        lock_path = "/var/coordinator/my_instance_lock_for_aws_uploader"
        logger.info(f"I"m about to enter an section of lock ({lock_path}) ")
        # kafka_log_handler.flush()
        lock = Lock(zk, lock_path)
        with lock:
            logger.info(
                f"I"m the only one running table_aws_uploader.py under lock path {lock_path}"
            )
            pull()
            logger.info("I finished the locked session")
    elif "signature" in args.debug:
        # example of debugging string
        args_debug = args.debug
        # extract "signature" from args.debug string
        args_debug_list = args_debug.split(",")
        debug_dict = {k.strip(): v.strip() for k, v in
                          (item.split("=") for item in args_debug_list)}
        signature = debug_dict["signature"]
        # search file for line matching "signature"
        with open("/tmp/table_aws_uploader.log") as file:
            for _line_num, line in enumerate(file, 1):
                if signature in line:
                    parts = line.split("output_path ")
                    json_path = parts[1].strip()
                    if "head" not in debug_dict:
                        upload(json_path, args.date_time, args.topic)
                    else:
                        import json

                        def copy_file_path(input_path):
                            # Separate the directory path and file name to extract the timestamp
                            parts = input_path.split("/")
                            # Extract the timestamp from the file name
                            parts[-1].split("_")[-1].split(".")[0]
                            # Reconstruct the output path with the desired changes
                            return "/".join(parts[:-1]) + "/bk." + parts[-1]
                            # Return the output path
                        new_path = copy_file_path(json_path)
                        def copy_json_lines(json_path, new_path, n):
                            with open(json_path) as file:
                                lines = file.readlines()[:n]

                            with open(new_path, "w") as new_file:
                                for line in lines:
                                    new_file.write(line)
                        copy_json_lines(json_path, new_path, int(debug_dict["head"]))
                        upload(new_path, args.date_time, args.topic)

                    break  # remove break if searching for all lines with "signature"

