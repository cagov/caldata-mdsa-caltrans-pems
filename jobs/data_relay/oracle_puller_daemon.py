#!/usr/bin/env python3
""" oracle_puller_daemon.py
This job performs the duty of
(1) Check the crawl queue's incoming tasks
(2) Do the task (pull oracle table and store data in corresponding kafka topic)
"""

import datetime
import os
import cx_Oracle
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic


# Create KafkaProducer instance
KAFKA_SERVERS = (
    "PLAINTEXT://svgcmdl03:9092, PLAINTEXT://svgcmdl04:9092, PLAINTEXT://svgcmdl05:9092"
)
KAFKA_SCHEMA_REGISTRY_SERVER = "http://svgcmdl04:8092"
ELASTIC_SEARCH_SERVER = "http://svgcmdl02:9200"
KAFKA_CLIENT_CONFIG = {
    "bootstrap.servers": KAFKA_SERVERS,
    "schema.registry.url": KAFKA_SCHEMA_REGISTRY_SERVER,
}
ZOOKEEPER_CLUSTER = (
    "svgcmdl03:2181,svgcmdl04:2181,svgcmdl05:2181,svgcmdl01:2181,svgcmdl02:2181"
)

SCHEMA_TEMPLATE_FOR_KEY = """
{
   "namespace": "com.caltrans.bia",
   "name": "KAFKATABLE_key",
   "type": "record",
   "fields" : [
     {
       "name" : "key_field",
       "type" : "string",
       "doc": ""
     }
   ]
}
"""

SCHEMA_TEMPLATE = """
{
   "namespace": "com.caltrans.bia",
   "name": "KAFKATABLE_value",
   "type": "record",
   "fields" : [
     {
       "name" : "serialized_json",
       "type" : "string"
     },
     {
       "name" : "meta",
       "type" : "string"
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
        elif isinstance(obj, datetime.datetime):
            return obj.isoformat()
        elif isinstance(obj, datetime.timedelta):
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
    f"/tmp/oracle_puller.log",
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


# -----------------------------------
def Get_Data_From_PeMS(db_name, sql_query):
    """
    This function retrieves data from PeMS database based on the provided database name
    and SQL query.

    Note: the environment that this function can run is strictly limited to
        `svgcmdl01.dot.ca.gov` Moreover, the user should be `jupyter`.

    Parameters:
    - db_name: str, name of the database (DWO or DB96)
    - sql_query: str, SQL query to retrieve data
    Returns:
    - df: pandas DataFrame containing the data retrieved from the database
    Raises:
    - ValueError: If an invalid database name is provided
    Example Usage:
       df = Get_Data_From_PeMS("DWO", "SELECT * FROM table_name")

    """

    # For security, the credentials are specified using ENV variables rather than hardcode.
    if db_name == "DWO":  # data warehouse
        user = os.getenv("DWO_USER", "default")
        password = os.getenv("DWO_PASSWORD", "default")
        host_name = os.getenv("DWO_HOST", "default")
        sid = os.getenv("DWO_SID", "default")
    elif db_name == "DB96":  # db96, source of the clearinghouse
        user = os.getenv("DB96_USER", "default")
        password = os.getenv("DB96_PASSWORD", "default")
        host_name = os.getenv("DB96_HOST", "default")
        sid = os.getenv("DB96_SID", "default")
    else:
        raise ValueError("Invalid database name")
    dsn = cx_Oracle.makedsn(host_name, 1521, sid=sid)
    conn = cx_Oracle.connect(user=user, password=password, dsn=dsn, encoding="UTF-8")
    try:
        logger.info(f"Start DB Query {sql_query}")
        start_time = time.time()

        df = pd.read_sql(sql_query, conn)
        conn.commit()
        conn.close()
        logger.info("End DB Query")
        end_time = time.time()
        elapsed_time = end_time - start_time
        logger.info(f"Elapsed time: {elapsed_time} seconds for query: {sql_query}")
        return df
    except pd.io.sql.DatabaseError as e:
        logger.error(f"Database error: {sql_query} {str(e)} ")
        return None


import time

import json


from confluent_kafka import avro, cimpl
from confluent_kafka.avro import AvroProducer


def create_topic_if_nonexist(table):
    """
    Create a topic if the specified topic not exist in the Kafka cluster.

    Args:
        table (str): The name of the topic to check.

    Returns:
        None

    The function connects to the Kafka cluster, retrieves the list of available topics,
      and checks if the specified topic exists.
    If the topic already exists, it logs a message indicating that the topic exists.
    If the topic does not exist, it logs a message indicating that it will create the topic.

    Example:
        check_topic_exists("my_topic")
    """
    admin_client = AdminClient({"bootstrap.servers": KAFKA_SERVERS})

    # retrieve the list of available topics
    available_topics = admin_client.list_topics().topics
    kafka_topic = table

    # check if variable table exists
    if kafka_topic in available_topics:
        logger.info(f'Topic "{kafka_topic}" already exists.')
    else:
        logger.info(f'Topic "{kafka_topic}" does not exist. creating...')
        create_kafka_topic(KAFKA_SERVERS, kafka_topic)


from confluent_kafka import Producer

# Create a kafka producer instance
crawl_event_producer = Producer(**{"bootstrap.servers": KAFKA_SERVERS})


aproducer = AvroProducer(
    KAFKA_CLIENT_CONFIG,
    default_key_schema=avro.loads(SCHEMA_TEMPLATE_FOR_KEY),
    default_value_schema=avro.loads(SCHEMA_TEMPLATE),
)


def create_kafka_topic(broker, topic_name):
    """

    This function creates a Kafka topic with the specified name on the given broker.

    Parameters:
    - broker (str): The Kafka broker address.
    - topic_name (str): The name of the topic to be created.

    Returns:
     None
    """
    admin_client = AdminClient({"bootstrap.servers": broker})

    # define the topic configs
    topic_configs = {
        "num_partitions": 1,
        "replication_factor": 3,
    }

    # instantiate a NewTopic object
    topic = NewTopic(
        topic_name,
        num_partitions=topic_configs["num_partitions"],
        replication_factor=topic_configs["replication_factor"],
    )
    # create the topic
    create_topics_resp_future_type = admin_client.create_topics([topic])
    try:
        result = create_topics_resp_future_type[topic_name].result()
        logger.info(f'Topic "{topic_name}" created.')
        logger.info(f"Result is {str(result)}")
    except cimpl.KafkaException as e:
        logger.error(f'Topic "{topic_name}" has exception {str(e)}.')


def inner_loop_crawl_operation(crawl_task):
    """
    Perform inner loop crawl operation based on the input crawl task.

    Parameters:
    - crawl_task (dict): Dictionary containing crawl task meta data

    Returns:
    - df (DataFrame): DataFrame pulled from oracle if the sql operation
        specified in the crawl task is successful, None otherwise
    """

    if crawl_task["Schema"] in ["v1", "v2", "v3", "v4", "v5"]:
        logger.info(f"Schema is {crawl_task['Schema']}, now skip")
        return None, ""
    elif crawl_task["Schema"] == "v6":
        # Get start and end indices
        if (
            crawl_task["v5_Database"] != "PEMS"
            and crawl_task["v5_Database"] != "PEMS_DB96"
        ):
            logger.info(
                f"Database {crawl_task['v5_Database']} is neither PEMS nor PEMS_DB96, and this crawler is not supporting that."
            )
        else:
            logger.info(
                f"Schema {crawl_task['Schema']} picked up and database is {crawl_task['v5_Database']}"
            )
            table = crawl_task["Table"]
            if crawl_task["v4_Purpose"] == "Crawl":
                start_idx = int(crawl_task["v1_Start_Index"])
                end_idx = int(crawl_task["v1_End_Index"])
                date = crawl_task["Date"]

                sql_query = (
                    crawl_task["v3_Sql"]
                    .replace("TABLE_PLACEHOLDER", table)
                    .replace("DATE_PLACEHOLDER", date)
                    .replace("START_PLACEHOLDER", str(start_idx))
                    .replace("END_PLACEHOLDER", str(end_idx))
                )

                logger.info(f"sql: {sql_query}")
                database_mapper = {"PEMS": "DWO", "PEMS_DB96": "DB96"}
                logger.info(f"Executing sql_query {sql_query}")
                df = Get_Data_From_PeMS(
                    database_mapper[crawl_task["v5_Database"]], sql_query
                )
                if df is not None:
                    logger.debug(f"git(GITHASH_PLACEHOLDER) df at outter {df.head()}")

                    create_topic_if_nonexist(table)
                    if not df.empty:
                        logger.info(f"git(GITHASH_PLACEHOLDER) head: {df.head()}")

                        import time
                        import requests
                        import json

                        url = f"{KAFKA_SCHEMA_REGISTRY_SERVER}/subjects/{table}-value/versions/latest"
                        response = requests.get(url)
                        logger.info(f"returned: {response.json()}")
                        target_schema = SCHEMA_TEMPLATE.replace(
                            "KAFKATABLE_value", f"{table}_value"
                        )
                        response_obj = response.json()
                        if (
                            "error_code" in response_obj
                            and response_obj["error_code"] == 40401
                        ):
                            logger.info(
                                f"At this points, topic does not exist. So, decide to change schema later"
                            )
                        else:
                            latest_schema = response_obj["schema"]
                            from deepdiff import DeepDiff

                            known_fields_lookup = {
                                "serialized_json": True,
                                "meta": True,
                            }

                            diff = DeepDiff(
                                [
                                    o
                                    for o in json.loads(latest_schema)["fields"]
                                    if known_fields_lookup[o["name"]]
                                ],
                                [
                                    o
                                    for o in json.loads(target_schema)["fields"]
                                    if known_fields_lookup[o["name"]]
                                ],
                            )

                            if diff != {}:
                                logger.info(
                                    f"Old schema {latest_schema} new schema {target_schema}"
                                )
                                logger.info(f"Diff is {json.dumps(diff)}")
                                url = f"{KAFKA_SCHEMA_REGISTRY_SERVER}/subjects/{table}-value/versions"
                                headers = {
                                    "Content-Type": "application/vnd.schemaregistry.v1+json"
                                }
                                payload = {"schema": target_schema}
                                response = requests.post(
                                    url, headers=headers, data=json.dumps(payload)
                                )
                                if response.status_code == 200:
                                    logger.info(
                                        "Schema has been successfully registered"
                                    )
                                else:
                                    logger.info(
                                        "Error registering the schema: ",
                                        response.json(),
                                    )
                                    raise ValueError("schema update failed")

                                import requests
                                import json

                                headers = {"Content-Type": "application/json"}

                                dockey = f"kafka_{table}-value-schema"
                                data = {
                                    "document_key": dockey,
                                    "document_value": target_schema,
                                }
                                es_url = (
                                    f"{ELASTIC_SEARCH_SERVER}/data_dict/_doc/{dockey}"
                                )
                                response = requests.post(
                                    es_url, headers=headers, data=json.dumps(data)
                                )
                                logger.info(
                                    "Registered ES index with response " + response.text
                                )

                            else:
                                logger.info("Schema is up to date")

                        start_time = time.time()

                        for index, row_chunk in df.iterrows():
                            # Convert row (a Series) to a DataFrame
                            row_chunk_obj = pd.DataFrame(row_chunk)
                            # Convert the DataFrame to JSON
                            json_data = row_chunk_obj.to_json()
                            # Send data to Kafka topic
                            import json

                            dict_data = json.loads(json_data)
                            json_data = list(dict_data.values())[0]

                            import random

                            random_number = random.randint(0, 999)
                            random_number_str = str(random_number)
                            key_data = {"key_field": random_number_str}

                            meta_obj = {
                                "h": crawl_task["GitHash"],
                                "dt": crawl_task["Date"],
                                "s": int(crawl_task["v1_Start_Index"]),
                                "e": int(crawl_task["v1_End_Index"]),
                            }

                            aproducer.produce(
                                topic=table,
                                key=key_data,
                                value={
                                    "serialized_json": json.dumps(json_data),
                                    "meta": json.dumps(meta_obj),
                                },
                                value_schema=avro.loads(target_schema),
                                key_schema=avro.loads(SCHEMA_TEMPLATE_FOR_KEY),
                            )

                        elapsed_time = time.time() - start_time
                        logger.info(
                            f"Elapsed time: {elapsed_time} seconds for the above code block for DataFrame processing"
                        )
                        aproducer.flush()
                        logger.info("Producer flushed successfully")
                    else:
                        logger.info(
                            f"Empty chunk, does not involve kafka topic operations for {sql_query}"
                        )
                else:
                    logger.info(f"This returned None for {sql_query}")
            else:
                raise ValueError(f'{crawl_task["v4_Purpose"]} not in Count Crawl')

    return df


from confluent_kafka.avro import AvroConsumer


from kazoo.client import KazooClient
from time import sleep


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


import pandas as pd
import json


def load_latest_offset_details():
    """
    Load the latest offset details from the "crawl_queue_offsets.json" temporary file in the same directory

    Returns:
        dict: A dictionary containing the offset details.
        If the file is empty or does not exist, returns None.

    Raises:
        FileNotFoundError: If the "crawl_queue_offsets.json" file is not found.

    Example:
        If the content of "offsets.json" file is:
        {"offset": 100}
        {"offset": 200}
        {"offset": 300}
        load_latest_offset_details() will return:
        {"offset": 300}
    """
    offset_details = None
    try:
        with open("crawl_queue_offsets.json", "r") as f:
            lines = f.readlines()
            if lines:
                offset_details = json.loads(lines[-1])
    except FileNotFoundError:
        logger.info("File not found")
    return offset_details


def force_start_at_offset(cconsumer, offset_info):
    """
    Force the consumer to start consuming messages from a specific offset.

    Args:
    cconsumer (confluent_kafka.Consumer): The Kafka consumer instance.
    offset_details (dict): A dictionary containing information about the offset to start consuming from.
                           Should have keys 'topic', 'partition', and 'offset'.
    Returns:
    None

    Description:
    If offset_details is not None, this function seeks the consumer to the latest offset for every partition,
    If offset_details is None, the function simply restarts consumption.

    """

    if offset_info is not None:
        # Seek to the latest offset for specific partition
        logger.info(
            f"Seek to the latest offset for specific partition {json.dumps(offset_info)}"
        )
        from confluent_kafka import TopicPartition

        topic_part = TopicPartition(
            offset_info.get("topic"),
            offset_info.get("partition"),
            offset_info.get("offset"),
        )
        cconsumer.assign([topic_part])
    else:
        logger.info("Offset is none, just restart")


def save_offset_info_as_json(message):
    """
    Saves the offset information of a consumed Kafka message as JSON in a file named crawl_queue_offsets.json.

    Args:
        message: The Kafka message object containing the offset information.

    Returns:
        None
    """

    # Check if the message is null, a null message may mean no new messages to consume
    if message is None:
        logger.info("No message received by consumer")
    elif message.error():
        logger.info(f"Error in message consumption: {message.error()}")
    else:
        logger.info(
            f"Successfully consumed message {message.value()} at topic={message.topic()}, partition={message.partition()}, offset: {message.offset()}"
        )
        # Write to json file
        with open("crawl_queue_offsets.json", "a") as f:
            json.dump(
                {
                    "topic": message.topic(),
                    "partition": message.partition(),
                    "offset": message.offset(),
                },
                f,
            )
            f.write("\n")


def crawl_oracle():
    """
    This function carries out the crawling tasks for Oracle data by getting crawl task messages from the Kafka
      topic 'oracle_crawl_queue_topic'.
    It follows the instruction of the crawl task message(i.e. sql query, parameter substitutions),
        performs data crawling operations, and save the crawled data into kafka topic.

    Dependencies:
    - AvroConsumer for consuming messages from Kafka.
    - KazooClient and Lock for managing locks in Zookeeper.

    Note:
    - This function is designed to be run by a single instance under a designated lock path
       to ensure exclusive processing.

    """

    import time

    main_df_date_str, main_df = None, None

    import json

    crawl_task_queue_consumer = AvroConsumer(
        {
            "bootstrap.servers": KAFKA_SERVERS,
            "group.id": "puller_run_004",
            "schema.registry.url": "http://svgcmdl04:8092",
            "auto.offset.reset": "earliest",  # add this line
        }
    )

    crawl_task_queue_consumer.subscribe(["oracle_crawl_queue_topic"])

    # Load the latest offset details
    crawl_queue_offset_info = load_latest_offset_details()
    logger.info(f"crawl_queue_offset_info {json.dumps(crawl_queue_offset_info)}")
    force_start_at_offset(crawl_task_queue_consumer, crawl_queue_offset_info)

    zk = KazooClient(hosts=ZOOKEEPER_CLUSTER)
    if not start_zoo_client_with_retry(zk):
        logger.error("Unable to connect after multiple retries")
        kafka_log_handler.flush()
        raise ValueError("cannot start zookeeper")
    from kazoo.recipe.lock import Lock

    lock_path = "/var/coordinator/my_single_instance_lock_for_oracle_puller"
    logger.info(f"I'm about to enter an section of lock ({lock_path}) ")
    kafka_log_handler.flush()
    lock = Lock(zk, lock_path)
    with lock:
        logger.info(
            f"I'm the only one running oracle_puller_daemon.py under lock path {lock_path}"
        )
        while True:
            try:
                logger.info("start iter")
                # Attempt to pull a new message from the Kafka topic
                message = crawl_task_queue_consumer.poll(10)
                kafka_log_handler.flush()
                save_offset_info_as_json(message)

                if message is None:
                    logger.info("Message is None, skip")
                    time.sleep(0)
                    continue
                if message.error():
                    logger.info("AvroConsumer error: {}".format(message.error()))
                    time.sleep(3)
                    continue
                msg_json = message.value()
                crawl_event_producer.produce(
                    "oracle_crawl_queue_event", value=json.dumps({"dummy": "0"})
                )
                crawl_event_producer.flush()

                # Check whether the key Schema is present and its value is v1
                logger.info("have message parsed")
                logger.info("process " + json.dumps(message.value()))
                if (
                    "Schema" in msg_json
                    and "v3_Sql" in msg_json
                    and msg_json["Schema"] == "v6"
                ):
                    if True:
                        logger.debug("Picked up v6")
                        # Create a new dataframe row
                        try:
                            inner_loop_crawl_operation(msg_json)
                            time.sleep(3)
                        except cx_Oracle.DatabaseError as e:
                            logger.error(
                                f"Database error {str(e)} and decide to skip this {json.dumps(msg_json)}"
                            )
                            time.sleep(3)
                    else:
                        logger.info(f"Table {msg_json['Table']} skipped")
                        time.sleep(0)
                else:
                    logger.info("This is an incompatible schema, skip it")
                    time.sleep(0)
            except StopIteration:
                # If there's no new message, wait for a bit before retrying
                logger.info("no message")
                time.sleep(5)
            except json.JSONDecodeError:
                # If we fail to parse the message as JSON, just continue to the next iteration
                logger.info("json error")
                time.sleep(3)
                continue


if __name__ == "__main__":
    crawl_oracle()

kafka_log_handler.flush()
