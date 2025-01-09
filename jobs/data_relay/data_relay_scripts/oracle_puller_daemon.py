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
from confluent_kafka.cimpl import NewTopic, TopicPartition

# Create KafkaProducer instance
KAFKA_SERVERS = (
    "PLAINTEXT://svgcmdl03:9092, PLAINTEXT://svgcmdl04:9092, PLAINTEXT://svgcmdl05:9092"
)
KAFKA_SCHEMA_REGISTRY_SERVER = "http://svgcmdl04:8092"
ELASTIC_SEARCH_SERVER = "https://svgcmdl02.dot.ca.gov:9200"
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


import argparse

# Create the parser
parser = argparse.ArgumentParser(description='Crawl at a specified queue')

# Add the --queue argument
parser.add_argument(
    '--queue',
    type=int,
    default=-1,
    help='Specify the crawl queue. Default is -1, which will be random. Otherwise, it will be a specified crawl queue.'
)

# Parse the arguments
args = parser.parse_args()



logger.add(
    f"/tmp/oracle_puller.log",
    level="INFO",
    format="{time} {level} {file}:{line} {message}",
    rotation="500 MB",
    retention="10 days",
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

        df = pd.read_sql(sql_query, conn, dtype_backend="pyarrow")
        logger.debug(f"df.dtypes {df.dtypes}")
        logger.debug(f"df.memory_usage {df.memory_usage()}")

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
        "retention.ms": 1800000  # 30 minutes in milliseconds
    }

    # instantiate a NewTopic object
    topic = NewTopic(
        topic_name,
        num_partitions=topic_configs["num_partitions"],
        replication_factor=topic_configs["replication_factor"],
        config={"retention.ms": topic_configs["retention.ms"]}
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

    if crawl_task["Database"] not in ["PEMS","PEMS_DB96"]:
        logger.info(
            f"Database {crawl_task['v5_Database']} is neither PEMS nor PEMS_DB96, and this crawler is not supporting that."
        )
        raise ValueError('Not supported')

   
    sql_query = (
        crawl_task["Sql"]
        .replace("TABLE_PLACEHOLDER",  crawl_task["Table"])
        .replace("DATE_PLACEHOLDER", crawl_task["Date"])
    )

    logger.info(f"sql: {sql_query}")
    database_mapper = {"PEMS": "DWO", "PEMS_DB96": "DB96"}
    logger.info(f"Executing sql_query {sql_query}")
    df = Get_Data_From_PeMS(
        database_mapper[crawl_task["Database"]], sql_query
    )

    if df is None:
        raise ValueError('No data')

    create_topic_if_nonexist(crawl_task["Table"])
    if not df.empty:
        logger.info(f"git(GITHASH_PLACEHOLDER) head: {df.head()}")


    start_time = time.time()

    if crawl_task["Table"] in ['STATION_CONFIG',
                    'STATION_CONFIG_LOG',
                    'DETECTOR_CONFIG',
                    'DETECTOR_CONFIG_LOG',
                    'CONTROLLER_CONFIG',
                    'CONTROLLER_CONFIG_LOG']:
        df.to_parquet(f"/nfsdata/dataop/uploader/tmp/{crawl_task["Table"]}/{crawl_task["Table"]}_dump_static.parquet")
        logger.info(f"Table {crawl_task["Table"]} is one of six static files. We write to parquet file.")
        return
    
    # In VDS30SEC then. 

    logger.debug("Table {table} is not in the six config tables so we do not write to static file")
    logger.info(f" size of this chunk is {len(df)}")
    for index, row_chunk in df.iterrows():
        try:
            if index % 10000 == 0:
                logger.info(f" current index: {index} ")
            # Convert row (a Series) to a DataFrame
            row_chunk_obj = pd.DataFrame(row_chunk)
            # row_chunk_obj = row_chunk_obj.astype(dtype='pyarrow')
            # Convert the DataFrame to JSON
            json_data = row_chunk_obj.to_json()
            row_chunk_obj = None
            # Send data to Kafka topic
            import json

            dict_data = json.loads(json_data)
            json_data = list(dict_data.values())[0]

            import random

            random_number = random.randint(0, 999)
            random_number_str = str(random_number)
            key_data = {"key_field": random_number_str}

            meta_obj = {
                "dt": crawl_task["Date"]
            }

            aproducer.produce(
                topic=crawl_task["Table"],
                key=key_data,
                value={
                    "serialized_json": json.dumps(json_data),
                    "meta": json.dumps(meta_obj),
                },
                value_schema=avro.loads(SCHEMA_TEMPLATE),
                key_schema=avro.loads(SCHEMA_TEMPLATE_FOR_KEY),
            )
            if index % 10000 == 0:
                logger.info(f" current index end of iter: {index}")
                aproducer.flush()
        except Exception as e:
            print("An error occurred:", e)

    elapsed_time = time.time() - start_time
    logger.info(
        f"Elapsed time: {elapsed_time} seconds for the above code block for DataFrame processing"
    )
    aproducer.flush()
    logger.info("Producer flushed successfully")
    

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
        logger.debug("No message received by consumer")
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


    import json

    import random
    if args.queue == -1:
        partition_number = random.choice([1, 1, 1])
    else:
        partition_number = args.queue

    PARTITION_NUMBER_FOR_THE_VDS30SEC_TABLE_CRAWL_TASKS = 0
    PARTITION_NUMBER_FOR_THE_CONFIG_TABLE_CRAWL_TASKS = 1

    # Production partitions (VDS30SEC and CONFIG) is using manual commit (better guarantee)
    #    The risk is, if a particular task always failing, the crawl will stall there. 
    if partition_number in [PARTITION_NUMBER_FOR_THE_VDS30SEC_TABLE_CRAWL_TASKS, 
                            PARTITION_NUMBER_FOR_THE_CONFIG_TABLE_CRAWL_TASKS]:
        logger.info(f"Production partition {partition_number} is using commit mechanism")
        crawl_task_queue_consumer = AvroConsumer(
            {
                "bootstrap.servers": KAFKA_SERVERS,
                "group.id": "puller_run_004",
                "schema.registry.url": "http://svgcmdl04:8092",
                "auto.offset.reset": "earliest",  # add this line
                'enable.auto.commit': False  # Disable auto commit
            }
        )
    else: # Adhoc partitions (2) is using auto commit (crawl based on best effort basis, and for 
        # the failed task, it won't re-try, it will commit regardless and move to the next task. 
        logger.info(f"Adhoc (non-production) partition {partition_number} is using autocommit mechanism")
        crawl_task_queue_consumer = AvroConsumer(
            {
                "bootstrap.servers": KAFKA_SERVERS,
                "group.id": "puller_run_004",
                "schema.registry.url": "http://svgcmdl04:8092",
                "auto.offset.reset": "earliest",  # add this line
                'enable.auto.commit': True  # Disable auto commit
            }
        )

    # Manually assign partitions
    partitions = [TopicPartition('oracle_crawl_queue_topic', partition_number)]
    crawl_task_queue_consumer.assign(partitions)



    zk = KazooClient(hosts=ZOOKEEPER_CLUSTER)
    if not start_zoo_client_with_retry(zk):
        logger.error("Unable to connect after multiple retries")
        raise ValueError("cannot start zookeeper")
    from kazoo.recipe.lock import Lock

    lock_path = "/var/coordinator/my_single_instance_lock_for_oracle_puller"
    logger.info(f"I'm about to enter an section of lock ({lock_path}) ")
    lock = Lock(zk, lock_path)
    with lock:
        logger.info(
            f"I'm the only one running oracle_puller_daemon.py under lock path {lock_path}"
        )
        remaining_life = 30
        while remaining_life > 0:
            remaining_life = remaining_life - 1
            logger.debug(f"Remaining life is {remaining_life}")
            try:
                logger.info(f"start iter /w remaining life {remaining_life}")
                # Attempt to pull a new message from the Kafka topic
                message = crawl_task_queue_consumer.poll(10)
                
                if message is None:
                    logger.debug("Message is None, skip")
                    time.sleep(0)
                    continue
                if message.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition
                        logger.info(f"End of partition")
                        continue
                    else:
                        logger.info("AvroConsumer error: {}".format(message.error()))
                        time.sleep(3)
                        continue
                msg_json = message.value()
                if partition_number == 0 or partition_number == 1:
                    logger.info(f"Right now the offset of crawl queue is {message.offset()}, uncommited, and I'm not going to commit until success and grace wait all ends, because this is production partition {partition_number}")
                else:
                    logger.info(f"Right now the offset of crawl queue is {message.offset()}, and it will be auto-commited regardless of outcome because partition {partition_number} is non-production.")

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
                    logger.debug("Picked up v6")
                    
                    # Create a new dataframe row
                    try:
                        inner_loop_crawl_operation(msg_json)
                        time.sleep(3)
                        if partition_number == 0 or partition_number == 1:
                            # Make sure that after wait, no interruption, then proceed to the next, to give user interruption an
                            # grace period before moving forward.
                            crawl_task_queue_consumer.commit(asynchronous=False)  # Synchronous commit
                            logger.info(f"Now the message with offset {message.offset()} has been commited. Moving forward to the next...")
                        else:
                            logger.info(f"The message with offset {message.offset()} is auto commited without using the explicit commit. Moving forward...")
                    except cx_Oracle.DatabaseError as e:
                        logger.error(
                            f"Database error {str(e)} and decide to skip this {json.dumps(msg_json)}"
                        )
                        time.sleep(3)
                    except Exception as e:
                        logger.error(f"Exception {str(e)}")
                        time.sleep(3)
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

