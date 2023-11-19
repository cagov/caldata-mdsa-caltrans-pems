from __future__ import annotations

from datetime import date, datetime, timedelta

import boto3
import fsspec
import requests

from airflow.decorators import dag, task

CLHOUSE_PREFIX = "https://pems.dot.ca.gov/feeds/clhouse"
S3_PREFIX = "s3://caltrans-pems-dev-us-west-2-raw"
DISTRICTS = [ "d03", "d04", "d05", "d06", "d07", "d08", "d10", "d11", "d12" ]

def copy_file(src, dst, s3) -> None:
    with fsspec.open(src, "rb") as f:
        s3.upload_fileobj(
            f,
            dst.removeprefix("s3://").split("/")[0],
            "/".join(dst.removeprefix("s3://").split("/")[1:]),
        )

def clearinghouse_to_s3(day: date) -> None:
    """
    Copy PeMS vehicle detector data from the clearinghouse website to S3 for a date.

    Note: this could be massively parallelized, but we are trying not to stress the
    somewhat old and brittle web server, and are keeping to only a few concurrent
    downloads. So there is not much sense in doing huge amounts of concurrency here.

    Parameters
    ----------

    day: datetime.date
        The date for which to copy data.
    """
    s3 = boto3.client("s3")

    for d in DISTRICTS:
        # Figure out whether we are using the old style URL or the new style
        dist = d
        r = requests.get(
            f"{CLHOUSE_PREFIX}/{dist}/{day.year}/{day.month:02}/text/station_raw/"
        )
        if r.status_code == 404:
            # Didn't find data, try the old style
            dist = dist.lstrip("d0")
            r = requests.get(
                f"{CLHOUSE_PREFIX}/{dist}/{day.year}/{day.month:02}/text/station_raw/"
            )
            if r.status_code == 404:
                raise RuntimeError(f"Could not find raw data for date {day}")

        # Download the raw data
        print(f"Copying Caltrans district {d} data for {day}")
        src_raw_url = (
            f"{CLHOUSE_PREFIX}/{dist}/{day.year}/{day.month:02}/text/station_raw/"
            f"{d}_text_station_raw_{day.year}_{day.month:02}_{day.day:02}.txt.gz"
        )
        dst_raw_url = (
            f"{S3_PREFIX}/clhouse/raw/{d}/{day.year}/{day.month:02}/"
            f"{d}_text_station_raw_{day.year}_{day.month:02}_{day.day:02}.txt.gz"
        )
        try:
            copy_file(src_raw_url, dst_raw_url, s3)
        except (FileNotFoundError, ValueError):
            # Some dates are missing, probably due to past incidents!
            print(f"Failed to download {src_raw_url}, it may be missing")

        # Download the metadata
        src_meta_url = (
            f"{CLHOUSE_PREFIX}/{dist}/{day.year}/{day.month:02}/meta/"
            f"{d}_text_meta_{day.year}_{day.month:02}_{day.day:02}.txt"
        )
        dst_meta_url = (
            f"{S3_PREFIX}/clhouse/meta/{d}/{day.year}/{day.month:02}/"
            f"{d}_text_meta_{day.year}_{day.month:02}_{day.day:02}.txt"
        )
        try:
            copy_file(src_meta_url, dst_meta_url, s3)
        except (FileNotFoundError, ValueError):
            pass  # Not every date has meta

        src_meta_xml_url = (
            f"{CLHOUSE_PREFIX}/{dist}/{day.year}/{day.month:02}/meta/"
            f"{d}_tmdd_meta_{day.year}_{day.month:02}_{day.day:02}.xml"
        )
        dst_meta_xml_url = (
            f"{S3_PREFIX}/clhouse/status/{d}/{day.year}/{day.month:02}/"
            f"{d}_tmdd_meta_{day.year}_{day.month:02}_{day.day:02}.xml"
        )
        try:
            copy_file(src_meta_xml_url, dst_meta_xml_url, s3)
        except (FileNotFoundError, ValueError):
            # Not every date has status, some are missing (FileNotFoundError),
            # and some are empty (ValueError)
            pass


@task
def pems_clearinghouse_to_s3_task(**context):
    prev_date = (context["execution_date"] - timedelta(days=1)).date()
    clearinghouse_to_s3(prev_date)

@dag(
    description="Load data from the PeMS clearinghouse webserver to S3",
    start_date=datetime(1994, 12, 1),
    schedule_interval="0 12 * * *",  # 4 AM PST
    default_args={
        "owner": "Traffic Operations",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    concurrency=2,  # We don't want to stress the webserver
    catchup=False,
)
def pems_clearinghouse_to_s3_dag():
    pems_clearinghouse_to_s3_task()

# TODO: is this necessary in Airflow 2.7.2?
run = pems_clearinghouse_to_s3_dag()
