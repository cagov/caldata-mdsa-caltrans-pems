from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id="pems_backfill_dag",
    schedule_interval=None,
    catchup=False,
    start_date=days_ago(1)
) as dag:
    cli_command = BashOperator(
        task_id="backfill_command",
        bash_command=(
            "airflow dags backfill --start-date 2020-01-01 --end-date 2023-01-01"
            " --run-backwards --continue-on-failure pems_clearinghouse_to_s3_dag"
        ),
    )
