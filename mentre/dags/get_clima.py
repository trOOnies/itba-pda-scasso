"""ETL pipeline for Mentre data in Redshift."""

from airflow import DAG
from airflow.operators.python import PythonOperator
import os

from code.database_funcs import select_query
from tasks.get_clima import (
    extract_data,
    load_to_redshift,
    transform_data,
)

DIR_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    "local",
)
REDSHIFT_TABLE = "clima"


with DAG(
    "get_clima",
    description="ETL pipeline for climate data",
    # default_args={
    #     "retries": 1,
    # },
    # schedule_interval="@hourly",
    # start_date=datetime(2024, 1, 1),
    # catchup=False,
) as dag:
    try_redshift_connection_task = PythonOperator(
        task_id="try_redshift_connection_task",
        python_callable=select_query("tables.sql"),
    )

    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
        op_kwargs={"extracted_fd_path": DIR_PATH},
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
        op_kwargs={"transformed_fd_path": DIR_PATH},
    )

    load_to_redshift_task = PythonOperator(
        task_id="load_to_redshift",
        python_callable=load_to_redshift,
        op_kwargs={"redshift_table": REDSHIFT_TABLE},
    )

    # We first try the Redshift connection,
    # to avoid wasting calls to the AccuWeather API
    try_redshift_connection_task >> extract_task
    extract_task >> transform_task >> load_to_redshift_task
