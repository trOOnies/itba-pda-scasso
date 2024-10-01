"""ETL pipeline for Mentre data in Redshift."""

from airflow import DAG
from airflow.operators.python import PythonOperator
import os

from code.etl import extract_data, transform_data, load_to_redshift

DIR_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    "local",
)
REDSHIFT_TABLE = "example_table"


with DAG(
    "etl_redshift",
    description="ETL pipeline for Mentre data in Redshift",
    # default_args={
    #     "retries": 1,
    # },
    # schedule_interval="@hourly",
    # start_date=datetime(2024, 1, 1),
    # catchup=False,
) as dag:
    # pass
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

    extract_task >> transform_task >> load_to_redshift_task
