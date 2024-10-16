"""ETL pipeline for mocking more Mentre trips in Redshift."""

from airflow import DAG
from airflow.operators.python import PythonOperator

from code.database_funcs import select_query
from tasks.mock_data_redshift import (
    get_mock_viajes,
    get_mock_viajes_eventos,
)
from tasks.mock_new_viajes import check_table_task


with DAG(
    "mock_new_viajes",
    description="Generate more mock trips in Redshift",
) as dag:
    # Tasks

    try_redshift_connection_task = PythonOperator(
        task_id="try_redshift_connection_task",
        python_callable=select_query("tables.sql"),
    )

    mock_drivers_task = PythonOperator(
        task_id="mock_drivers",
        python_callable=check_table_task(
            "drivers",
            is_fixed_table=False,
            check_local_csv=True,
        ),
    )
    mock_usuarios_task = PythonOperator(
        task_id="mock_usuarios",
        python_callable=check_table_task(
            "usuarios",
            is_fixed_table=False,
            check_local_csv=True,
        ),
    )

    mock_viajes_task = PythonOperator(
        task_id="mock_viajes",
        python_callable=get_mock_viajes(append=True),
    )
    mock_viajes_eventos_task = PythonOperator(
        task_id="mock_viajes_eventos",
        python_callable=get_mock_viajes_eventos(append=True),
    )

    # Task dependencies

    try_redshift_connection_task >> [mock_drivers_task, mock_usuarios_task] >> mock_viajes_task >> mock_viajes_eventos_task
