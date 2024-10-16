"""ETL pipeline for inserting mock Mentre data in Redshift.
This is just for demonstrating purposes and ease-of-use for the developer
and the class professors.
"""

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator

from code.database_funcs import ddl_query, select_query
from tasks.mock_data_redshift import (
    check_viajes_analisis,
    mock_clima,
    mock_clima_id,
    mock_drivers,
    mock_usuarios,
    mock_viajes,
    mock_viajes_eventos,
    viajes_analisis_is_full,
)


with DAG(
    "mock_data_redshift",
    description="Generate mock data and replace contents in Redshift",
) as dag:
    # Tasks

    try_redshift_connection_task = PythonOperator(
        task_id="try_redshift_connection_task",
        python_callable=select_query("tables.sql"),
    )

    mock_drivers_task = PythonOperator(
        task_id="mock_drivers",
        python_callable=mock_drivers,
    )
    mock_usuarios_task = PythonOperator(
        task_id="mock_usuarios",
        python_callable=mock_usuarios,
    )
    mock_viajes_task = PythonOperator(
        task_id="mock_viajes",
        python_callable=mock_viajes,
    )
    mock_viajes_eventos_task = PythonOperator(
        task_id="mock_viajes_eventos",
        python_callable=mock_viajes_eventos,
    )

    mock_clima_id_task = PythonOperator(
        task_id="mock_clima_id",
        python_callable=mock_clima_id,
    )
    mock_clima_task = PythonOperator(
        task_id="mock_clima",
        python_callable=mock_clima,
    )

    check_viajes_analisis_task = PythonOperator(
        task_id="check_viajes_analisis",
        python_callable=check_viajes_analisis,
    )

    branch_task = BranchPythonOperator(
        task_id='branch',
        python_callable=viajes_analisis_is_full,
    )
    va_is_empty_task = PythonOperator(
        task_id="va_is_empty",
        python_callable=ddl_query("create", "viajes_analisis.sql"),
    )
    va_is_full_task = EmptyOperator(task_id="va_is_full")

    # Task dependencies

    try_redshift_connection_task >> mock_clima_id_task >> mock_clima_task
    (
        try_redshift_connection_task
        >> [mock_drivers_task, mock_usuarios_task]
        >> mock_viajes_task
        >> [mock_viajes_eventos_task, mock_clima_task]
        >> check_viajes_analisis_task
        >> branch_task
        >> [va_is_empty_task, va_is_full_task]
    )
