"""ETL pipeline for inserting mock Mentre data in Redshift.
This is just for demonstrating purposes and ease-of-use for the developer
and the class professors.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator

from code.database_funcs import select_query
from code.mock_data_redshift import mock_drivers, mock_usuarios


with DAG(
    "mock_data_redshift",
    description="Generate mock data and replace contents in Redshift",
) as dag:
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
    # mock_viajes_and_eventos_task = PythonOperator(
    #     task_id="mock_viajes_and_eventos",
    #     python_callable=mock_viajes_and_eventos,
    # )

    try_redshift_connection_task >> [mock_drivers_task, mock_usuarios_task]  # >> mock_viajes_and_eventos
