"""ETL pipeline for creating Mentre data in Redshift."""

from airflow import DAG
from airflow.operators.python import PythonOperator

from code.database_funcs import ddl_query, select_query


with DAG(
    "create_db_redshift",
    description="Create the necessary tables for Mentre in Redshift",
) as dag:
    try_redshift_connection_task = PythonOperator(
        task_id="try_redshift_connection_task",
        python_callable=select_query("tables.sql"),
    )

    create_drivers_task = PythonOperator(
        task_id="create_drivers",
        python_callable=ddl_query("create_drivers.sql"),
    )
    create_usuarios_task = PythonOperator(
        task_id="create_usuarios",
        python_callable=ddl_query("create_usuarios.sql"),
    )
    create_viajes_task = PythonOperator(
        task_id="create_viajes",
        python_callable=ddl_query("create_viajes.sql"),
    )
    create_viajes_eventos_task = PythonOperator(
        task_id="create_viajes_eventos",
        python_callable=ddl_query("create_viajes_eventos.sql"),
    )
    create_clima_task = PythonOperator(
        task_id="create_clima",
        python_callable=ddl_query("create_clima.sql"),
    )

    try_redshift_connection_task >> create_clima_task
    try_redshift_connection_task >> [create_drivers_task, create_usuarios_task] >> create_viajes_task
    create_viajes_task >> create_viajes_eventos_task
