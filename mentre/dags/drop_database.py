"""ETL pipeline for dropping Mentre data in Redshift.
This is just for demonstrating purposes and ease-of-use for the developer
and the class professors.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator

from code.database_funcs import ddl_query, select_query


with DAG(
    "drop_db_redshift",
    description="Drop tables in Redshift",
) as dag:
    try_redshift_connection_task = PythonOperator(
        task_id="try_redshift_connection_task",
        python_callable=select_query("tables.sql"),
    )

    drop_clima_task = PythonOperator(
        task_id="drop_clima",
        python_callable=ddl_query("drop", "clima.sql"),
    )
    drop_viajes_eventos_task = PythonOperator(
        task_id="drop_viajes_eventos",
        python_callable=ddl_query("drop", "viajes_eventos.sql"),
    )
    drop_viajes_task = PythonOperator(
        task_id="drop_viajes",
        python_callable=ddl_query("drop", "viajes.sql"),
    )
    drop_usuarios_task = PythonOperator(
        task_id="drop_usuarios",
        python_callable=ddl_query("drop", "usuarios.sql"),
    )
    drop_drivers_task = PythonOperator(
        task_id="drop_drivers",
        python_callable=ddl_query("drop", "drivers.sql"),
    )

    try_redshift_connection_task >> drop_clima_task
    try_redshift_connection_task >> drop_viajes_eventos_task >> drop_viajes_task
    drop_viajes_task >> [drop_drivers_task, drop_usuarios_task]
