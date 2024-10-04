"""Functions for creating mock data in Redshift."""

import logging
import os
import pandas as pd
from random import randint
from sqlalchemy import create_engine

from code.database import REDSHIFT_CONN_STR
from code.database_funcs import execute_query
from code.mock_rows import mock_driver, mock_usuario

logger = logging.getLogger(__name__)


def mock_base(
    table_name: str,
    n_min: int,
    n_max: int,
    mock_f,
    cols: list[str],
):
    """Base function for the mocking of a table."""
    logging.info(f"[START] MOCK {table_name.upper()}")

    engine = create_engine(REDSHIFT_CONN_STR)
    # result = [t[0] for t in execute_query(engine, "select", "tables.sql").fetchall()]
    # assert "drivers" in result

    result = execute_query(
        engine,
        "select",
        "check_exists.sql",
        prev_kwargs={"DB_TABLE": table_name},
    )
    result = result.first()[0]
    assert not result, f"The table '{table_name}' exists but it's already full."

    n_items = randint(n_min, n_max)
    drivers = pd.DataFrame(
        [mock_f() for _ in range(n_items)],
        columns=cols,
    )

    drivers.to_sql(
        schema=os.environ["DB_SCHEMA"],
        name=table_name,
        con=engine,
        if_exists="replace",
        index=False,
        method="multi",
    )
    logging.info(f"Transformed data loaded into Redshift in table '{table_name}'")

    logging.info(f"[END] MOCK {table_name.upper()}")


def mock_drivers():
    mock_base(
        "drivers",
        n_min=100,
        n_max=300,
        mock_f=mock_driver,
        cols=[
            "id",
            "nombre",
            "apellido",
            "genero",
            "fecha_registro",
            "fecha_bloqueo",
            "direccion_provincia",
            "direccion_ciudad",
            "direccion_calle",
            "direccion_altura",
            "categoria",
        ],
    )


def mock_usuarios():
    mock_base(
        "usuarios",
        n_min=50_000,
        n_max=100_000,
        mock_f=mock_usuario,
        cols=[
            "id",
            "nombre",
            "apellido",
            "genero",
            "fecha_registro",
            "fecha_bloqueo",
            "tipo_premium",
        ],
    )


def mock_viajes_and_eventos():
    # Get a subset of the users and drivers
    # Then make a random union of the two so that we have (n_min, n_max) between them
    # Generate some randomness in the data
    # Generarte events as well, according to the previous step (can be a joined step)
    # Upload the viajes
    # Upload the eventos
    pass
