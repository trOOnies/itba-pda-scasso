"""Functions for creating mock data in Redshift."""

import logging
import os
import numpy as np
import pandas as pd
from random import randint
from sqlalchemy import create_engine

from code.database import REDSHIFT_CONN_STR
from code.database_funcs import execute_query
from code.mock_rows import mock_driver, mock_usuario, mock_viaje
from options import TIPO_CATEGORIA, TRIP_END

logger = logging.getLogger(__name__)


def save_mock(df: pd.DataFrame, table_name: str, engine) -> str:
    """Save mock data to local CSV folder and to SQL."""
    path = f"dags/local/mocked_{table_name}.csv"
    df.to_csv(path, index=False)
    df.to_sql(
        schema=os.environ["DB_SCHEMA"],
        name=table_name,
        con=engine,
        if_exists="replace",
        index=False,
        method="multi",
    )
    logging.info(f"Transformed data loaded into Redshift in table '{table_name}'")
    return path


def mock_base(
    table_name: str,
    n_min: int,
    n_max: int,
    mock_f,
    cols: list[str],
) -> str:
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
    df = pd.DataFrame(
        [mock_f() for _ in range(n_items)],
        columns=cols,
    )

    path = save_mock(df, table_name, engine)

    logging.info(f"[END] MOCK {table_name.upper()}")
    return path


def mock_drivers() -> str:
    """Mock drivers table."""
    return mock_base(
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


def mock_usuarios() -> str:
    """Mock users table."""
    return mock_base(
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


def mock_viajes(**kwargs) -> str:
    """Mock trips table."""
    path_drivers = kwargs["ti"].xcom_pull(task_ids="mock_drivers")
    path_usuarios = kwargs["ti"].xcom_pull(task_ids="mock_usuarios")

    drivers = pd.read_csv(path_drivers, usecols=["id", "categoria"])
    drivers["is_comfort"] = drivers["categoria"] == TIPO_CATEGORIA.COMFORT
    drivers = drivers.drop("categoria", axis=1)
    drivers = drivers.sample(frac=0.95, replace=False)

    usuarios = pd.read_csv(path_usuarios, usecols=["id"])
    usuarios = usuarios.sample(frac=0.75, replace=False)

    n_extra_viajes = randint(usuarios.shape[0], 2 * usuarios.shape[0])

    drivers = pd.concat(
        (
            drivers,
            drivers.sample(n=n_extra_viajes, replace=True),
        ),
        ignore_index=True,
    ).sample(frac=1, replace=False)
    drivers = drivers.rename({"id": "driver_id"}, axis=1)

    usuarios = pd.concat(
        (
            usuarios["id"],
            usuarios["id"].sample(n=n_extra_viajes, replace=True),
        ),
        ignore_index=True,
    ).sample(frac=1, replace=False)
    usuarios = usuarios.rename({"id": "usuario_id"}, axis=1)

    viajes = pd.DataFrame([drivers, usuarios])
    viajes = pd.concat(
        (
            viajes[["driver_id", "usuario_id"]],
            pd.DataFrame(
                [
                    mock_viaje(is_comfort)
                    for is_comfort in viajes["is_comfort"]
                ],
                columns=[
                    "origen_lat",
                    "origen_long",
                    "destino_lat",
                    "destino_long",
                    "distancia_metros",
                    "end_cerrado",
                    "end_cancelado_usuario",
                    "end_cancelado_driver",
                    "end_cancelado_mentre",
                    "end_otros",
                    "fue_facturado",
                    "tiempo_inicio",
                    "tiempo_fin",
                    "duracion_viaje_seg",
                    "precio_bruto_usuario",
                    "descuento",
                    "precio_neto_usuario",
                    "comision_driver",
                    "margen_mentre",
                ],
            ),
        ),
        axis=1,
        ignore_index=True,
    )
    viajes = viajes.sort_values("tiempo_inicio", axis=1)
    viajes["id"] = range(viajes.shape[0])

    engine = create_engine(REDSHIFT_CONN_STR)
    path = save_mock(viajes, "viajes", engine)

    return path


def mock_viajes_eventos(**kwargs):
    """Mock trips events table."""
    path_viajes = kwargs["ti"].xcom_pull(task_ids="mock_viajes")
    viajes = pd.read_csv(
        path_viajes,
        usecols=["id"] + TRIP_END.ALL_CLOSED,
    )

    viajes["last_evento_id"] = pd.from_dummies(
        viajes[TRIP_END.ALL_CLOSED],
        default_category=TRIP_END.ABIERTO,
    )
    viajes = viajes.drop(TRIP_END.ALL_CLOSED, axis=1)

    eventos_start = pd.concat(
        (
            viajes["id"].rename("id_viaje"),
            pd.Series(
                np.zeros_like(viajes["id"].values, dtype=int),
                name="id_ord",
            ),
            pd.Series(
                np.full_like(
                    viajes["id"].values,
                    TRIP_END.TO_EVENTO_ID[TRIP_END.ABIERTO],
                    dtype=int,
                ),
                name="evento_id",
            ),
            viajes["tiempo_inicio"].rename("tiempo_evento"),
        ),
        ignore_index=True,
    )

    # TODO: Other kind of events, but event=last
    for ... in TRIP_END.ALL_CLOSED_NOT_OK:
        ...

    # TODO: Other kind of events, but event!=last
    # Take into account as well that there could have been further event changes,
    # like errors and that
    ...

    # TODO: Concat all eventos (excluding Cerrado)
    eventos = pd.concat(
        (
            eventos_start,
            ...,
        ),
        ignore_index=True,
    ).sort_values("tiempo_evento", ignore_index=True)

    # TODO: Cerrado
    viajes_cerrado = viajes[
        viajes["last_evento_id"] == TRIP_END.TO_EVENTO_ID[TRIP_END.CERRADO]
    ]
    eventos_end = pd.concat(
        (
            viajes_cerrado["id"].rename("id_viaje"),
            ...,  # TODO: get last id_ord for each one
            pd.Series(
                np.full_like(
                    viajes_cerrado["id"].values,
                    TRIP_END.TO_EVENTO_ID[TRIP_END.CERRADO],
                    dtype=int,
                ),
                name="evento_id",
            ),
            viajes_cerrado["tiempo_fin"].rename("tiempo_evento"),
        ),
        ignore_index=True,
    )

    # TODO: Concat with the others

    engine = create_engine(REDSHIFT_CONN_STR)
    save_mock(eventos, "viajes_eventos", engine)
