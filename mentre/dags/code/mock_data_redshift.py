"""Functions for creating mock data in Redshift."""

import datetime as dt
import logging
import numpy as np
import os
import pandas as pd
from random import randint
from sqlalchemy import create_engine

from code.database import REDSHIFT_CONN_STR
from code.database_funcs import execute_query
from code.mock_rows import mock_viaje
from code.utils import random_categories_array
from options import TIPO_CATEGORIA, TRIP_END

logger = logging.getLogger(__name__)


# * Generic functions


def save_mock(df: pd.DataFrame, table_name: str, engine) -> str:
    """Save mock data to local CSV folder and to SQL."""
    logging.info(f"Loading transformed data in table {table_name}...")
    path = f"local/mocked_{table_name}.csv"

    df.to_csv(path, index=False)
    df.to_sql(
        schema=os.environ["DB_SCHEMA"],
        name=table_name,
        con=engine,
        if_exists="append",
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

    
    logging.info(f"Creando items para {table_name}...")
    n_items = randint(n_min, n_max)
    df = mock_f(n_items)
    logging.info(f"Items para {table_name} creados.")

    path = save_mock(df, table_name, engine)

    logging.info(f"[END] MOCK {table_name.upper()}")
    return path


# * Specific functions


def get_usuarios(path: str) -> pd.DataFrame:
    """Get users for mock_viajes task."""
    usuarios = pd.read_csv(path, usecols=["id"])
    usuarios = usuarios.sample(frac=0.75, replace=False)
    return usuarios


def preprocess_usuarios(usuarios: pd.DataFrame, n_extra_viajes: int) -> pd.DataFrame:
    """Preprocess users for mock_viajes task."""
    usuarios = pd.concat(
        (
            usuarios["id"],
            usuarios["id"].sample(n=n_extra_viajes, replace=True),
        ),
        ignore_index=True,
    ).sample(frac=1, replace=False)
    usuarios = usuarios.rename({"id": "usuario_id"}, axis=1)
    return usuarios


def get_preprocess_drivers(path: str, n_extra_viajes: int) -> pd.DataFrame:
    """Get and preprocess drivers for mock_viajes task."""
    drivers = pd.read_csv(path, usecols=["id", "categoria"])
    drivers["is_comfort"] = drivers["categoria"] == TIPO_CATEGORIA.COMFORT
    drivers = drivers.drop("categoria", axis=1)
    drivers = drivers.sample(frac=0.95, replace=False)

    drivers = pd.concat(
        (
            drivers,
            drivers.sample(n=n_extra_viajes, replace=True),
        ),
        ignore_index=True,
    ).sample(frac=1, replace=False)
    drivers = drivers.rename({"id": "driver_id"}, axis=1)

    return drivers


def merge_drivers_usuarios(drivers: pd.DataFrame, usuarios: pd.DataFrame) -> pd.DataFrame:
    """Merge drivers and users for the mock_viajes task."""
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
    return viajes


def get_eventos_start(viajes: pd.DataFrame) -> pd.DataFrame:
    """Get ride start events."""
    viajes_len = viajes.shape[0]
    return pd.concat(
        (
            viajes["id"].rename("id_viaje"),
            pd.Series(
                np.zeros_like(viajes_len, dtype=int),
                name="id_ord",
            ),
            pd.Series(
                TRIP_END.event_array(viajes_len, TRIP_END.ABIERTO),
                name="evento_id",
            ),
            viajes["tiempo_inicio"].rename("tiempo_evento"),
        ),
        ignore_index=True,
    )


def get_end_canceled_rides(viajes: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Get ride cancelation events."""
    eventos_not_ok_final = {}
    for event in TRIP_END.ALL_CLOSED_NOT_OK:
        viajes_i = viajes[
            viajes["last_evento_id"] == TRIP_END.TO_EVENTO_ID[event]
        ]

        viajes_i_len = viajes_i.shape[0]
        eventos_not_ok_final[event] = pd.concat(
            (
                viajes_i["id"].rename("id_viaje"),
                pd.Series(
                    np.ones_like(viajes_i_len, dtype=int),
                    name="id_ord",
                ),
                pd.Series(
                    TRIP_END.event_array(viajes_i_len, event),
                    name="evento_id",
                ),
                (
                    viajes_i["tiempo_inicio"].rename("tiempo_evento")
                    + pd.Series(
                        [dt.timedelta(seconds=randint(15, 1200)) for _ in range(viajes_i_len)]
                    )
                ),
            ),
            ignore_index=True,
        )
        del viajes_i
    return eventos_not_ok_final


def split_viajes_cerrado(viajes: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Closed rides: They can either be corrected as closed, or directly closed (the usual)."""
    mask = viajes["last_evento_id"] == TRIP_END.TO_EVENTO_ID[TRIP_END.CERRADO]
    viajes_cerrado = viajes[mask]
    mask_corrected_as_cerrado = np.random.random(viajes_cerrado.shape[0]) < 0.10
    return (
        viajes_cerrado[mask_corrected_as_cerrado],
        viajes_cerrado[~mask_corrected_as_cerrado],
    )


def get_eventos_uncorrected(viajes_cerrado_corrected: pd.DataFrame) -> pd.DataFrame:
    """Get ride cancelation events that later ended up in closed rides."""
    viajes_cerrado_corrected_len = viajes_cerrado_corrected.shape[0]
    return pd.concat(
        (
            viajes_cerrado_corrected["id"].rename("id_viaje"),
            pd.Series(
                np.ones_like(viajes_cerrado_corrected_len, dtype=int),
                name="id_ord",
            ),
            pd.Series(
                random_categories_array(
                    viajes_cerrado_corrected_len,
                    {
                        TRIP_END.CANCELADO_USUARIO: 0.25,
                        TRIP_END.CANCELADO_DRIVER: 0.50,
                        TRIP_END.CANCELADO_MENTRE: 0.75,
                        TRIP_END.OTROS: 1.00,
                    },
                    TRIP_END.TO_EVENTO_ID,
                ),
                name="evento_id",
            ),
            (
                viajes_cerrado_corrected["tiempo_inicio"]
                + dt.timedelta(seconds=viajes_cerrado_corrected["duracion_viaje"] / 2)
            ).rename("tiempo_evento"),
        ),
        ignore_index=True,
    )


def get_eventos_corrected(viajes_cerrado_corrected: pd.DataFrame) -> pd.DataFrame:
    """Get ride end events that are corrections after a ride cancelation."""
    viajes_cerrado_corrected_len = viajes_cerrado_corrected.shape[0]
    return pd.concat(
        (
            viajes_cerrado_corrected["id"].rename("id_viaje"),
            pd.Series(
                np.full_like(viajes_cerrado_corrected_len, 2, dtype=int),
                name="id_ord",
            ),
            pd.Series(
                TRIP_END.event_array(viajes_cerrado_corrected_len, TRIP_END.CERRADO),
                name="evento_id",
            ),
            viajes_cerrado_corrected["tiempo_fin"].rename("tiempo_evento"),
        ),
        ignore_index=True,
    )


def get_eventos_end(viajes_cerrado: pd.DataFrame) -> pd.DataFrame:
    """Get ride end normal events, that is, straight from ride start to ride end
    without any other event.
    """
    viajes_cerrado_len = viajes_cerrado.shape[0]
    return pd.concat(
        (
            viajes_cerrado["id"].rename("id_viaje"),
            pd.Series(
                np.ones_like(viajes_cerrado_len, dtype=int),
                name="id_ord",
            ),
            pd.Series(
                TRIP_END.event_array(viajes_cerrado_len, TRIP_END.CERRADO),
                name="evento_id",
            ),
            viajes_cerrado["tiempo_fin"].rename("tiempo_evento"),
        ),
        ignore_index=True,
    )
