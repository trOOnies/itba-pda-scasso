"""Functions for creating mock data in Redshift."""

import logging
import numpy as np
import os
import pandas as pd
from random import randint
from sqlalchemy import create_engine

from code.database import REDSHIFT_CONN_STR
from code.database_funcs import check_mock_is_full
from code.mock_rows import mock_viajes_f
from code.utils import random_categories_array
from options import TIPO_CATEGORIA, TRIP_END

logger = logging.getLogger(__name__)


# * Generic functions


def save_mock(df: pd.DataFrame, table_name: str, engine) -> str:
    """Save mock data to local CSV folder and to SQL."""
    logging.info(f"Loading transformed data in table {table_name}...")
    path = f"local/mocked_{table_name}.csv"

    df.to_csv(path, index=False)

    chunksize = 1_000
    for ix in range(0, df.shape[0], chunksize):
        df.iloc[ix:min(ix + chunksize, df.shape[0])].to_sql(
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
) -> str:
    """Base function for the mocking of a table."""
    logging.info(f"[START] MOCK {table_name.upper()}")

    engine = create_engine(REDSHIFT_CONN_STR)
    path = check_mock_is_full(engine, table_name)
    if path is not None:
        return path

    logging.info(f"Creando items para {table_name}...")
    n_items = randint(n_min, n_max)
    df = mock_f(n_items)
    logging.info(f"Items para {table_name} creados.")

    path = save_mock(df, table_name, engine)

    logging.info(f"[END] MOCK {table_name.upper()}")
    return path


# * Trips


def get_usuarios(path: str) -> pd.DataFrame:
    """Get users for mock_viajes task.

    Base hyphotesis: 75% of users rode at least once with Mentre.
    """
    usuarios = pd.read_csv(path, usecols=["id"])
    assert not usuarios.empty
    usuarios = usuarios.sample(frac=0.75, replace=False)
    return usuarios


def preprocess_usuarios(usuarios: pd.DataFrame, n_extra_viajes: int) -> pd.DataFrame:
    """Preprocess users for mock_viajes task.

    Base hyphotesis: some users account for more than 1 trip.
    """
    usuarios = pd.concat(
        (
            usuarios[["id"]],
            usuarios[["id"]].sample(n=n_extra_viajes, replace=True),
        ),
        ignore_index=True,
        axis=0,
    ).sample(frac=1, replace=False)
    usuarios = usuarios.rename({"id": "usuario_id"}, axis=1)
    return usuarios


def get_preprocess_drivers(path: str, total_viajes: int) -> pd.DataFrame:
    """Get and preprocess drivers for mock_viajes task.

    Base hyphotesis: almost every driver drove at least once with Mentre.
    Take into account that we compensate n_zero rows because we need (n + n_extra_viajes) rows.
    """
    drivers = pd.read_csv(path, usecols=["id", "categoria"])
    assert not drivers.empty

    drivers["is_comfort"] = drivers["categoria"] == TIPO_CATEGORIA.COMFORT
    drivers = drivers.drop("categoria", axis=1)

    n_with_trips = int(0.95 * drivers.shape[0])
    drivers = drivers.sample(n=n_with_trips, replace=False)

    drivers = pd.concat(
        (
            drivers,
            drivers.sample(n=(total_viajes - n_with_trips), replace=True),
        ),
        ignore_index=True,
        axis=0,
    ).sample(frac=1, replace=False)
    drivers = drivers.rename({"id": "driver_id"}, axis=1)

    return drivers


def merge_drivers_usuarios(drivers: pd.DataFrame, usuarios: pd.DataFrame) -> pd.DataFrame:
    """Merge drivers and users for the mock_viajes task."""
    assert drivers.shape[0] == usuarios.shape[0], (
        f"The rows don't match: {drivers.shape[0]} in drivers vs. {usuarios.shape[0]} in usuarios"
    )
    viajes = pd.concat((drivers, usuarios), axis=1)
    viajes = pd.concat(
        (
            viajes[["driver_id", "usuario_id"]],
            mock_viajes_f(viajes["is_comfort"]),
        ),
        axis=1,
    )
    viajes = viajes.sort_values("tiempo_inicio", ignore_index=True)
    viajes["id"] = range(viajes.shape[0])
    return viajes


# * Trip events


def get_viajes(path: str) -> pd.DataFrame:
    viajes = pd.read_csv(
        path,
        usecols=(
            ["id"]
            + TRIP_END.ALL_CLOSED
            + ["tiempo_inicio", "duracion_viaje_seg", "tiempo_fin"]
        ),
    )
    assert not viajes.empty, "'viajes' local CSV is empty."

    viajes["last_evento_id"] = pd.from_dummies(
        viajes[TRIP_END.ALL_CLOSED],
        default_category=TRIP_END.ABIERTO,
    )
    viajes = viajes.drop(TRIP_END.ALL_CLOSED, axis=1)
    viajes["last_evento_id"] = viajes["last_evento_id"].map(TRIP_END.TO_EVENTO_ID).astype(int)
    viajes["tiempo_inicio"] = pd.to_datetime(viajes["tiempo_inicio"])
    viajes["tiempo_fin"] = pd.to_datetime(viajes["tiempo_fin"])

    return viajes


def get_eventos_start(viajes: pd.DataFrame) -> pd.DataFrame:
    """Get ride start events."""
    viajes_len = viajes.shape[0]
    assert viajes_len > 0
    df = pd.concat(
        (
            viajes["id"].rename("id_viaje").reset_index(drop=True),
            pd.Series(
                np.zeros(viajes_len, dtype=int),
                name="id_ord",
            ),
            pd.Series(
                TRIP_END.event_array(viajes_len, TRIP_END.ABIERTO),
                name="evento_id",
            ),
            viajes["tiempo_inicio"].rename("tiempo_evento").reset_index(drop=True),
        ),
        axis=1,
    )
    assert df.notnull().all(None)
    return df


def get_end_canceled_rides(viajes: pd.DataFrame) -> pd.DataFrame:
    """Get ride cancelation events."""
    eventos_not_ok_final = {}
    for event in TRIP_END.ALL_CLOSED_NOT_OK:
        viajes_i = viajes[
            viajes["last_evento_id"] == TRIP_END.TO_EVENTO_ID[event]
        ]

        viajes_i_len = viajes_i.shape[0]
        if viajes_i_len == 0:
            continue

        eventos_not_ok_final[event] = pd.concat(
            (
                viajes_i["id"].rename("id_viaje").reset_index(drop=True),
                pd.Series(
                    np.ones(viajes_i_len, dtype=int),
                    name="id_ord",
                ),
                pd.Series(
                    TRIP_END.event_array((viajes_i_len,), event),
                    name="evento_id",
                ),
                (
                    viajes_i["tiempo_inicio"]
                    + pd.to_timedelta(
                        np.random.randint(15, 1200, viajes_i_len).reshape((-1,)),
                        unit="sec",
                    )
                ).rename("tiempo_evento").reset_index(drop=True),
            ),
            axis=1,
        )
        del viajes_i

    end_canceled_rides = pd.concat(
        list(eventos_not_ok_final.values()),
        axis=0,
    )
    assert not end_canceled_rides.empty
    assert end_canceled_rides.shape[1] == 4
    assert end_canceled_rides.notnull().all(None)
    return end_canceled_rides


def split_viajes_cerrado(viajes: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Closed rides: They can either be corrected as closed, or directly closed (the usual)."""
    mask = viajes["last_evento_id"] == TRIP_END.TO_EVENTO_ID[TRIP_END.CERRADO]
    viajes_cerrado = viajes[mask]
    assert not viajes_cerrado.empty, "No closed trips were found."

    mask_corrected_as_cerrado = np.random.random(size=viajes_cerrado.shape[0]) < 0.10
    return (
        viajes_cerrado[mask_corrected_as_cerrado],
        viajes_cerrado[~mask_corrected_as_cerrado],
    )


def get_eventos_uncorrected(viajes_cerrado_corrected: pd.DataFrame) -> pd.DataFrame:
    """Get ride cancelation events that later ended up in closed rides."""
    viajes_cerrado_corrected_len = viajes_cerrado_corrected.shape[0]
    assert viajes_cerrado_corrected_len > 0
    df = pd.concat(
        (
            viajes_cerrado_corrected["id"].rename("id_viaje").reset_index(drop=True),
            pd.Series(
                np.ones(viajes_cerrado_corrected_len, dtype=int),
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
                    cat_to_id=TRIP_END.TO_EVENTO_ID,
                ),
                name="evento_id",
            ),
            (
                viajes_cerrado_corrected["tiempo_inicio"]
                + pd.to_timedelta(viajes_cerrado_corrected["duracion_viaje_seg"] / 2, unit="sec")
            ).rename("tiempo_evento").reset_index(drop=True),
        ),
        axis=1,
    )
    assert df.notnull().all(None)
    return df


def get_eventos_corrected(viajes_cerrado_corrected: pd.DataFrame) -> pd.DataFrame:
    """Get ride end events that are corrections after a ride cancelation."""
    viajes_cerrado_corrected_len = viajes_cerrado_corrected.shape[0]
    assert viajes_cerrado_corrected_len > 0
    df = pd.concat(
        (
            viajes_cerrado_corrected["id"].rename("id_viaje").reset_index(drop=True),
            pd.Series(
                np.full(viajes_cerrado_corrected_len, 2, dtype=int),
                name="id_ord",
            ),
            pd.Series(
                TRIP_END.event_array((viajes_cerrado_corrected_len,), TRIP_END.CERRADO),
                name="evento_id",
            ),
            viajes_cerrado_corrected["tiempo_fin"].rename("tiempo_evento").reset_index(drop=True),
        ),
        axis=1,
    )
    assert df.notnull().all(None)
    return df


def get_eventos_end(viajes_cerrado: pd.DataFrame) -> pd.DataFrame:
    """Get ride end normal events, that is, straight from ride start to ride end
    without any other event.
    """
    viajes_cerrado_len = viajes_cerrado.shape[0]
    assert viajes_cerrado_len > 0
    df = pd.concat(
        (
            viajes_cerrado["id"].rename("id_viaje").reset_index(drop=True),
            pd.Series(
                np.ones(viajes_cerrado_len, dtype=int),
                name="id_ord",
            ),
            pd.Series(
                TRIP_END.event_array((viajes_cerrado_len,), TRIP_END.CERRADO),
                name="evento_id",
            ),
            viajes_cerrado["tiempo_fin"].rename("tiempo_evento").reset_index(drop=True),
        ),
        axis=1,
    )
    assert df.notnull().all(None)
    return df
