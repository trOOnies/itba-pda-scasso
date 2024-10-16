import datetime as dt
import logging
import numpy as np
from random import randint
import pandas as pd
from sqlalchemy import create_engine

from code.database import REDSHIFT_CONN_STR
from code.database_funcs import get_max_id
from code.utils import random_categories_array
from options import TIPO_CATEGORIA, TRIP_END


def mock_geolocations(n: int) -> pd.DataFrame:
    """Mock geolocation variables."""
    df = pd.DataFrame(
        10.0 * np.random.random((n, 4)),
        columns=["origen_lat", "origen_long", "destino_lat", "destino_long"],
    )

    # Example function for distance (not accurate)
    df["distancia_metros"] = np.sqrt(
        np.square(df["destino_lat"] - df["origen_lat"])
        + np.square(df["destino_long"] - df["origen_long"])
    ).astype(int)

    return df


def mock_ends(n: int) -> pd.DataFrame:
    """Mock trip ending related variables."""
    df = pd.DataFrame(
        random_categories_array(
            n,
            {
                TRIP_END.ABIERTO: 0.01,
                TRIP_END.CERRADO: 0.75,
                TRIP_END.CANCELADO_USUARIO: 0.90,
                TRIP_END.CANCELADO_DRIVER: 0.95,
                TRIP_END.CANCELADO_MENTRE: 0.98,
                TRIP_END.OTROS: 1.00,
            },
            cat_to_id=None,
        ),
        columns=["end_cat"],
    )

    df = pd.get_dummies(df)
    df = df.rename({c: c[8:] for c in df.columns}, axis=1)  # delete 'end_cat_' prefix

    if TRIP_END.ABIERTO in df.columns:
        df = df.drop(TRIP_END.ABIERTO, axis=1)
    for col in TRIP_END.ALL_CLOSED:
        if col not in df.columns:
            df.loc[:, col] = False

    df["fue_facturado"] = np.logical_and(
        df[TRIP_END.CERRADO],
        np.random.random(n) < 0.95,
    )

    return df


def mock_timestamps(closed_col: pd.Series) -> pd.DataFrame:
    """Mock timestamp related variables."""
    n = closed_col.size

    df = pd.DataFrame(
        (
            np.full(n, dt.datetime(2024, 1, 1, 0, 0, 0, tzinfo=dt.timezone.utc))
            + pd.to_timedelta(np.random.randint(0, 120, n), unit="days")
            + pd.to_timedelta(np.random.randint(0, 24, n), unit="hr")
            + pd.to_timedelta(np.random.randint(0, 60, n), unit="min")
            + pd.to_timedelta(np.random.randint(0, 60, n), unit="sec")
        ),
        columns=["tiempo_inicio"],
    )

    df.loc[:, "duracion_viaje_seg"] = None
    df.loc[:, "tiempo_fin"] = None
    df["duracion_viaje_seg"][closed_col] = np.random.randint(
        120,
        3601,
        closed_col.sum(),
    )
    df["tiempo_fin"][closed_col] = df["tiempo_inicio"][closed_col] + pd.to_timedelta(
        df["duracion_viaje_seg"][closed_col],
        unit="sec",
    )

    return df


def mock_financials(distance_m: pd.Series, is_comfort: pd.Series) -> pd.DataFrame:
    """Mock financial variables.

    Note that a ride has a flat rate of 1.0, and it also has a distance-dependent rate.
    """
    n = distance_m.size
    assert n > 0
    assert is_comfort.size == n

    df = pd.DataFrame(distance_m.rename("precio_bruto_usuario") * 1.50)
    df["precio_bruto_usuario"] = df["precio_bruto_usuario"] * (
        1.00 + is_comfort.astype(int) * 0.15
    )
    df["precio_bruto_usuario"] += 1.0

    df["descuento_pc"] = random_categories_array(
        n,
        {0.00: 0.7500, 0.15: 0.9500, 0.50: 0.9900, 1.00: 1.0000},
        cat_to_id=None,
    )
    df["descuento"] = df["descuento_pc"] * df["precio_bruto_usuario"]
    del df["descuento_pc"]

    df["precio_neto_usuario"] = df["precio_bruto_usuario"] - df["descuento"]

    df["mentre_margin_pc"] = 0.15 + (np.random.random(size=n) / 10.0)
    df["margen_mentre"] = df["precio_neto_usuario"] * df["mentre_margin_pc"]
    df["comision_driver"] = df["precio_neto_usuario"] - df["margen_mentre"]
    del df["mentre_margin_pc"]

    return df


# * Middle level functions


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


def mock_viajes_f(is_comfort: pd.Series) -> pd.DataFrame:
    """Create information for a mock trip."""
    n = is_comfort.size

    df = pd.concat((mock_geolocations(n), mock_ends(n)), axis=1)
    assert df.shape[0] == n

    df = pd.concat(
        (
            df,
            mock_timestamps(df[TRIP_END.CERRADO]),
            mock_financials(df["distancia_metros"], is_comfort),
        ),
        axis=1,
    )
    assert df.shape[0] == n

    return df


def merge_drivers_usuarios(
    drivers: pd.DataFrame,
    usuarios: pd.DataFrame,
    max_id: int | None,
) -> pd.DataFrame:
    """Merge drivers and users for the mock_viajes task."""
    d = drivers.shape[0]
    u = usuarios.shape[0]
    assert d == u, f"The rows don't match: {d} in drivers vs. {u} in usuarios"

    viajes = pd.concat((drivers, usuarios), axis=1)
    viajes = pd.concat(
        (
            viajes[["driver_id", "usuario_id"]],
            mock_viajes_f(viajes["is_comfort"]),
        ),
        axis=1,
    )

    viajes = viajes.sort_values("tiempo_inicio", ignore_index=True)
    viajes["id"] = (
        range(viajes.shape[0])
        if max_id is None
        else range(max_id + 1, max_id + 1 + viajes.shape[0])
    )

    return viajes


# * High level function


def mock_viajes_hlf(path_usuarios: str, path_drivers: str, append: bool = False) -> pd.DataFrame:
    """High level function for the mocking of viajes."""
    if append:
        engine = create_engine(REDSHIFT_CONN_STR)
        max_id = get_max_id(engine, "viajes")
    else:
        max_id = None

    usuarios = get_usuarios(path_usuarios)
    n_extra_viajes = randint(usuarios.shape[0], 2 * usuarios.shape[0])  # ! both inclusive
    logging.info(f"usuarios: {usuarios.shape[0]}")
    logging.info(f"n_extra_viajes: {n_extra_viajes}")

    usuarios = preprocess_usuarios(usuarios, n_extra_viajes)
    drivers = get_preprocess_drivers(path_drivers, usuarios.shape[0])

    viajes = merge_drivers_usuarios(drivers, usuarios, max_id=max_id)
    del drivers, usuarios

    return viajes
