import datetime as dt
import numpy as np
import pandas as pd

from code.utils import random_categories_array
from options import TIPO_CATEGORIA, TIPO_PREMIUM, TRIP_END


def get_random_names(ref: str, n: int) -> pd.Series:
    assert ref in {"apellidos", "hombres", "mujeres"}
    df = pd.read_csv(f"mock/{ref}.csv")
    col = "apellido" if ref == "apellidos" else "nombre"
    return df[col].sample(n, replace=True)


def get_random_provs(n: int) -> pd.DataFrame:
    df = pd.read_csv("tables/provincias.csv")
    return df["nombre"].sample(n, replace=True)


def mock_persons(n: int, m_thresh: float, f_thresh: float) -> pd.DataFrame:
    """Create information for n mock persons.
    M = Male (man), F = Female (woman), X = Non-binary.
    """
    assert 0.00 < m_thresh
    assert m_thresh < f_thresh
    assert f_thresh < 1.00

    df = pd.DataFrame(
        random_categories_array(n, {"M": m_thresh, "F": f_thresh, "X": 1.00}, cat_to_id=None),
        columns=["genero"],
    )
    non_binary_w_men_name = np.random.random(n)

    mixed_names = np.vstack(
        (
            get_random_names("mujeres", n).values,
            get_random_names("hombres", n).values,
        )
    )
    assert mixed_names.shape == (2, n), f"Invalid shape: {mixed_names.shape} (exp={(2, n)})"
    mask_man = np.logical_or(
        (df["genero"] == "M").values,
        non_binary_w_men_name,
    ).astype(int)

    nombre = mixed_names[mask_man, np.arange(n)]
    assert nombre.shape == (n,), f"Invalid shape: {nombre.shape} (exp={(n,)})"
    df["nombre"] = nombre

    df["apellido"] = get_random_names("apellidos", n).values

    df.loc[:, "fecha_registro"] = "2024-01-01"

    df.loc[:, "fecha_bloqueo"] = None
    mask_blocked = (np.random.random(n)) < 0.15
    df.loc[mask_blocked, "fecha_bloqueo"] = "2024-01-03"

    return df


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
            + pd.to_timedelta(np.random.randint(0, 119, n), unit="days")
            + pd.to_timedelta(np.random.randint(0, 23, n), unit="hr")
            + pd.to_timedelta(np.random.randint(0, 59, n), unit="min")
            + pd.to_timedelta(np.random.randint(0, 59, n), unit="sec")
        ),
        columns=["tiempo_inicio"],
    )

    df.loc[:, "duracion_viaje_seg"] = None
    df.loc[:, "tiempo_fin"] = None
    df["duracion_viaje_seg"][closed_col] = np.random.randint(120, 3600, closed_col.sum())
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
    df["precio_bruto_usuario"] = df["precio_bruto_usuario"] * (1.00 + is_comfort.astype(int) * 0.15)
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


# * Item mock functions


def mock_drivers_f(n: int) -> pd.DataFrame:
    """Create information for a mock driver."""
    df = mock_persons(n, m_thresh=0.80, f_thresh=0.99)

    df["id"] = np.random.randint(0, 100_000_000, size=n)
    df["direccion_altura"] = np.random.randint(1, 10_000, size=n)
    df.loc[:, "direccion_ciudad"] = "CIUDAD"
    df.loc[:, "direccion_calle"] = "CALLE"
    df["categoria"] = random_categories_array(
        n,
        {TIPO_CATEGORIA.STANDARD: 0.9, TIPO_CATEGORIA.COMFORT: 1.0},
        cat_to_id=None,
    )
    df["direccion_provincia"] = get_random_provs(n).values

    return df


def mock_usuarios_f(n: int) -> pd.DataFrame:
    """Create information for a mock user."""
    df = mock_persons(n, m_thresh=0.65, f_thresh=0.99)

    df["id"] = np.random.randint(0, 100_000_000, size=n)
    df["tipo_premium"] = random_categories_array(
        n,
        {
            TIPO_PREMIUM.STANDARD: 0.80,
            TIPO_PREMIUM.GOLD: 0.90,
            TIPO_PREMIUM.BLACK: 0.99,
            TIPO_PREMIUM.CORTESIA: 1.00,
        },
        cat_to_id=None,
    )

    return df


def mock_viajes_f(is_comfort: pd.Series) -> pd.DataFrame:
    """Create information for a mock trip."""
    n = is_comfort.size

    df = pd.concat(
        (
            mock_geolocations(n),
            mock_ends(n),
        ),
        axis=1,
    )
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
