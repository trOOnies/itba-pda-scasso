import datetime as dt
from math import sqrt
import numpy as np
import pandas as pd
from random import randint, random

from code.utils import random_categories_array
from options import TIPO_CATEGORIA, TIPO_PREMIUM, TRIP_END


def get_random_names(ref: str, n: int) -> pd.DataFrame:
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
        random_categories_array(n, {"M": m_thresh, "F": f_thresh, "X": 1.00}),
        columns=["genero"],
    )
    non_binary_w_men_name = np.random.random(n)

    mixed_names = np.hstack(
        (
            get_random_names("mujeres", n),
            get_random_names("hombres", n),
        )
    )
    assert mixed_names.shape[1] == 2
    mask_man = np.logical_or(
        (df["genero"] == "M").values,
        non_binary_w_men_name,
    ).astype(int)

    df["nombre"] = mixed_names[:, mask_man]
    df["apellido"] = get_random_names("apellidos", n)

    df["fecha_registro"] = np.full(n, "2024-01-01", dtype=str)

    df["fecha_bloqueo"] = np.full(n, "2024-02-01", dtype=str)
    mask_blocked = (np.random.random(n)) < 0.15
    df["fecha_bloqueo"][mask_blocked] = None

    return df


def mock_geolocations() -> tuple[dict, int]:
    """Mock geolocation variables."""
    locs = {
        "origen_lat": 10.0 * random(),
        "origen_long": 10.0 * random(),
        "destino_lat": 10.0 * random(),
        "destino_long": 10.0 * random(),
    }
    distancia_metros = int(sqrt(
        (locs["destino_lat"] - locs["origen_lat"])**2
        + (locs["destino_long"] - locs["origen_long"])**2
    ))
    return locs, distancia_metros


def mock_end() -> tuple[str, list[bool], bool]:
    """Mock trip ending related variables."""
    end_cat = random_category(
        {
            TRIP_END.ABIERTO: 0.01,
            TRIP_END.CERRADO: 0.75,
            TRIP_END.CANCELADO_USUARIO: 0.90,
            TRIP_END.CANCELADO_DRIVER: 0.95,
            TRIP_END.CANCELADO_MENTRE: 0.98,
            TRIP_END.OTROS: 1.00,
        }
    )
    was_charged = (
        random() < 0.95
        if end_cat == TRIP_END.CERRADO
        else False
    )
    return end_cat, [end_cat == te for te in TRIP_END.ALL_CLOSED], was_charged


def mock_timestamps(end_cat: str) -> tuple[dt.datetime, dt.datetime | None, int | None]:
    """Mock timestamp related variables."""
    tiempo_inicio = (
        dt.datetime(2024, 1, 1, 0, 0, 0, tzinfo=dt.timezone.utc)
        + dt.timedelta(days=randint(0, 120))
        + dt.timedelta(hours=randint(0, 23))
        + dt.timedelta(minutes=randint(0, 59))
        + dt.timedelta(seconds=randint(0, 59))
    )

    if end_cat == TRIP_END.CERRADO:
        duracion_viaje_seg = randint(120, 3600)
        tiempo_fin = tiempo_inicio + dt.timedelta(seconds=duracion_viaje_seg)
    else:
        duracion_viaje_seg = None
        tiempo_fin = None

    return [tiempo_inicio, tiempo_fin, duracion_viaje_seg]


def mock_financial(distance_m: int, is_comfort: bool) -> list[float]:
    base_price = distance_m * 1.50
    if is_comfort:
        base_price *= 1.15

    discount_pc = random_category(
        {0.00: 0.7500, 0.15: 0.9500, 0.50: 0.9900, 1.00: 1.0000}
    )
    discount = discount_pc * base_price
    net_price = base_price - discount

    mentre_margin_pc = 0.15 + (random() / 10)

    return [
        base_price,                            # precio_bruto_usuario
        discount,                              # descuento
        net_price,                             # precio_neto_usuario
        net_price * (1.00 - mentre_margin_pc), # comision_driver
        net_price * mentre_margin_pc,          # margen_mentre
    ]


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
    )
    df["direccion_provincia"] = get_random_provs(n)

    return df


def mock_usuario_f(n: int) -> pd.DataFrame:
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
    )

    return df


def mock_viaje(is_comfort: bool) -> list:
    """Create information for a mock trip."""
    geo_locs, distancia_metros = mock_geolocations()

    end_cat, cats_bools, fue_facturado = mock_end()

    return (
        [
            geo_locs["origen_lat"],              # origen_lat
            geo_locs["origen_long"],             # origen_long
            geo_locs["destino_lat"],             # destino_lat
            geo_locs["destino_long"],            # destino_long
            distancia_metros,                    # distancia_metros
        ]
        + cats_bools  # end_cerrado, end_cancelado_usuario, end_cancelado_driver, end_cancelado_mentre, end_otros
        + [fue_facturado]  # fue_facturado
        + mock_timestamps(end_cat)  # tiempo_inicio, tiempo_fin, duracion_viaje_seg
        + mock_financial(distancia_metros, is_comfort)  # precio_bruto_usuario, descuento, precio_neto_usuario, comision_driver, margen_mentre
    )
