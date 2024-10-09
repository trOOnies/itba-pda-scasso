from math import isclose
import numpy as np
import pandas as pd

from code.utils import random_categories_array
from options import TIPO_CATEGORIA, TIPO_PREMIUM


def get_random_names(ref: str, n: int) -> pd.Series:
    assert ref in {"apellidos", "hombres", "mujeres"}
    df = pd.read_csv(f"mock/{ref}.csv")
    col = "apellido" if ref == "apellidos" else "nombre"
    return df[col].sample(n, replace=True)


def get_mask_man(gender_ser: pd.Series, men_name_nb_proba: float) -> np.ndarray:
    """Get mask that states if the person has a 'man's name'.

    This is done to take into account that non-binary people
    may or may not have one, and it's used in the people's
    mocking process.
    """
    is_male = (gender_ser == "M").values
    is_nb   = (gender_ser == "X").values

    # Border cases
    if isclose(men_name_nb_proba, 1.00):
        return np.logical_or(is_male, is_nb).astype(int)
    elif isclose(men_name_nb_proba, 0.00):
        return is_male.astype(int)

    # Inside cases
    assert (0.00 < men_name_nb_proba) and (men_name_nb_proba < 1.00), (
        "'men_name_nb_pc' value must be between 0.00 and 1.00 (both inclusive)."
    )

    n = gender_ser.shape[0]
    non_binary_w_men_name = np.random.random(n)

    return np.logical_or(
        is_male,
        np.logical_and(
            is_nb,
            (non_binary_w_men_name < men_name_nb_proba),
        ),
    ).astype(int)


def process_names(df: pd.DataFrame) -> np.ndarray:
    n = df.shape[0]
    
    mixed_names = np.vstack(
        (
            get_random_names("mujeres", n).values,
            get_random_names("hombres", n).values,
        )
    )
    assert mixed_names.shape == (2, n), f"Invalid shape: {mixed_names.shape} (exp={(2, n)})"

    mask_man = get_mask_man(df["genero"], 0.50)

    nombre = mixed_names[mask_man, np.arange(n)]
    assert nombre.shape == (n,), f"Invalid shape: {nombre.shape} (exp={(n,)})"

    return nombre


def process_dates(df: pd.DataFrame) -> pd.DataFrame:
    n = df.shape[0]
    df.loc[:, "fecha_registro"] = "2024-01-01"

    df.loc[:, "fecha_bloqueo"] = None
    mask_blocked = (np.random.random(n)) < 0.15
    df.loc[mask_blocked, "fecha_bloqueo"] = "2024-01-03"
    return df


def get_random_provs(n: int) -> pd.DataFrame:
    df = pd.read_csv("tables/provincias.csv")
    return df["nombre"].sample(n, replace=True)


# * Middle level functions


def mock_people(n: int, m_thresh: float, f_thresh: float) -> pd.DataFrame:
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
    df["nombre"] = process_names(df)
    df["apellido"] = get_random_names("apellidos", n).values

    df = process_dates(df)

    return df


def mock_ids(v_min: int, v_max: int, size: int) -> np.ndarray:
    """Mock non-repeating ids. Both ends are inclusive."""
    while True:
        ids = np.random.randint(v_min, v_max, size=size)
        if np.unique(ids).size == size:
            break
        del ids
    return ids


def mock_address(df: pd.DataFrame) -> pd.DataFrame:
    n = df.shape[0]

    df["direccion_altura"] = np.random.randint(1, 10_000, size=n)
    df.loc[:, "direccion_ciudad"] = "CIUDAD"
    df.loc[:, "direccion_calle"] = "CALLE"
    df["direccion_provincia"] = get_random_provs(n).values

    return df


# * High level functions


def mock_drivers_f(n: int) -> pd.DataFrame:
    """Create information for a mock driver."""
    df = mock_people(n, m_thresh=0.80, f_thresh=0.99)

    df["id"] = mock_ids(0, 100_000_000, size=n)
    df = mock_address(df)
    df["categoria"] = random_categories_array(
        n,
        {TIPO_CATEGORIA.STANDARD: 0.9, TIPO_CATEGORIA.COMFORT: 1.0},
        cat_to_id=None,
    )

    return df


def mock_usuarios_f(n: int) -> pd.DataFrame:
    """Create information for a mock user."""
    df = mock_people(n, m_thresh=0.65, f_thresh=0.99)

    df["id"] = mock_ids(0, 100_000_000, size=n)
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
