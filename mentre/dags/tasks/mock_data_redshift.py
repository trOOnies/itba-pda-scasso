"""Tasks for creating mock data in Redshift."""

import logging
import pandas as pd
from random import randint
from sqlalchemy import create_engine

from code.database import REDSHIFT_CONN_STR
from code.database_funcs import check_mock_is_full
from code.mock_data_redshift import (
    get_preprocess_drivers,
    get_end_canceled_rides,
    get_eventos_corrected,
    get_eventos_end,
    get_eventos_start,
    get_eventos_uncorrected,
    get_usuarios,
    get_viajes,
    merge_drivers_usuarios,
    mock_base,
    preprocess_usuarios,
    save_mock,
    split_viajes_cerrado,
)
from code.mock_rows import mock_drivers_f, mock_usuarios_f

logger = logging.getLogger(__name__)


def mock_drivers() -> str:
    """Mock drivers table."""
    return mock_base(
        "drivers",
        n_min=100,
        n_max=300,
        mock_f=mock_drivers_f,
    )


def mock_usuarios() -> str:
    """Mock users table."""
    return mock_base(
        "usuarios",
        n_min=5_000,
        n_max=10_000,
        mock_f=mock_usuarios_f,
    )


def mock_viajes(**kwargs) -> str:
    """Mock trips table."""
    logging.info("[START] MOCK VIAJES")

    engine = create_engine(REDSHIFT_CONN_STR)
    path = check_mock_is_full(engine, "viajes")
    if path is not None:
        return path

    path_drivers = kwargs["ti"].xcom_pull(task_ids="mock_drivers")
    path_usuarios = kwargs["ti"].xcom_pull(task_ids="mock_usuarios")

    usuarios = get_usuarios(path_usuarios)
    n_extra_viajes = randint(usuarios.shape[0], 2 * usuarios.shape[0])
    logging.info(f"usuarios: {usuarios.shape[0]}")
    logging.info(f"n_extra_viajes: {n_extra_viajes}")

    usuarios = preprocess_usuarios(usuarios, n_extra_viajes)
    drivers = get_preprocess_drivers(path_drivers, usuarios.shape[0])

    viajes = merge_drivers_usuarios(drivers, usuarios)
    del drivers, usuarios

    path = save_mock(viajes, "viajes", engine)

    logging.info("[END] MOCK VIAJES")
    return path


def mock_viajes_eventos(**kwargs) -> None:
    """Mock trips events table."""
    logging.info("[START] MOCK VIAJES_EVENTOS")

    engine = create_engine(REDSHIFT_CONN_STR)
    path = check_mock_is_full(engine, "viajes_eventos")
    if path is not None:
        return path

    path_viajes = kwargs["ti"].xcom_pull(task_ids="mock_viajes")
    viajes = get_viajes(path_viajes)

    eventos_start = get_eventos_start(viajes)
    eventos_not_ok_final = get_end_canceled_rides(viajes)
    viajes_cerrado_corrected, viajes_cerrado = split_viajes_cerrado(viajes)

    eventos_uncorrected = get_eventos_uncorrected(viajes_cerrado_corrected)
    eventos_corrected = get_eventos_corrected(viajes_cerrado_corrected)

    eventos_end = get_eventos_end(viajes_cerrado)

    eventos = [eventos_start, eventos_not_ok_final, eventos_uncorrected, eventos_corrected, eventos_end]
    logging.info(f"Event DataFrames shapes: {[df.shape for df in eventos]}")
    eventos = pd.concat(eventos, axis=0).sort_values("tiempo_evento", ignore_index=True)
    del eventos_start, eventos_not_ok_final, eventos_uncorrected, eventos_corrected, eventos_end

    logging.info("Events formed and ready to be saved and loaded.")

    save_mock(eventos, "viajes_eventos", engine)
    logging.info("[END] MOCK VIAJES_EVENTOS")
