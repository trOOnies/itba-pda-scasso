"""Tasks for creating mock data in Redshift."""

import logging
import pandas as pd
from sqlalchemy import create_engine

from code.database import REDSHIFT_CONN_STR
from code.database_funcs import check_mock_is_full
from code.mock_data_redshift import (
    mock_base,
    mock_clima_hlf,
    mock_viajes_hlf,
    mock_viajes_eventos_hlf,
    save_mock,
    save_to_sql,
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

    viajes = mock_viajes_hlf(path_usuarios, path_drivers)

    path = save_mock(viajes, "viajes", engine)

    logging.info("[END] MOCK VIAJES")
    return path


def mock_viajes_eventos(**kwargs) -> None:
    """Mock trips events table."""
    logging.info("[START] MOCK VIAJES_EVENTOS")

    engine = create_engine(REDSHIFT_CONN_STR)
    path = check_mock_is_full(engine, "viajes_eventos")
    if path is not None:
        return

    path_viajes = kwargs["ti"].xcom_pull(task_ids="mock_viajes")
    eventos = mock_viajes_eventos_hlf(path_viajes)

    save_mock(eventos, "viajes_eventos", engine)
    logging.info("[END] MOCK VIAJES_EVENTOS")


def mock_clima_id() -> str:
    logging.info("[START] MOCK CLIMA_ID")

    engine = create_engine(REDSHIFT_CONN_STR)
    path = check_mock_is_full(engine, "clima_id", is_fixed_table=True)
    if path is not None:
        return path

    path = "tables/clima_id.csv"
    df = pd.read_csv(path)
    save_to_sql(df, "clima_id", engine)

    logging.info("[END] MOCK CLIMA_ID")

    return path


def mock_clima(**kwargs):
    logging.info("[START] MOCK CLIMA")

    engine = create_engine(REDSHIFT_CONN_STR)
    path = check_mock_is_full(engine, "clima", is_fixed_table=True)
    if path is not None:
        return

    path_clima_id = kwargs["ti"].xcom_pull(task_ids="mock_clima_id")
    clima = mock_clima_hlf(path_clima_id)

    save_mock(clima, "clima", engine)
    logging.info("[END] MOCK CLIMA")
