"""Tasks for creating mock data in Redshift."""

import pandas as pd
from sqlalchemy import create_engine

from code.database import REDSHIFT_CONN_STR
from code.database_funcs import check_mock_is_full
from code.mock import mock_base, save_mock, save_to_sql
from code.mock_clima import mock_clima_hlf
from code.mock_people import mock_drivers_f, mock_usuarios_f
from code.mock_viajes import mock_viajes_hlf
from code.mock_viajes_eventos import mock_viajes_eventos_hlf
from code.utils import start_end_log


@start_end_log("MOCK DRIVERS")
def mock_drivers() -> str:
    """Mock drivers table."""
    return mock_base(
        "drivers",
        n_min=100,
        n_max=300,
        mock_f=mock_drivers_f,
    )


@start_end_log("MOCK USUARIOS")
def mock_usuarios() -> str:
    """Mock users table."""
    return mock_base(
        "usuarios",
        n_min=5_000,
        n_max=10_000,
        mock_f=mock_usuarios_f,
    )


def get_mock_viajes(append: bool):
    @start_end_log("MOCK VIAJES")
    def mock_viajes(**kwargs) -> str:
        """Mock trips table."""
        engine = create_engine(REDSHIFT_CONN_STR)
        path = check_mock_is_full(engine, "viajes")
        if append:
            assert path is not None, "Path must exist when appending new trip data."
        else:
            if path is not None:
                return path

        path_drivers = kwargs["ti"].xcom_pull(task_ids="mock_drivers")
        path_usuarios = kwargs["ti"].xcom_pull(task_ids="mock_usuarios")

        viajes = mock_viajes_hlf(path_usuarios, path_drivers, append=append)

        # First append new data, then save appended data separately
        # If appending, return only new data path, else return the complete data path
        path = save_mock(viajes, "viajes", engine, append=append)
        if append:
            path_append = save_mock(viajes, "viajes_append", engine, append=False, to_sql=False)
            return path_append
        else:
            return path

    return mock_viajes


def get_mock_viajes_eventos(append: bool):
    @start_end_log("MOCK VIAJES_EVENTOS")
    def mock_viajes_eventos(**kwargs) -> None:
        """Mock trips events table."""
        engine = create_engine(REDSHIFT_CONN_STR)
        if append:
            # NOTE: Appended local CSV will be checked further down
            path = check_mock_is_full(engine, "viajes_eventos", check_local_csv=False)
        else:
            path = check_mock_is_full(engine, "viajes_eventos")
            if path is not None:
                return

        path_viajes = kwargs["ti"].xcom_pull(task_ids="mock_viajes")
        eventos = mock_viajes_eventos_hlf(path_viajes)

        # append: append new data (T) or replace data (F)
        save_mock(eventos, "viajes_eventos", engine, append=append)

    return mock_viajes_eventos


@start_end_log("MOCK CLIMA_ID")
def mock_clima_id() -> str:
    engine = create_engine(REDSHIFT_CONN_STR)
    path = check_mock_is_full(engine, "clima_id", is_fixed_table=True)
    if path is not None:
        return path

    path = "tables/clima_id.csv"
    df = pd.read_csv(path)
    save_to_sql(df, "clima_id", engine)

    return path


@start_end_log("MOCK CLIMA")
def mock_clima(**kwargs) -> None:
    engine = create_engine(REDSHIFT_CONN_STR)
    path = check_mock_is_full(engine, "clima", is_fixed_table=False)
    if path is not None:
        return

    path_clima_id = kwargs["ti"].xcom_pull(task_ids="mock_clima_id")
    path_viajes = kwargs["ti"].xcom_pull(task_ids="mock_viajes")

    clima = mock_clima_hlf(path_clima_id, path_viajes)

    save_mock(clima, "clima", engine)


def check_viajes_analisis() -> str | None:
    engine = create_engine(REDSHIFT_CONN_STR)
    return check_mock_is_full(
        engine,
        "viajes_analisis", 
        is_fixed_table=False, 
        check_local_csv=False,
        count_not_exists_as_empty=True,
    )


def viajes_analisis_is_full(**kwargs) -> str:
    path = kwargs["ti"].xcom_pull(task_ids="check_viajes_analisis")
    return "va_is_full" if isinstance(path, str) else "va_is_empty"
