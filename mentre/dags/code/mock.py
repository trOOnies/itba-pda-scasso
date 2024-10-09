"""Generic functions for creating mock data in Redshift."""

import logging
import os
import pandas as pd
from random import randint
from sqlalchemy import create_engine

from code.database import REDSHIFT_CONN_STR
from code.database_funcs import check_mock_is_full

logger = logging.getLogger(__name__)


def save_to_sql(df: pd.DataFrame, table_name: str, engine) -> None:
    logging.info(f"Loading transformed data in table {table_name}...")
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


def save_mock(df: pd.DataFrame, table_name: str, engine) -> str:
    """Save mock data to local CSV folder and to SQL."""
    path = f"local/mocked_{table_name}.csv"
    df.to_csv(path, index=False)
    save_to_sql(df, table_name, engine)
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
