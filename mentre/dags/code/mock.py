"""Generic functions for creating mock data in Redshift."""

import logging
import pandas as pd
import os
from random import randint
from sqlalchemy import create_engine

from code.database import REDSHIFT_CONN_STR
from code.database_funcs import append_df_to_redshift, check_mock_is_full

logger = logging.getLogger(__name__)


def save_to_sql(df: pd.DataFrame, table_name: str, engine) -> None:
    logging.info(f"Loading transformed data in table {table_name}...")
    chunksize = 1_000
    for ix in range(0, df.shape[0], chunksize):
        next_ix = min(ix + chunksize, df.shape[0])
        append_df_to_redshift(
            df.iloc[ix:next_ix],
            table_name=table_name,
            engine=engine,
        )
    logging.info(f"Transformed data loaded into Redshift in table '{table_name}'")


def save_mock(
    df: pd.DataFrame,
    table_name: str,
    engine,
    append: bool = False,
    to_sql: bool = True,
) -> str:
    """Save mock data to local CSV folder and to SQL."""
    path = f"local/mocked_{table_name}.csv"

    if append:
        df.to_csv(path, index=False, mode="a", header=not os.path.exists(path))
    else:
        df.to_csv(path, index=False)

    if to_sql:
        save_to_sql(df, table_name, engine)
    return path


def mock_base(
    table_name: str,
    n_min: int,
    n_max: int,
    mock_f,
) -> str:
    """Base function for the mocking of a table."""
    engine = create_engine(REDSHIFT_CONN_STR)
    path = check_mock_is_full(engine, table_name)
    if path is not None:
        return path

    logging.info(f"Creating items for {table_name}...")
    n_items = randint(n_min, n_max)  # ! both inclusive
    df = mock_f(n_items)
    logging.info(f"Items for {table_name} created.")

    path = save_mock(df, table_name, engine)
    return path
