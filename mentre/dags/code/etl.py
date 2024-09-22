"""ETL Airflow functions."""

import datetime as dt
import os
import pandas as pd
import requests
from sqlalchemy import create_engine

from code.database import REDSHIFT_CONN_STR

import logging

logger = logging.getLogger(__name__)


def extract_data(**kwargs) -> str:
    """Extract the data from the API and save as parquet file."""
    logging.info("[START] EXTRACT DATA")

    extracted_fd_path = kwargs["extracted_fd_path"]
    assert os.path.isdir(extracted_fd_path)
    path = os.path.join(
        extracted_fd_path,
        f"extracted_data_{dt.date.today()}.pq",
    )

    logging.info("Getting data...")
    resp = requests.get("https://dummy-json.mock.beeceptor.com/posts")
    data = resp.json()
    logging.info("Getting data OK")

    df = pd.DataFrame(data)

    df.to_parquet(path, index=False)
    df.to_csv(path[:-3] + ".csv", index=False)
    logging.info(f"Extracted data saved to {path}")

    logging.info("[END] EXTRACT DATA")
    return path


def transform_data(**kwargs) -> str:
    """Transform the data and save it as parquet file."""
    logging.info("[START] TRANSFORM DATA")

    path_df = (
        kwargs["extracted_path"]
        if "extracted_path" in kwargs
        else kwargs["ti"].xcom_pull(task_ids="extract_data")
    )
    df = pd.read_parquet(path_df)

    transformed_fd_path = kwargs["transformed_fd_path"]
    assert os.path.isdir(transformed_fd_path)
    transformed_path = os.path.join(
        transformed_fd_path,
        f"transformed_data_{dt.date.today()}.pq",
    )

    # TODO: Example transformation
    logging.info("Transforming data...")
    df = df[df["comment_count"] > 10]
    logging.info("Transforming data OK")

    df.to_parquet(transformed_path, index=False)
    df.to_csv(transformed_path[:-3] + ".csv", index=False)
    logging.info(f"Transformed data saved to {transformed_path}")

    logging.info("[END] TRANSFORM DATA")
    return transformed_path


def load_to_redshift(**kwargs) -> None:
    """Load transformed data to Amazon Redshift."""
    logging.info("[START] LOAD TO REDSHIFT")

    path_df = (
        kwargs["transformed_path"]
        if "transformed_path" in kwargs
        else kwargs["ti"].xcom_pull(task_ids="transform_data")
    )
    df = pd.read_parquet(path_df)
    redshift_table = kwargs["redshift_table"]

    with open("queries/tables.sql") as f:
        tables_query = f.read()
    tables_query = tables_query.format(DB_SCHEMA=os.environ["DB_SCHEMA"])

    engine = create_engine(REDSHIFT_CONN_STR)

    tables = [t[0] for t in engine.execute(tables_query).fetchall()]
    logging.info("Tables in schema:", tables)

    df.to_sql(
        schema=os.environ["DB_SCHEMA"],
        name=redshift_table,
        con=engine,
        if_exists="replace",
        index=False,
        method="multi",
    )
    logging.info(f"Transformed data loaded into Redshift in table '{redshift_table}'")

    logging.info("[END] LOAD TO REDSHIFT")
