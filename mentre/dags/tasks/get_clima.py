"""Weather related Airflow tasks functions."""

import datetime as dt
import json
import logging
import os
import pandas as pd
import requests
from sqlalchemy import create_engine

from code.database import REDSHIFT_CONN_STR
from code.database_funcs import append_df_to_redshift
from code.mock_clima import transform_function

logger = logging.getLogger(__name__)

BUENOS_AIRES_ID = 7894


def extract_data(**kwargs) -> str:
    """Extract the data from the API and save as parquet file."""
    logging.info("[START] EXTRACT DATA")

    extracted_fd_path = kwargs["extracted_fd_path"]
    assert os.path.isdir(extracted_fd_path), f"Path not found: {extracted_fd_path}"
    path = os.path.join(
        extracted_fd_path,
        f"extracted_data_{dt.date.today()}.json",
    )

    logging.info("Getting data...")
    resp = requests.get(
        f"{os.environ['ACWT_URL']}/currentconditions/v1/{BUENOS_AIRES_ID}",
        params={
            "apikey": os.environ["ACWT_API_KEY"],
            "language": "en-us",
            "details": True,
        },
    )
    if resp.status_code != 200:
        logging.error(resp.text)
        logging.error(resp.reason)
        raise AssertionError(f"Status code: {resp.status_code}")

    data = resp.json()
    logging.info("Getting data OK")

    with open(path, "w") as f:
        json.dump(data, f)
    logging.info(f"Extracted data saved to {path}")

    logging.info("[END] EXTRACT DATA")
    return path


def transform_data(**kwargs) -> str:
    """Transform the data and save it as parquet file."""
    logging.info("[START] TRANSFORM DATA")

    path_extr = kwargs["ti"].xcom_pull(task_ids="extract_data")
    with open(path_extr, "r") as f:
        data = json.load(f)
    data = data[0]  # Get first and only item

    transformed_fd_path = kwargs["transformed_fd_path"]
    assert os.path.isdir(transformed_fd_path), f"Path not found: {transformed_fd_path}"
    transformed_path = os.path.join(
        transformed_fd_path,
        f"transformed_data_{dt.date.today()}.pq",
    )

    logging.info("Transforming data...")
    engine = create_engine(REDSHIFT_CONN_STR)
    df = transform_function(data, engine)
    logging.info("Transforming data OK")

    df.to_parquet(transformed_path, index=False)
    df.to_csv(transformed_path[:-3] + ".csv", index=False)
    logging.info(f"Transformed data saved to {transformed_path}")

    logging.info("[END] TRANSFORM DATA")
    return transformed_path


def load_to_redshift(**kwargs) -> None:
    """Load transformed data to Amazon Redshift."""
    logging.info("[START] LOAD TO REDSHIFT")

    path_df = kwargs["ti"].xcom_pull(task_ids="transform_data")
    df = pd.read_parquet(path_df)
    redshift_table = kwargs["redshift_table"]

    engine = create_engine(REDSHIFT_CONN_STR)
    append_df_to_redshift(df, redshift_table, engine)
    logging.info(f"Transformed data loaded into Redshift in table '{redshift_table}'")

    logging.info("[END] LOAD TO REDSHIFT")
