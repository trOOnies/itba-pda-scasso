"""Database connection code."""

import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

REDSHIFT_CONN_STR = (
    f"redshift+psycopg2://{os.environ['DB_USER']}"
    + f":{os.environ['DB_PASSWORD']}@{os.environ['DB_HOST']}"
    + f":{os.environ['DB_PORT']}/{os.environ['DB_DATABASE']}"
)


def get_engine():
    return create_engine(REDSHIFT_CONN_STR)
