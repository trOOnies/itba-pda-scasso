"""database_funcs DAG's functions."""

import logging
import os
import re
from sqlalchemy import create_engine

from code.database import REDSHIFT_CONN_STR

logger = logging.getLogger(__name__)

KEYWORDS_PATT = re.compile(r"\{[A-Z\_]+\}")


def ddl_query(filename: str):
    assert filename.endswith(".sql")
    assert "." not in filename[:-4]
    assert "~" not in filename[:-4]

    def run_query() -> None:
        logging.info("[START] REDSHIFT QUERY")
        engine = create_engine(REDSHIFT_CONN_STR)

        with open(f"queries/{filename}") as f:
            tables_query = f.read()

        keywords = KEYWORDS_PATT.findall(tables_query)
        if keywords:
            tables_query = tables_query.format(**{k[1:-1]: os.environ[k[1:-1]] for k in keywords})

        engine.execute(tables_query)
        logging.info("[END] REDSHIFT QUERY")

    return run_query


def select_query(filename: str):
    assert filename.endswith(".sql")
    assert "." not in filename[:-4]
    assert "~" not in filename[:-4]

    def run_query() -> None:
        logging.info("[START] REDSHIFT QUERY")
        engine = create_engine(REDSHIFT_CONN_STR)

        with open(f"queries/{filename}") as f:
            tables_query = f.read()

        keywords = KEYWORDS_PATT.findall(tables_query)
        if keywords:
            tables_query = tables_query.format(**{k[1:-1]: os.environ[k[1:-1]] for k in keywords})

        result = engine.execute(tables_query).fetchall()
        logging.info(result)
        logging.info("[END] REDSHIFT QUERY")

    return run_query

