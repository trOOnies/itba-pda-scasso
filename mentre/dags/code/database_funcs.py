"""database_funcs DAG's functions."""

import logging
import os
import re
from sqlalchemy import create_engine

from code.database import REDSHIFT_CONN_STR

logger = logging.getLogger(__name__)

KEYWORDS_PATT = re.compile(r"\{[A-Z\_]+\}")


def execute_query(
    engine,
    statement: str,
    filename: str,
    prev_kwargs: dict[str, str] | None = None,
):
    """NOTE: Validation must have been made before running this function."""
    with open(f"queries/{statement}/{filename}") as f:
        query = f.read()
    if prev_kwargs is not None:
        query = query.replace(**prev_kwargs)

    keywords = KEYWORDS_PATT.findall(query)
    if keywords:
        query = query.format(**{k[1:-1]: os.environ[k[1:-1]] for k in keywords})

    return engine.execute(query)


def ddl_query(statement: str, filename: str):
    """Allows: create, drop."""
    statement_ = statement.lower()
    assert statement_ in {"create", "drop"}, "Only certain DDL statements are allowed."

    assert filename.endswith(".sql")
    assert "." not in filename[:-4]
    assert "~" not in filename[:-4]

    def run_query() -> None:
        logging.info("[START] REDSHIFT QUERY")
        engine = create_engine(REDSHIFT_CONN_STR)
        execute_query(engine, statement_, filename)
        logging.info("[END] REDSHIFT QUERY")

    return run_query


def dml_query(statement: str, filename: str):
    """Allows: delete, insert."""
    statement_ = statement.lower()
    assert statement_ in {"delete", "insert"}, "Only certain DDL statements are allowed."

    assert filename.endswith(".sql")
    assert "." not in filename[:-4]
    assert "~" not in filename[:-4]

    def run_query() -> None:
        logging.info("[START] REDSHIFT QUERY")
        engine = create_engine(REDSHIFT_CONN_STR)
        execute_query(engine, statement_, filename)
        logging.info("[END] REDSHIFT QUERY")

    return run_query


def select_query(filename: str):
    assert filename.endswith(".sql")
    assert "." not in filename[:-4]
    assert "~" not in filename[:-4]

    def run_query() -> None:
        logging.info("[START] REDSHIFT QUERY")
        engine = create_engine(REDSHIFT_CONN_STR)
        result = execute_query(engine, "select", filename)
        result = result.fetchall()
        logging.info(result)
        logging.info("[END] REDSHIFT QUERY")

    return run_query
