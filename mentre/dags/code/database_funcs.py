"""database_funcs DAG's functions."""

import logging
import os
import re
from sqlalchemy import create_engine

from code.database import REDSHIFT_CONN_STR

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
        for old, new in prev_kwargs.items():
            query = query.replace("{" + old + "}", new)

    keywords = KEYWORDS_PATT.findall(query)
    if keywords:
        query = query.format(**{k[1:-1]: os.environ[k[1:-1]] for k in keywords})

    result = engine.execute(query)
    return result


def ddl_query(statement: str, filename: str):
    """Allows: create, drop."""
    statement_ = statement.lower()
    assert statement_ in {"create", "drop"}, "Only certain DDL statements are allowed."

    assert filename.endswith(".sql")
    assert "." not in filename[:-4]
    assert "~" not in filename[:-4]

    def run_query() -> None:
        logging.info(f"[START] REDSHIFT {statement_.upper()} QUERY")
        engine = create_engine(REDSHIFT_CONN_STR)
        execute_query(engine, statement_, filename)
        logging.info(f"[END] REDSHIFT {statement_.upper()} QUERY")

    return run_query


def dml_query(statement: str, filename: str):
    """Allows: delete, insert."""
    statement_ = statement.lower()
    assert statement_ in {"delete", "insert"}, "Only certain DDL statements are allowed."

    assert filename.endswith(".sql")
    assert "." not in filename[:-4]
    assert "~" not in filename[:-4]

    def run_query() -> None:
        logging.info(f"[START] REDSHIFT {statement_.upper()} QUERY")
        engine = create_engine(REDSHIFT_CONN_STR)
        execute_query(engine, statement_, filename)
        logging.info(f"[END] REDSHIFT {statement_.upper()} QUERY")

    return run_query


def select_query(filename: str):
    assert filename.endswith(".sql")
    assert "." not in filename[:-4]
    assert "~" not in filename[:-4]

    def run_query() -> None:
        logging.info("[START] REDSHIFT SELECT QUERY")
        engine = create_engine(REDSHIFT_CONN_STR)
        result = execute_query(engine, "select", filename)
        result = result.fetchall()
        logging.info(result)
        logging.info("[END] REDSHIFT SELECT QUERY")

    return run_query


def check_mock_is_full(engine, table_name: str, is_fixed_table: bool = False) -> str | None:
    """Check if the existing table is full.

    Returns the CSV path if it is. Else returns None.
    """
    assert all(ch not in table_name for ch in [".", "/", "\\"])
    result = execute_query(
        engine,
        "select",
        "check_exists.sql",
        prev_kwargs={"DB_TABLE": table_name},
    )
    is_full = result.first()[0]
    if is_full:
        logging.info(f"The table '{table_name}' exists and it's already full. Skipping data creation.")

        path = (
            f"tables/{table_name}.csv"
            if is_fixed_table
            else f"local/mocked_{table_name}.csv"
        )

        assert os.path.exists(path), f"Table '{table_name}' is full but local CSV doesn't exist."
        return path
    return None
