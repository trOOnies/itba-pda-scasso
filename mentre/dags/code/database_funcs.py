"""database_funcs DAG's functions."""

import logging
import os
import re
from typing import TYPE_CHECKING
from sqlalchemy import create_engine

from code.database import REDSHIFT_CONN_STR
from code.utils import start_end_log

if TYPE_CHECKING:
    from pandas import DataFrame

KEYWORDS_PATT = re.compile(r"\{[A-Z\_]+\}")


def parse_prev_kwargs(query: str, prev_kwargs: dict[str, str] | None) -> str:
    """Replace kwargs in the query previous to replacing env vars."""
    if prev_kwargs is not None:
        for old, new in prev_kwargs.items():
            query = query.replace("{" + old + "}", new)
    return query


def execute_query(
    engine,
    statement: str,
    filename: str,
    prev_kwargs: dict[str, str] | None = None,
):
    """NOTE: Validation must have been made before running this function."""
    with open(f"queries/{statement}/{filename}") as f:
        query = f.read()

    query = parse_prev_kwargs(query, prev_kwargs)
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

    @start_end_log(f"REDSHIFT {statement_.upper()} QUERY")
    def run_query() -> None:
        engine = create_engine(REDSHIFT_CONN_STR)
        execute_query(engine, statement_, filename)

    return run_query


def dml_query(statement: str, filename: str):
    """Allows: delete, insert."""
    statement_ = statement.lower()
    assert statement_ in {
        "delete",
        "insert",
    }, "Only certain DDL statements are allowed."

    assert filename.endswith(".sql")
    assert "." not in filename[:-4]
    assert "~" not in filename[:-4]

    @start_end_log(f"REDSHIFT {statement_.upper()} QUERY")
    def run_query() -> None:
        engine = create_engine(REDSHIFT_CONN_STR)
        execute_query(engine, statement_, filename)

    return run_query


def select_query(filename: str):
    assert filename.endswith(".sql")
    assert "." not in filename[:-4]
    assert "~" not in filename[:-4]

    @start_end_log("REDSHIFT SELECT QUERY")
    def run_query() -> None:
        engine = create_engine(REDSHIFT_CONN_STR)
        result = execute_query(engine, "select", filename)
        result = result.fetchall()
        logging.info(result)

    return run_query


def check_mock_is_full(
    engine,
    table_name: str,
    is_fixed_table: bool = False,
    check_local_csv: bool = True,
    count_not_exists_as_empty: bool = False,
) -> str | None:
    """Check if the existing table is full.

    Returns the CSV path if it is. Else returns None.
    """
    assert all(ch not in table_name for ch in [".", "/", "\\"])
    try:
        result = execute_query(
            engine,
            "select",
            "check_exists.sql",
            prev_kwargs={"DB_TABLE": table_name},
        )
    except Exception:
        if not count_not_exists_as_empty:
            raise
        result = None

    is_full = result.first()[0] if result is not None else False
    if not is_full:
        return None
    logging.info(
        f"The table '{table_name}' exists and it's already full. Skipping data creation."
    )

    if not check_local_csv:
        return "fake/path"
    path = (
        f"tables/{table_name}.csv"
        if is_fixed_table
        else f"local/mocked_{table_name}.csv"
    )

    assert os.path.exists(
        path
    ), f"Table '{table_name}' is full but local CSV doesn't exist."
    return path


def get_max_id(engine, table_name: str) -> int:
    assert all(ch not in table_name for ch in [".", "/", "\\"])
    result = execute_query(
        engine,
        "select",
        "get_max_id.sql",
        prev_kwargs={"DB_TABLE": table_name},
    )
    return result.first()[0]


def append_df_to_redshift(df: "DataFrame", table_name: str, engine) -> None:
    """Append DataFrame to Redshift table."""
    df.to_sql(
        schema=os.environ["DB_SCHEMA"],
        name=table_name,
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
    )
