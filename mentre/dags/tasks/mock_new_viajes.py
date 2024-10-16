"""Task functions for mock_new_viajes DAG."""

from sqlalchemy import create_engine

from code.database import REDSHIFT_CONN_STR
from code.database_funcs import check_mock_is_full


def check_table_task(
    table_name: str,
    is_fixed_table: bool,
    check_local_csv: bool,
):
    """Wrapper task function for `check_mock_is_full`."""
    def wrapper(*args, **kwargs):
        engine = create_engine(REDSHIFT_CONN_STR)
        return check_mock_is_full(engine, table_name, is_fixed_table, check_local_csv)
    return wrapper
