"""Create the synthetic preexisting data for the project."""

from sqlalchemy import text

from code.database import get_engine


def open_query(path: str) -> str:
    with open(path) as f:
        query = f.read()
    return query


def run_query(query: str, engine, fetch_all: bool = False):
    with engine.connect() as conn:
        if fetch_all:
            return conn.execute(text(query)).fetchall()
        else:
            conn.execute(text(query))
            return


# * Tasks


def delete_tables():
    """Delete existing tables from Redshift."""
    query = open_query("queries/delete.sql")
    engine = get_engine()
    run_query(query, engine)
    rows = run_query(query, engine, fetch_all=True)


def create_tables():
    """Create the empty tables needed for the project."""
    query = open_query("queries/create.sql")
    engine = get_engine()
    run_query(query, engine)
    rows = run_query(query, engine, fetch_all=True)


def populate_tables():
    """Populate the empty Redshift tables with synthetic, random data."""
    ...


# * Main function


def main():
    ...
