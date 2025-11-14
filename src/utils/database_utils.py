import typer
import pandas as pd
from sqlalchemy import create_engine
from ..config import CONNECTION_STRING as connection_string

def get_sql_server_engine():
    engine = create_engine(
        connection_string,
        fast_executemany=True,
    )

    return engine

def load_to_bronze(df: pd.DataFrame, table_name: str):
    if df.empty:
        typer.echo(f"\nNo data found for table {table_name}")
        return

    engine = get_sql_server_engine()

    try:
        df.to_sql(
            table_name,
            con=engine,
            schema="bronze",
            if_exists="append",
            index=False
        )

        typer.echo(f"Loaded {len(df)} records into bronze.{table_name} successfully.")

    except Exception as e:
        typer.echo(f"Error loading data into bronze.{table_name}: {e}")