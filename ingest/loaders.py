"""Bulk loaders that COPY validated DataFrames into Postgres raw schema."""

from __future__ import annotations

import io
import json
import os
from collections.abc import Iterable

import pandas as pd
import psycopg
import structlog

log = structlog.get_logger(__name__)


def get_conn() -> psycopg.Connection:
    """Open a psycopg3 connection from environment variables."""
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.getenv("POSTGRES_USER", "energy"),
        password=os.getenv("POSTGRES_PASSWORD", "energy"),
        dbname=os.getenv("POSTGRES_DB", "energy"),
    )


def truncate(conn: psycopg.Connection, table: str) -> None:
    """Truncate a fully-qualified table."""
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE {table} RESTART IDENTITY CASCADE")


def copy_dataframe(
    conn: psycopg.Connection,
    df: pd.DataFrame,
    table: str,
    columns: Iterable[str],
    source_file: str,
) -> int:
    """Bulk-load a DataFrame into ``table`` via COPY.

    Args:
        conn: Open psycopg3 connection.
        df: DataFrame whose columns are a superset of ``columns``.
        table: Fully-qualified target table name.
        columns: Ordered list of columns to load (excluding metadata).
        source_file: Value to put in the ``_source_file`` metadata column.

    Returns:
        Number of rows loaded.
    """
    cols = list(columns)
    df = df[cols].copy()
    df["_source_file"] = source_file

    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False, na_rep="\\N")
    buf.seek(0)

    col_sql = ", ".join([*cols, "_source_file"])
    sql = f"COPY {table} ({col_sql}) FROM STDIN WITH (FORMAT CSV, NULL '\\N')"
    with conn.cursor() as cur, cur.copy(sql) as copy:
        copy.write(buf.read())
    log.info("copied", table=table, rows=len(df))
    return len(df)


def quarantine(
    conn: psycopg.Connection, source_table: str, rows: list[dict[str, object]], error: str
) -> None:
    """Persist rejected rows to ``raw.rejected_rows``."""
    if not rows:
        return
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO raw.rejected_rows (source_table, payload, error) VALUES (%s, %s, %s)",
            [(source_table, json.dumps(r, default=str), error) for r in rows],
        )
    log.warning("quarantined", source_table=source_table, count=len(rows), error=error)
