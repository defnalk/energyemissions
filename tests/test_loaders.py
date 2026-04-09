"""Tests for the bulk Postgres loaders."""

from __future__ import annotations

import pandas as pd
import psycopg

from ingest import loaders


def test_copy_dataframe_preserves_row_count(
    conn: psycopg.Connection, sample_installations: pd.DataFrame
) -> None:
    loaders.truncate(conn, "raw.installations")
    rows = loaders.copy_dataframe(
        conn,
        sample_installations,
        "raw.installations",
        ["installation_id", "name", "country_code", "sector", "latitude", "longitude"],
        source_file="installations.csv",
    )
    conn.commit()
    assert rows == 100
    with conn.cursor() as cur:
        cur.execute("select count(*) from raw.installations")
        result = cur.fetchone()
        assert result is not None
        assert result[0] == 100


def test_quarantine_persists_rows(conn: psycopg.Connection) -> None:
    loaders.quarantine(
        conn, "raw.emissions", [{"installation_id": "EU999999", "year": 9999}], "bad year"
    )
    conn.commit()
    with conn.cursor() as cur:
        cur.execute("select count(*) from raw.rejected_rows where source_table = 'raw.emissions'")
        result = cur.fetchone()
        assert result is not None
        assert result[0] >= 1
