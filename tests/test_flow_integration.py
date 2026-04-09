"""End-to-end integration test against the testcontainers Postgres."""

from __future__ import annotations

import psycopg

from orchestration.tasks import (
    clean_table,
    download_source,
    load_country_codes,
    load_to_raw,
    validate_raw,
)


def test_ingest_through_load_populates_raw(conn: psycopg.Connection) -> None:
    """End-to-end ingest → validate → clean → load against testcontainers PG.

    Stops short of dbt (covered separately by ``dbt build --target test`` in CI)
    so this test does not depend on dbt_packages being installed.
    """
    load_country_codes.fn()
    for name in ("installations", "emissions", "allowances"):
        raw = download_source.fn(name)
        validated = validate_raw.fn(name, raw)
        cleaned = clean_table.fn(name, validated)
        rows = load_to_raw.fn(name, cleaned)
        assert rows > 0

    with conn.cursor() as cur:
        cur.execute("select count(*) from raw.installations")
        r = cur.fetchone()
        assert r is not None and r[0] > 0
        cur.execute("select count(*) from raw.emissions")
        r = cur.fetchone()
        assert r is not None and r[0] > 0
