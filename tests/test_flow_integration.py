"""End-to-end integration test against the testcontainers Postgres."""

from __future__ import annotations

import psycopg

from orchestration.flows import energy_pipeline


def test_full_flow_populates_marts(conn: psycopg.Connection) -> None:
    result = energy_pipeline()
    assert result["installations"] > 0
    assert result["emissions"] > 0
    assert result["allowances"] > 0
    with conn.cursor() as cur:
        cur.execute("select count(*) from mart.mart_country_emissions")
        row = cur.fetchone()
        assert row is not None and row[0] > 0
