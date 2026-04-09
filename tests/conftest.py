"""Shared fixtures: ephemeral Postgres via testcontainers + small CSV sample."""

from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path

import pandas as pd
import psycopg
import pytest
from testcontainers.postgres import PostgresContainer

MIGRATIONS = Path(__file__).resolve().parents[1] / "warehouse" / "migrations"


@pytest.fixture(scope="session")
def pg() -> Iterator[PostgresContainer]:
    """Start a Postgres 16 container for the test session."""
    with PostgresContainer("postgres:16") as container:
        os.environ["POSTGRES_HOST"] = container.get_container_host_ip()
        os.environ["POSTGRES_PORT"] = str(container.get_exposed_port(5432))
        os.environ["POSTGRES_USER"] = container.username
        os.environ["POSTGRES_PASSWORD"] = container.password
        os.environ["POSTGRES_DB"] = container.dbname
        yield container


@pytest.fixture(scope="session")
def conn(pg: PostgresContainer) -> Iterator[psycopg.Connection]:
    """Open a connection and apply all migrations."""
    c = psycopg.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ["POSTGRES_PORT"]),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ["POSTGRES_DB"],
    )
    with c.cursor() as cur:
        for sql_file in sorted(MIGRATIONS.glob("*.sql")):
            cur.execute(sql_file.read_text())
    c.commit()
    yield c
    c.close()


@pytest.fixture
def sample_installations() -> pd.DataFrame:
    """100-row installation fixture."""
    return pd.DataFrame(
        [
            {
                "installation_id": f"EU{i:06d}",
                "name": f"Plant {i}",
                "country_code": "DE" if i % 2 else "FR",
                "sector": "Power & Heat",
                "latitude": 50.0,
                "longitude": 10.0,
            }
            for i in range(100)
        ]
    )


@pytest.fixture
def sample_emissions() -> pd.DataFrame:
    """100-row emissions fixture."""
    return pd.DataFrame(
        [
            {
                "installation_id": f"EU{i:06d}",
                "year": 2022,
                "activity_type": "combustion",
                "verified_tonnes": 1000.0 + i,
                "reporting_date": "2023-03-31",
            }
            for i in range(100)
        ]
    )
