"""Granular Prefect tasks with retries, caching, and structured logging."""

from __future__ import annotations

import subprocess
from datetime import timedelta

import pandas as pd
import structlog
from prefect import task

from ingest import loaders, sources
from ingest.schemas import AllowanceSchema, EmissionSchema, InstallationSchema
from transform import clean

log = structlog.get_logger(__name__)


@task(retries=3, retry_delay_seconds=60, cache_expiration=timedelta(hours=24), tags=["ingest"])
def download_source(name: str) -> pd.DataFrame:
    """Download (or load from local fallback) one source table."""
    log.info("task_download", name=name)
    return sources.load_table(name)


@task(retries=2, retry_delay_seconds=30, tags=["quality"])
def validate_raw(name: str, df: pd.DataFrame) -> pd.DataFrame:
    """Validate a raw DataFrame against its Pandera schema."""
    schemas = {
        "installations": InstallationSchema,
        "emissions": EmissionSchema,
        "allowances": AllowanceSchema,
    }
    schema = schemas[name]
    log.info("task_validate", name=name, rows=len(df))
    return schema.validate(df, lazy=True)


@task(retries=2, retry_delay_seconds=30, tags=["transform"])
def clean_table(name: str, df: pd.DataFrame) -> pd.DataFrame:
    """Apply the cleaning function appropriate for the table."""
    fn = {
        "installations": clean.clean_installations,
        "emissions": clean.clean_emissions,
        "allowances": clean.clean_allowances,
    }[name]
    return fn(df)


@task(retries=3, retry_delay_seconds=60, tags=["warehouse"])
def load_to_raw(name: str, df: pd.DataFrame) -> int:
    """Truncate and bulk-COPY a cleaned DataFrame into the raw schema."""
    columns = {
        "installations": [
            "installation_id",
            "name",
            "country_code",
            "sector",
            "latitude",
            "longitude",
        ],
        "emissions": [
            "installation_id",
            "year",
            "activity_type",
            "verified_tonnes",
            "reporting_date",
        ],
        "allowances": [
            "installation_id",
            "year",
            "allocated_tonnes",
            "surrendered_tonnes",
        ],
    }[name]
    table = f"raw.{name}"
    with loaders.get_conn() as conn:
        loaders.truncate(conn, table)
        rows = loaders.copy_dataframe(conn, df, table, columns, source_file=f"{name}.csv")
        conn.commit()
    return rows


@task(retries=1, tags=["warehouse"])
def load_country_codes() -> int:
    """Load the bundled ISO country lookup into raw.country_codes."""
    df = sources.load_country_codes()
    with loaders.get_conn() as conn:
        loaders.truncate(conn, "raw.country_codes")
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO raw.country_codes (country_code, country_name, population) VALUES (%s, %s, %s)",
                df[["country_code", "country_name", "population"]].itertuples(
                    index=False, name=None
                ),
            )
        conn.commit()
    return len(df)


@task(retries=1, tags=["warehouse"])
def run_dbt(command: str) -> None:
    """Invoke dbt with the given subcommand from the dbt_project directory."""
    log.info("task_dbt", command=command)
    result = subprocess.run(
        ["dbt", *command.split()],
        cwd="dbt_project",
        capture_output=True,
        text=True,
        check=False,
    )
    log.info("dbt_stdout", out=result.stdout[-2000:])
    if result.returncode != 0:
        log.error("dbt_failed", stderr=result.stderr[-2000:])
        raise RuntimeError(f"dbt {command} failed: {result.stderr[-500:]}")
