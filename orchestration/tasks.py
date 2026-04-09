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
from transform.forecast import detect_anomalies, forecast_country_emissions

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
        # dbt writes test failures and model errors to stdout, not stderr,
        # so surface both streams when the command fails.
        log.error(
            "dbt_failed",
            returncode=result.returncode,
            stdout=result.stdout[-2000:],
            stderr=result.stderr[-2000:],
        )
        raise RuntimeError(
            f"dbt {command} failed (rc={result.returncode}): "
            f"{(result.stderr or result.stdout)[-500:]}"
        )


@task(retries=2, retry_delay_seconds=30, tags=["transform", "ml"])
def build_forecast_and_anomalies() -> tuple[int, int]:
    """Read mart_country_emissions, fit per-country forecasts, flag anomalies.

    Writes results to ``mart.mart_emissions_forecast`` and
    ``mart.mart_emissions_anomalies``. Returns ``(forecast_rows, anomaly_rows)``.
    """
    with loaders.get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "select country_code, year, total_emissions_tonnes from mart.mart_country_emissions"
        )
        rows = cur.fetchall()
    history = pd.DataFrame(rows, columns=["country_code", "year", "total_emissions_tonnes"])
    log.info("forecast_input", rows=len(history))

    forecast = forecast_country_emissions(history)
    anomalies = detect_anomalies(history)

    with loaders.get_conn() as conn:
        loaders.truncate(conn, "mart.mart_emissions_forecast")
        loaders.truncate(conn, "mart.mart_emissions_anomalies")
        if not forecast.empty:
            loaders.copy_dataframe(
                conn,
                forecast,
                "mart.mart_emissions_forecast",
                ["country_code", "year", "forecast_tonnes", "lower_band", "upper_band", "model"],
                source_file="forecast_task",
            )
        if not anomalies.empty:
            with conn.cursor() as cur:
                cur.executemany(
                    "INSERT INTO mart.mart_emissions_anomalies "
                    "(country_code, year, total_emissions_tonnes, yoy_pct, z_score, severity) "
                    "VALUES (%s, %s, %s, %s, %s, %s)",
                    list(
                        anomalies[
                            [
                                "country_code",
                                "year",
                                "total_emissions_tonnes",
                                "yoy_pct",
                                "z_score",
                                "severity",
                            ]
                        ].itertuples(index=False, name=None)
                    ),
                )
        conn.commit()

    log.info("forecast_done", forecast_rows=len(forecast), anomaly_rows=len(anomalies))
    return len(forecast), len(anomalies)
