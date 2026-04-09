"""Forecasting and anomaly detection on country-level emissions.

Simple, dependency-free approach designed to run inside the Prefect flow
*after* dbt has built ``mart.mart_country_emissions``:

* **Forecast:** ordinary least squares on (year, total_emissions) per country,
  projected ``horizon`` years forward, with a +/- 1.96 * residual-stderr band.
* **Anomalies:** rolling z-score (window=3) on year-over-year change; rows with
  ``|z| > 2.5`` are flagged.

Both outputs are written to dedicated mart tables so the dashboard can read
them with the same cached ``query()`` helper as everything else.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
import structlog

log = structlog.get_logger(__name__)


@dataclass(frozen=True)
class ForecastConfig:
    """Configuration for the forecast task."""

    horizon_years: int = 5
    min_history_years: int = 4
    anomaly_z_threshold: float = 2.5
    anomaly_window: int = 3


def _fit_linear(years: np.ndarray, values: np.ndarray) -> tuple[float, float, float]:
    """Return (slope, intercept, residual_stderr) from a 1-D OLS fit."""
    n = len(years)
    if n < 2:
        return 0.0, float(values.mean() if n else 0.0), 0.0
    x_mean = years.mean()
    y_mean = values.mean()
    denom = ((years - x_mean) ** 2).sum()
    if denom == 0:
        return 0.0, float(y_mean), 0.0
    slope = float(((years - x_mean) * (values - y_mean)).sum() / denom)
    intercept = float(y_mean - slope * x_mean)
    fitted = slope * years + intercept
    residuals = values - fitted
    dof = max(n - 2, 1)
    stderr = float(np.sqrt((residuals**2).sum() / dof))
    return slope, intercept, stderr


def forecast_country_emissions(
    history: pd.DataFrame, config: ForecastConfig | None = None
) -> pd.DataFrame:
    """Project emissions ``horizon_years`` forward for every country.

    Args:
        history: Output of ``mart.mart_country_emissions`` with columns
            ``country_code``, ``year``, ``total_emissions_tonnes``.
        config: Forecast configuration; defaults to ``ForecastConfig()``.

    Returns:
        DataFrame with columns ``country_code``, ``year``, ``forecast_tonnes``,
        ``lower_band``, ``upper_band``, ``model``.
    """
    cfg = config or ForecastConfig()
    out_rows: list[dict[str, object]] = []

    for country, group in history.groupby("country_code", sort=True):
        g = group.sort_values("year")
        if len(g) < cfg.min_history_years:
            log.warning("forecast_skipped_insufficient_history", country=country, years=len(g))
            continue
        years = g["year"].to_numpy(dtype=float)
        values = g["total_emissions_tonnes"].to_numpy(dtype=float)
        slope, intercept, stderr = _fit_linear(years, values)

        last_year = int(years.max())
        for h in range(1, cfg.horizon_years + 1):
            yr = last_year + h
            point = slope * yr + intercept
            band = 1.96 * stderr * np.sqrt(h)  # widening band
            out_rows.append(
                {
                    "country_code": country,
                    "year": yr,
                    "forecast_tonnes": max(0.0, point),
                    "lower_band": max(0.0, point - band),
                    "upper_band": max(0.0, point + band),
                    "model": "ols_linear",
                }
            )

    return pd.DataFrame(out_rows)


def detect_anomalies(history: pd.DataFrame, config: ForecastConfig | None = None) -> pd.DataFrame:
    """Flag country-years whose YoY change is a rolling z-score outlier.

    Args:
        history: ``mart.mart_country_emissions`` rows.
        config: Anomaly window + threshold.

    Returns:
        DataFrame containing only the anomalous rows, with the computed
        ``yoy_pct``, ``z_score``, and a string ``severity`` label.
    """
    cfg = config or ForecastConfig()
    df = history.sort_values(["country_code", "year"]).copy()
    df["yoy_pct"] = df.groupby("country_code")["total_emissions_tonnes"].pct_change() * 100.0

    # Compute the rolling baseline from prior rows only (shift by 1) so that
    # an anomalous point does not contaminate its own z-score.
    grouped = df.groupby("country_code")["yoy_pct"]
    prior = grouped.shift(1)
    rolling_mean = prior.groupby(df["country_code"]).transform(
        lambda s: s.rolling(cfg.anomaly_window, min_periods=2).mean()
    )
    rolling_std = prior.groupby(df["country_code"]).transform(
        lambda s: s.rolling(cfg.anomaly_window, min_periods=2).std()
    )
    df["z_score"] = (df["yoy_pct"] - rolling_mean) / rolling_std.replace(0, np.nan)

    anomalies = df[df["z_score"].abs() > cfg.anomaly_z_threshold].copy()
    anomalies["severity"] = np.where(anomalies["z_score"].abs() > 4.0, "critical", "warning")
    return anomalies[
        ["country_code", "year", "total_emissions_tonnes", "yoy_pct", "z_score", "severity"]
    ].reset_index(drop=True)
