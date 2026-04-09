"""Tests for the forecasting and anomaly detection module."""

from __future__ import annotations

import pandas as pd

from transform.forecast import ForecastConfig, detect_anomalies, forecast_country_emissions


def _history(country: str, values: list[float], start: int = 2015) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "country_code": [country] * len(values),
            "year": list(range(start, start + len(values))),
            "total_emissions_tonnes": values,
        }
    )


def test_forecast_extrapolates_linear_trend() -> None:
    df = _history("DE", [100.0, 110.0, 120.0, 130.0, 140.0])
    out = forecast_country_emissions(df, ForecastConfig(horizon_years=3))
    assert len(out) == 3
    # OLS slope is 10/year — first forecast year should be ~150.
    assert abs(out.iloc[0]["forecast_tonnes"] - 150.0) < 1e-6
    assert (out["upper_band"] >= out["forecast_tonnes"]).all()
    assert (out["lower_band"] <= out["forecast_tonnes"]).all()


def test_forecast_skips_short_history() -> None:
    df = _history("FR", [100.0, 110.0])
    out = forecast_country_emissions(df, ForecastConfig(min_history_years=4))
    assert out.empty


def test_forecast_handles_multiple_countries() -> None:
    df = pd.concat(
        [
            _history("DE", [100.0, 110.0, 120.0, 130.0, 140.0]),
            _history("FR", [200.0, 195.0, 190.0, 185.0, 180.0]),
        ]
    )
    out = forecast_country_emissions(df, ForecastConfig(horizon_years=2))
    assert set(out["country_code"]) == {"DE", "FR"}
    assert len(out) == 4


def test_anomaly_detects_large_spike() -> None:
    df = _history("DE", [100.0, 102.0, 101.0, 103.0, 102.0, 500.0, 105.0])
    out = detect_anomalies(df, ForecastConfig(anomaly_z_threshold=2.0))
    assert not out.empty
    assert 2020 in out["year"].tolist()
    assert out["severity"].iloc[0] in {"warning", "critical"}


def test_anomaly_returns_empty_for_steady_series() -> None:
    df = _history("DE", [100.0, 101.0, 102.0, 103.0, 104.0, 105.0])
    out = detect_anomalies(df)
    assert out.empty
