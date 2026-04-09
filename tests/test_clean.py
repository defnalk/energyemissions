"""Tests for the cleaning layer."""

from __future__ import annotations

import pandas as pd

from transform import clean


def test_fix_encoding_repairs_mojibake() -> None:
    s = pd.Series(["Köln".encode("utf-8").decode("latin-1")])
    out = clean.fix_encoding(s)
    assert out.iloc[0] == "Köln"


def test_coerce_numeric_nulls_bad_values() -> None:
    df = pd.DataFrame({"x": ["1", "2", "oops"]})
    out = clean.coerce_numeric(df, ["x"])
    assert out["x"].isna().sum() == 1


def test_dedupe_keeps_latest_report() -> None:
    df = pd.DataFrame(
        {
            "installation_id": ["EU000001"] * 2,
            "year": [2022, 2022],
            "activity_type": ["combustion"] * 2,
            "verified_tonnes": [100.0, 200.0],
            "reporting_date": ["2023-01-01", "2023-06-01"],
        }
    )
    out = clean.dedupe_emissions(df)
    assert len(out) == 1
    assert out.iloc[0]["verified_tonnes"] == 200.0


def test_clean_emissions_drops_nulls() -> None:
    df = pd.DataFrame(
        {
            "installation_id": ["EU000001", "EU000002"],
            "year": [2022, 2022],
            "activity_type": ["combustion", "combustion"],
            "verified_tonnes": [100.0, None],
            "reporting_date": ["2023-01-01", "2023-01-01"],
        }
    )
    out = clean.clean_emissions(df)
    assert len(out) == 1


def test_normalize_units_converts_kt() -> None:
    df = pd.DataFrame({"verified_tonnes": [1.0, 2.0], "unit": ["kt", "t"]})
    out = clean.normalize_units(df, "verified_tonnes")
    assert out["verified_tonnes"].tolist() == [1000.0, 2.0]
    assert "unit" not in out.columns
