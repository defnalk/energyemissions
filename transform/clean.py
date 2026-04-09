"""Python-side cleaning: encoding, units, dedup, type coercion."""

from __future__ import annotations

import pandas as pd
import structlog

log = structlog.get_logger(__name__)

# All emissions reported as tonnes CO2 equivalent. Conversion factors below
# express the multiplier required to convert each unit to tonnes CO2e.
UNIT_FACTORS: dict[str, float] = {
    "t": 1.0,
    "tonnes": 1.0,
    "kt": 1_000.0,
    "Mt": 1_000_000.0,
}


def fix_encoding(series: pd.Series) -> pd.Series:
    """Repair Windows-1252/Latin-1 mojibake in a string column."""

    def _fix(value: object) -> object:
        if not isinstance(value, str):
            return value
        try:
            return value.encode("latin-1").decode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError):
            return value

    return series.map(_fix)


def normalize_units(df: pd.DataFrame, value_col: str, unit_col: str = "unit") -> pd.DataFrame:
    """Convert ``value_col`` to tonnes CO2e using ``unit_col`` (default: 't')."""
    df = df.copy()
    if unit_col not in df.columns:
        return df
    factors = df[unit_col].map(UNIT_FACTORS).fillna(1.0)
    df[value_col] = df[value_col] * factors
    df = df.drop(columns=[unit_col])
    return df


def coerce_numeric(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Coerce columns to numeric; failures become NaN with a logged warning."""
    df = df.copy()
    for col in columns:
        before_null = df[col].isna().sum()
        df[col] = pd.to_numeric(df[col], errors="coerce")
        after_null = df[col].isna().sum()
        if after_null > before_null:
            log.warning(
                "numeric_coercion_loss", column=col, lost=int(after_null - before_null)
            )
    return df


def dedupe_emissions(df: pd.DataFrame) -> pd.DataFrame:
    """Drop dupes on (installation_id, year, activity_type), keeping latest report."""
    if "reporting_date" in df.columns:
        df = df.sort_values("reporting_date")
    return df.drop_duplicates(
        subset=["installation_id", "year", "activity_type"], keep="last"
    ).reset_index(drop=True)


def clean_installations(df: pd.DataFrame) -> pd.DataFrame:
    """Apply the full installation cleaning pipeline."""
    df = df.copy()
    df["name"] = fix_encoding(df["name"])
    df = coerce_numeric(df, ["latitude", "longitude"])
    df = df.drop_duplicates(subset=["installation_id"], keep="last").reset_index(drop=True)
    return df


def clean_emissions(df: pd.DataFrame) -> pd.DataFrame:
    """Apply the full emissions cleaning pipeline."""
    df = coerce_numeric(df, ["verified_tonnes", "year"])
    df = df.dropna(subset=["installation_id", "year", "verified_tonnes"])
    df["year"] = df["year"].astype(int)
    df = dedupe_emissions(df)
    return df


def clean_allowances(df: pd.DataFrame) -> pd.DataFrame:
    """Apply the full allowances cleaning pipeline."""
    df = coerce_numeric(df, ["allocated_tonnes", "surrendered_tonnes", "year"])
    df = df.dropna(
        subset=["installation_id", "year", "allocated_tonnes", "surrendered_tonnes"]
    )
    df["year"] = df["year"].astype(int)
    df = df.drop_duplicates(subset=["installation_id", "year"], keep="last").reset_index(drop=True)
    return df
