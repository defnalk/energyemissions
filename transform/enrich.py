"""Derived-column enrichment helpers (used in tests / ad-hoc; dbt does the heavy lift)."""

from __future__ import annotations

import pandas as pd


def add_yoy(df: pd.DataFrame, group: str, value: str, year_col: str = "year") -> pd.DataFrame:
    """Add a year-over-year percent change column ``{value}_yoy_pct``."""
    df = df.sort_values([group, year_col]).copy()
    df[f"{value}_yoy_pct"] = df.groupby(group)[value].pct_change() * 100.0
    return df


def add_per_capita(
    df: pd.DataFrame, value: str, population: str, out: str | None = None
) -> pd.DataFrame:
    """Add a per-capita ratio column."""
    df = df.copy()
    out = out or f"{value}_per_capita"
    df[out] = df[value] / df[population].replace(0, pd.NA)
    return df


def sector_rollup(df: pd.DataFrame, value: str = "verified_tonnes") -> pd.DataFrame:
    """Aggregate emissions by (sector, year)."""
    result = df.groupby(["sector", "year"], as_index=False)[value].sum()
    return pd.DataFrame(result)
