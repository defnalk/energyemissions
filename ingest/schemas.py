"""Pandera schemas for raw EU ETS data validation."""

from __future__ import annotations

try:
    import pandera.pandas as pa
except ImportError:  # pandera < 0.22
    import pandera as pa  # type: ignore[no-redef]
from pandera.typing import Series

COUNTRIES = [
    "AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR",
    "DE", "GR", "HU", "IE", "IT", "LV", "LT", "LU", "MT", "NL",
    "PL", "PT", "RO", "SK", "SI", "ES", "SE", "NO", "IS", "LI",
]


class InstallationSchema(pa.DataFrameModel):
    """Schema for raw installation rows."""

    installation_id: Series[str] = pa.Field(unique=True, str_matches=r"^EU\d{6}$")
    name: Series[str] = pa.Field(nullable=False)
    country_code: Series[str] = pa.Field(isin=COUNTRIES)
    sector: Series[str] = pa.Field(nullable=False)
    latitude: Series[float] = pa.Field(ge=-90, le=90, nullable=True)
    longitude: Series[float] = pa.Field(ge=-180, le=180, nullable=True)

    class Config:
        strict = False
        coerce = True


class EmissionSchema(pa.DataFrameModel):
    """Schema for raw emission rows."""

    installation_id: Series[str] = pa.Field(str_matches=r"^EU\d{6}$")
    year: Series[int] = pa.Field(ge=2005, le=2100)
    activity_type: Series[str] = pa.Field(nullable=False)
    verified_tonnes: Series[float] = pa.Field(ge=0)
    reporting_date: Series[str] = pa.Field(nullable=True)

    class Config:
        strict = False
        coerce = True


class AllowanceSchema(pa.DataFrameModel):
    """Schema for raw allowance rows."""

    installation_id: Series[str] = pa.Field(str_matches=r"^EU\d{6}$")
    year: Series[int] = pa.Field(ge=2005, le=2100)
    allocated_tonnes: Series[float] = pa.Field(ge=0)
    surrendered_tonnes: Series[float] = pa.Field(ge=0)

    class Config:
        strict = False
        coerce = True
