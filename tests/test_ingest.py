"""Tests for ingest schemas and source loading."""

from __future__ import annotations

import pandas as pd
import pandera.errors
import pytest

from ingest import sources
from ingest.schemas import EmissionSchema, InstallationSchema


def test_installation_schema_accepts_valid(sample_installations: pd.DataFrame) -> None:
    InstallationSchema.validate(sample_installations)


def test_installation_schema_rejects_bad_country(sample_installations: pd.DataFrame) -> None:
    bad = sample_installations.copy()
    bad.loc[0, "country_code"] = "ZZ"
    with pytest.raises(pandera.errors.SchemaError):
        InstallationSchema.validate(bad)


def test_emission_schema_rejects_negative(sample_emissions: pd.DataFrame) -> None:
    bad = sample_emissions.copy()
    bad.loc[0, "verified_tonnes"] = -1
    with pytest.raises(pandera.errors.SchemaError):
        EmissionSchema.validate(bad)


def test_load_table_uses_local_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("USE_LOCAL_FALLBACK", "true")
    df = sources.load_table("installations")
    assert not df.empty
    assert "installation_id" in df.columns
