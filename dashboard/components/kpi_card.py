"""Styled KPI metric card."""

from __future__ import annotations

import streamlit as st


def kpi_card(label: str, value: str, delta: str | None = None) -> None:
    """Render a single metric card with optional delta."""
    st.metric(label=label, value=value, delta=delta)
