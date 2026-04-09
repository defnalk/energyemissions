"""Reusable Streamlit sidebar filters wired to ``st.session_state``."""

from __future__ import annotations

import streamlit as st

from dashboard.utils.db import query


def sidebar_filters() -> dict[str, object]:
    """Render the global sidebar filters and return the active selection."""
    st.sidebar.header("Filters")

    years = query("select distinct year from mart.mart_country_emissions order by year")
    countries = query("select distinct country_code from mart.mart_country_emissions order by 1")
    sectors = query("select distinct sector from mart.mart_sector_trends order by 1")

    if years.empty:
        st.sidebar.warning("No data loaded yet — run `make seed`.")
        return {"year_range": (2013, 2023), "countries": [], "sectors": []}

    yr_min, yr_max = int(years["year"].min()), int(years["year"].max())
    year_range = st.sidebar.slider("Year range", yr_min, yr_max, (yr_min, yr_max))
    selected_countries = st.sidebar.multiselect(
        "Countries", countries["country_code"].tolist(), default=[]
    )
    selected_sectors = st.sidebar.multiselect(
        "Sectors", sectors["sector"].tolist(), default=[]
    )

    state = {
        "year_range": year_range,
        "countries": selected_countries,
        "sectors": selected_sectors,
    }
    st.session_state["filters"] = state
    return state
