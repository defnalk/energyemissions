"""Overview page: KPIs + choropleth."""

from __future__ import annotations

import plotly.express as px
import streamlit as st

from dashboard.components.filters import sidebar_filters
from dashboard.components.kpi_card import kpi_card
from dashboard.utils.db import query

st.title("Overview")
filters = sidebar_filters()
yr_min, yr_max = filters["year_range"]

df = query(
    "select * from mart.mart_country_emissions where year between %s and %s",
    (yr_min, yr_max),
)

if df.empty:
    st.warning("No data — run `make seed` to populate the warehouse.")
    st.stop()

latest_year = int(df["year"].max())
latest = df[df["year"] == latest_year]
prev = df[df["year"] == latest_year - 1]

c1, c2, c3, c4 = st.columns(4)
with c1:
    kpi_card("Total emissions (latest yr, Mt)", f"{latest['total_emissions_tonnes'].sum() / 1e6:,.1f}")
with c2:
    yoy = (
        (latest["total_emissions_tonnes"].sum() - prev["total_emissions_tonnes"].sum())
        / prev["total_emissions_tonnes"].sum()
        * 100
        if not prev.empty and prev["total_emissions_tonnes"].sum()
        else 0.0
    )
    kpi_card("YoY change", f"{yoy:+.1f}%")
with c3:
    kpi_card("Countries", f"{latest['country_code'].nunique():,}")
with c4:
    kpi_card("Installations", f"{int(latest['installation_count'].sum()):,}")

st.subheader(f"Emissions by country ({latest_year})")
fig = px.choropleth(
    latest,
    locations="country_name",
    locationmode="country names",
    color="total_emissions_tonnes",
    hover_name="country_name",
    scope="europe",
    color_continuous_scale="Reds",
)
st.plotly_chart(fig, use_container_width=True)
