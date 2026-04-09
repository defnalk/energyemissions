"""Sector deep dive: filterable line chart over time."""

from __future__ import annotations

import plotly.express as px
import streamlit as st

from dashboard.components.filters import sidebar_filters
from dashboard.utils.db import query

st.title("Sector Deep Dive")
filters = sidebar_filters()
yr_min, yr_max = filters["year_range"]

df = query(
    "select * from mart.mart_sector_trends where year between %s and %s",
    (yr_min, yr_max),
)

if filters["countries"]:
    df = df[df["country_code"].isin(filters["countries"])]
if filters["sectors"]:
    df = df[df["sector"].isin(filters["sectors"])]

if df.empty:
    st.warning("No rows match your filters.")
    st.stop()

agg = df.groupby(["sector", "year"], as_index=False)["total_emissions_tonnes"].sum()
fig = px.line(
    agg,
    x="year",
    y="total_emissions_tonnes",
    color="sector",
    markers=True,
    title="Sector emissions over time",
)
st.plotly_chart(fig, use_container_width=True)
st.dataframe(agg, use_container_width=True)
