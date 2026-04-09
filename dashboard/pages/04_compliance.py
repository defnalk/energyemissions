"""Compliance: allocated vs surrendered scatter with 45-degree reference."""

from __future__ import annotations

import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from dashboard.components.filters import sidebar_filters
from dashboard.utils.db import query

st.title("Compliance: Allocated vs Surrendered")
filters = sidebar_filters()
yr_min, yr_max = filters["year_range"]

df = query(
    "select * from mart.mart_compliance_gap where year between %s and %s",
    (yr_min, yr_max),
)
if filters["countries"]:
    df = df[df["country_code"].isin(filters["countries"])]

if df.empty:
    st.warning("No rows match your filters.")
    st.stop()

fig = px.scatter(
    df,
    x="allocated_tonnes",
    y="surrendered_tonnes",
    color="country_code",
    hover_data=["year", "compliance_gap_tonnes"],
    title="Allocated vs surrendered allowances",
)
mx = float(max(df["allocated_tonnes"].max(), df["surrendered_tonnes"].max()))
fig.add_trace(
    go.Scatter(x=[0, mx], y=[0, mx], mode="lines", name="parity", line={"dash": "dash"})
)
st.plotly_chart(fig, use_container_width=True)
st.caption("Above the line = surplus allowances; below = deficit (over-emission).")
st.dataframe(df, use_container_width=True)
