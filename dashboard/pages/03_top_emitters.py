"""Top emitters: ranked horizontal bar chart."""

from __future__ import annotations

import plotly.express as px
import streamlit as st

from dashboard.components.filters import sidebar_filters
from dashboard.utils.db import query

st.title("Top Emitters")
filters = sidebar_filters()

years = query("select distinct year from mart.mart_top_emitters order by year desc")
if years.empty:
    st.warning("No data — run `make seed` first.")
    st.stop()

year = st.selectbox("Year", years["year"].tolist())
df = query(
    "select * from mart.mart_top_emitters where year = %s order by rank_in_year limit 20",
    (int(year),),
)

fig = px.bar(
    df.sort_values("total_emissions_tonnes"),
    x="total_emissions_tonnes",
    y="installation_name",
    color="sector",
    orientation="h",
    title=f"Top 20 emitters in {year}",
)
fig.update_layout(height=700)
st.plotly_chart(fig, use_container_width=True)
st.dataframe(df, use_container_width=True)
