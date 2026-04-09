"""Forecast + anomaly page: actuals, OLS projection, and flagged outliers."""

from __future__ import annotations

import plotly.graph_objects as go
import streamlit as st

from dashboard.components.filters import sidebar_filters
from dashboard.utils.db import query

st.title("Forecast & Anomalies")
sidebar_filters()

countries = query("select distinct country_code from mart.mart_country_emissions order by 1")
if countries.empty:
    st.warning("No data — run `make seed` first.")
    st.stop()

country = st.selectbox("Country", countries["country_code"].tolist())

actuals = query(
    "select year, total_emissions_tonnes from mart.mart_country_emissions "
    "where country_code = %s order by year",
    (country,),
)
forecast = query(
    "select year, forecast_tonnes, lower_band, upper_band from mart.mart_emissions_forecast "
    "where country_code = %s order by year",
    (country,),
)
anomalies = query(
    "select year, total_emissions_tonnes, z_score, severity "
    "from mart.mart_emissions_anomalies where country_code = %s order by year",
    (country,),
)

fig = go.Figure()
fig.add_trace(
    go.Scatter(
        x=actuals["year"],
        y=actuals["total_emissions_tonnes"],
        mode="lines+markers",
        name="actual",
    )
)
if not forecast.empty:
    fig.add_trace(
        go.Scatter(
            x=forecast["year"],
            y=forecast["forecast_tonnes"],
            mode="lines+markers",
            name="forecast",
            line={"dash": "dash"},
        )
    )
    fig.add_trace(
        go.Scatter(
            x=list(forecast["year"]) + list(forecast["year"][::-1]),
            y=list(forecast["upper_band"]) + list(forecast["lower_band"][::-1]),
            fill="toself",
            fillcolor="rgba(255,140,0,0.15)",
            line={"width": 0},
            name="95% band",
            showlegend=True,
        )
    )
if not anomalies.empty:
    fig.add_trace(
        go.Scatter(
            x=anomalies["year"],
            y=anomalies["total_emissions_tonnes"],
            mode="markers",
            marker={"size": 14, "color": "red", "symbol": "x"},
            name="anomaly",
        )
    )
fig.update_layout(
    title=f"{country}: actuals, forecast, and anomalies",
    xaxis_title="year",
    yaxis_title="tonnes CO₂e",
    height=550,
)
st.plotly_chart(fig, use_container_width=True)

c1, c2 = st.columns(2)
with c1:
    st.subheader("Forecast")
    st.dataframe(forecast, use_container_width=True)
with c2:
    st.subheader("Anomalies")
    st.dataframe(anomalies, use_container_width=True)
