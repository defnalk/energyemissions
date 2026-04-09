"""Streamlit multi-page entrypoint for the EU ETS dashboard."""

from __future__ import annotations

import streamlit as st

st.set_page_config(page_title="EU ETS Emissions", layout="wide", page_icon=":factory:")

st.title("EU ETS Emissions Dashboard")
st.markdown(
    """
    Explore verified emissions, sector trends, top emitters, and compliance gaps
    from the EU Emissions Trading System.

    Use the pages in the sidebar to navigate.
    """
)
st.info("Pick a page from the sidebar to begin.")
