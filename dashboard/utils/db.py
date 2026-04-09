"""Cached Postgres connection helpers for the Streamlit dashboard."""

from __future__ import annotations

import os

import pandas as pd
import psycopg
import streamlit as st


@st.cache_resource
def get_conn() -> psycopg.Connection:
    """Open and cache a Postgres connection for the lifetime of the session."""
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.getenv("POSTGRES_USER", "energy"),
        password=os.getenv("POSTGRES_PASSWORD", "energy"),
        dbname=os.getenv("POSTGRES_DB", "energy"),
        autocommit=True,
    )


@st.cache_data(ttl=3600)
def query(sql: str, params: tuple[object, ...] | None = None) -> pd.DataFrame:
    """Run a SQL query and return a DataFrame, cached for one hour."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
        cols = [d.name for d in cur.description] if cur.description else []
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)
