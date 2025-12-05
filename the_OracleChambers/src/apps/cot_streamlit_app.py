"""
Streamlit front-end for viewing COT Engine Lite output.

Reads from data/cot/cot_store.parquet produced by cot_engine.engine_runner.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

COT_STORE_PATH = Path("data/cot/cot_store.parquet")

st.set_page_config(page_title="COT Engine Viewer", layout="wide")


@st.cache_data(ttl=3600)
def load_cot_store() -> pd.DataFrame:
    if COT_STORE_PATH.exists():
        return pd.read_parquet(COT_STORE_PATH)
    return pd.DataFrame()


def main() -> None:
    st.title("📊 COT Engine Lite Viewer")

    df = load_cot_store()
    if df.empty:
        st.warning(
            "No COT data found in data/cot/cot_store.parquet.\n\n"
            "Run the COT engine once to populate it:\n\n"
            "    $env:PYTHONPATH = \"$PWD/src\"\n"
            "    python -m cot_engine.engine_runner"
        )
        return

    # Sidebar filters
    st.sidebar.header("Filters")
    exchanges = sorted(df["exchange"].dropna().unique().tolist())
    commodities = sorted(df["commodity"].dropna().unique().tolist())

    sel_exchange = st.sidebar.selectbox(
        "Exchange", options=["All"] + exchanges, index=0
    )
    sel_commodity = st.sidebar.selectbox(
        "Commodity", options=["All"] + commodities, index=0
    )

    filtered = df.copy()
    if sel_exchange != "All":
        filtered = filtered[filtered["exchange"] == sel_exchange]
    if sel_commodity != "All":
        filtered = filtered[filtered["commodity"] == sel_commodity]

    st.caption(f"{len(filtered)} rows after filters")

    st.dataframe(filtered.head(100), use_container_width=True)


if __name__ == "__main__":
    main()
