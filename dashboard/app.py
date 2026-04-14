"""
Streamlit dashboard: two primary tiles for course rubric
1) Categorical — HS chapter mix of imports
2) Temporal — monthly import totals (global shock narrative)
"""
from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from google.cloud import bigquery

ROOT = Path(__file__).resolve().parents[1]
st.set_page_config(page_title="Global Trade Shocks", layout="wide")
load_dotenv(ROOT / ".env")


@st.cache_data(ttl=600)
def run_query(sql: str) -> pd.DataFrame:
    creds = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    project = os.environ["GCP_PROJECT_ID"]
    client = bigquery.Client.from_service_account_json(creds) if creds else bigquery.Client(project=project)
    return client.query(sql).to_dataframe()


def main() -> None:
    dataset = os.environ["BQ_DATASET"]
    project = os.environ["GCP_PROJECT_ID"]

    st.title("Global trade shocks")
    st.caption(
        "Which **HS chapters** and **trade partners** move when a shock window hits? "
        "Data flows: Parquet → GCS (lake) → BigQuery (warehouse) → dbt marts → this dashboard."
    )

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Tile 1 — Import mix by HS2 chapter (categorical)")
        st.markdown("_Share of U.S. import value by broad product chapter (dbt mart)._")
        df_hs = run_query(
            f"""
            SELECT hs_chapter, import_value_usd, pct_of_total
            FROM `{project}.{dataset}.mart_hs_chapter_import_mix`
            ORDER BY import_value_usd DESC
            """
        )
        if df_hs.empty:
            st.warning("No rows — run the pipeline first.")
        else:
            st.bar_chart(df_hs.set_index("hs_chapter")["import_value_usd"])
            st.dataframe(df_hs, use_container_width=True, hide_index=True)

    with col2:
        st.subheader("Tile 2 — Monthly import trend (temporal)")
        st.markdown("_Total reported imports by month — look for inflection after the shock window._")
        df_ts = run_query(
            f"""
            SELECT trade_month, import_value_usd
            FROM `{project}.{dataset}.mart_monthly_import_trend`
            ORDER BY trade_month
            """
        )
        if df_ts.empty:
            st.warning("No rows — run the pipeline first.")
        else:
            df_ts["trade_month"] = pd.to_datetime(df_ts["trade_month"])
            st.line_chart(df_ts.set_index("trade_month")["import_value_usd"])
            st.caption("X-axis: month · Y-axis: USD import value (synthetic demo series).")


if __name__ == "__main__":
    main()
