"""
Prefect DAG: generate trade panel → GCS data lake → BigQuery warehouse → dbt transforms.

Prefect is workflow orchestration (similar role to Airflow): schedules, retries, observability.
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from dotenv import load_dotenv
from prefect import flow, get_run_logger, task


ROOT = Path(__file__).resolve().parents[1]


@task(name="generate-local-parquet")
def generate_local_parquet() -> Path:
    load_dotenv(ROOT / ".env")
    logger = get_run_logger()
    out = ROOT / "data/raw/trade_monthly.parquet"
    cmd = [sys.executable, str(ROOT / "scripts/generate_trade_sample.py"), "--out", str(out)]
    logger.info("Running: %s", " ".join(cmd))
    subprocess.check_call(cmd, cwd=str(ROOT))
    return out


@task(name="upload-to-gcs-lake")
def upload_to_gcs(local_path: Path) -> str:
    load_dotenv(ROOT / ".env")
    logger = get_run_logger()
    cmd = [
        sys.executable,
        str(ROOT / "scripts/upload_to_gcs.py"),
        "--local",
        str(local_path),
    ]
    logger.info("Running: %s", " ".join(cmd))
    subprocess.check_call(cmd, cwd=str(ROOT))
    bucket = os.environ["GCS_BUCKET"]
    return f"gs://{bucket}/raw/trade_monthly/trade_monthly.parquet"


@task(name="load-bigquery-warehouse")
def load_bigquery(gcs_uri: str) -> None:
    load_dotenv(ROOT / ".env")
    logger = get_run_logger()
    cmd = [sys.executable, str(ROOT / "scripts/load_bq_from_gcs.py")]
    logger.info("Loading warehouse from %s", gcs_uri)
    subprocess.check_call(cmd, cwd=str(ROOT))


@task(name="dbt-run")
def dbt_run() -> None:
    load_dotenv(ROOT / ".env")
    logger = get_run_logger()
    env = os.environ.copy()
    env.setdefault("DBT_PROFILES_DIR", str(ROOT / "dbt_trade"))
    cmd = ["dbt", "run", "--project-dir", str(ROOT / "dbt_trade"), "--profiles-dir", str(ROOT / "dbt_trade")]
    logger.info("Running dbt")
    subprocess.check_call(cmd, cwd=str(ROOT), env=env)


@flow(name="global-trade-shocks-pipeline", log_prints=True)
def trade_pipeline() -> None:
    """End-to-end pipeline for peer review: multiple sequential steps in one flow."""
    path = generate_local_parquet()
    uri = upload_to_gcs(path)
    load_bigquery(uri)
    dbt_run()


if __name__ == "__main__":
    trade_pipeline()
