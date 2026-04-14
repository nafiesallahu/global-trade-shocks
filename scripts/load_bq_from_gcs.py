"""
Load Parquet from GCS into BigQuery:
1) Staging table (truncate load)
2) Curated native table partitioned by trade_month and clustered for dashboard filters
"""
from __future__ import annotations

import os

from dotenv import load_dotenv
from google.cloud import bigquery


def main() -> None:
    load_dotenv()
    project = os.environ["GCP_PROJECT_ID"]
    dataset = os.environ["BQ_DATASET"]
    bucket = os.environ["GCS_BUCKET"]
    uri = f"gs://{bucket}/raw/trade_monthly/*.parquet"

    creds = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    client = bigquery.Client.from_service_account_json(creds) if creds else bigquery.Client(project=project)

    staging = f"{project}.{dataset}.stg_trade_monthly_raw"
    final = f"{project}.{dataset}.fct_trade_monthly"

    job = client.load_table_from_uri(
        uri,
        staging,
        job_config=bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True,
        ),
    )
    job.result()
    print(f"Loaded staging {staging}: {client.get_table(staging).num_rows:,} rows")

    sql = f"""
    CREATE OR REPLACE TABLE `{final}`
    PARTITION BY trade_month
    CLUSTER BY partner_iso, hs_chapter, trade_flow
    AS
    SELECT
      DATE(trade_month) AS trade_month,
      CAST(reporter_iso AS STRING) AS reporter_iso,
      CAST(partner_iso AS STRING) AS partner_iso,
      LPAD(CAST(hs_chapter AS STRING), 2, '0') AS hs_chapter,
      CAST(trade_flow AS STRING) AS trade_flow,
      CAST(trade_value_usd AS FLOAT64) AS trade_value_usd
    FROM `{staging}`
    """
    client.query(sql).result()
    t = client.get_table(final)
    print(f"Built {final}: {t.num_rows:,} rows (partitioned by trade_month, clustered by partner/hs/flow)")


if __name__ == "__main__":
    main()
