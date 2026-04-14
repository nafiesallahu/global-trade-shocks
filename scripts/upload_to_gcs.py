"""Upload local parquet to the GCS data lake (raw zone)."""
from __future__ import annotations

import argparse
import os
from pathlib import Path

from dotenv import load_dotenv
from google.cloud import storage


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("--local", type=Path, default=Path("data/raw/trade_monthly.parquet"))
    parser.add_argument(
        "--blob",
        default="raw/trade_monthly/trade_monthly.parquet",
        help="Object path inside the bucket",
    )
    args = parser.parse_args()

    bucket_name = os.environ["GCS_BUCKET"]
    creds = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    client = storage.Client.from_service_account_json(creds) if creds else storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(args.blob)
    blob.upload_from_filename(str(args.local), content_type="application/octet-stream")
    print(f"Uploaded gs://{bucket_name}/{args.blob}")


if __name__ == "__main__":
    main()
