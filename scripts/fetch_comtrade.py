"""
Optional extract: pull a small slice from the UN Comtrade HTTP API into the pipeline's Parquet shape.

Uses the same URL layout as the official `comtradeapicall` client:
  - No subscription key → `GET https://comtradeapi.un.org/public/v1/preview/C/M/HS` (max ~500 rows)
  - With `COMTRADE_SUBSCRIPTION_KEY` → `GET https://comtradeapi.un.org/data/v1/get/C/M/HS` (higher limits)

Docs: https://comtradedeveloper.un.org/ · https://uncomtrade.org/docs
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any

import pandas as pd
import requests
from dotenv import load_dotenv
from requests import Response


# M49 numeric codes used by Comtrade → ISO 3166-1 alpha-3 (subset; unknown → M49_<code>)
M49_TO_ISO3: dict[int, str] = {
    4: "AFG",
    8: "ALB",
    12: "DZA",
    24: "AGO",
    36: "AUS",
    40: "AUT",
    56: "BEL",
    76: "BRA",
    124: "CAN",
    156: "CHN",
    170: "COL",
    180: "COD",
    218: "ECU",
    250: "FRA",
    276: "DEU",
    356: "IND",
    360: "IDN",
    364: "IRN",
    368: "IRQ",
    372: "IRL",
    376: "ISR",
    380: "ITA",
    392: "JPN",
    410: "KOR",
    484: "MEX",
    528: "NLD",
    578: "NOR",
    643: "RUS",
    682: "SAU",
    702: "SGP",
    724: "ESP",
    752: "SWE",
    756: "CHE",
    792: "TUR",
    804: "UKR",
    826: "GBR",
    840: "USA",
    842: "USA",  # United States of America (alternate M49 in some extracts)
    858: "URY",
    862: "VEN",
    704: "VNM",
    710: "ZAF",
}


class ComtradeHttpError(RuntimeError):
    pass


def _m49_to_iso3(code: Any) -> str:
    if code is None or (isinstance(code, float) and pd.isna(code)):
        return "UNK"
    try:
        n = int(float(code))
    except (TypeError, ValueError):
        return "UNK"
    if n == 0:
        return "WLD"
    return M49_TO_ISO3.get(n, f"M49_{n}")


def _flow_to_label(flow: Any) -> str | None:
    if flow is None or (isinstance(flow, str) and flow.strip() == ""):
        return None
    s = str(flow).strip().upper()
    if s in ("M", "IMPORT"):
        return "Import"
    if s in ("X", "EXPORT", "X-FINAL"):
        return "Export"
    if s in ("RX", "RE-IMPORT"):
        return "Re-Import"
    if s in ("RM", "RE-EXPORT"):
        return "Re-Export"
    return s.title()


def _hs_chapter_from_cmd(cmd: Any) -> str:
    if cmd is None or (isinstance(cmd, float) and pd.isna(cmd)):
        return "00"
    s = str(cmd).strip()
    if s.upper() in ("TOTAL", "TO", "ALL"):
        return "00"
    digits = "".join(ch for ch in s if ch.isdigit())
    if len(digits) >= 2:
        return digits[:2].zfill(2)
    if len(digits) == 1:
        return digits.zfill(2)
    return "00"


def _build_url(type_code: str, freq_code: str, cl_code: str, subscription_key: str | None) -> str:
    base = f"{type_code}/{freq_code}/{cl_code}"
    if subscription_key:
        return f"https://comtradeapi.un.org/data/v1/get/{base}"
    return f"https://comtradeapi.un.org/public/v1/preview/{base}"


def _request_comtrade(
    *,
    subscription_key: str | None,
    type_code: str,
    freq_code: str,
    cl_code: str,
    params: dict[str, Any],
    timeout_s: int,
) -> dict[str, Any]:
    url = _build_url(type_code, freq_code, cl_code, subscription_key)
    q: dict[str, Any] = {k: v for k, v in params.items() if v is not None}
    if subscription_key:
        q["subscription-key"] = subscription_key
    q.setdefault("format", "JSON")

    try:
        resp: Response = requests.get(url, params=q, timeout=timeout_s)
    except requests.Timeout as e:
        raise ComtradeHttpError(f"Comtrade request timed out after {timeout_s}s: {url}") from e
    except requests.RequestException as e:
        raise ComtradeHttpError(f"Comtrade request failed (network): {e}") from e

    if resp.status_code == 401 or resp.status_code == 403:
        raise ComtradeHttpError(
            "Comtrade returned 401/403. Check COMTRADE_SUBSCRIPTION_KEY for the data/v1/get endpoint."
        )
    if resp.status_code == 404:
        raise ComtradeHttpError(
            f"Comtrade returned 404 for {url} — verify period/classification/cmd/partner parameters."
        )
    if resp.status_code == 429:
        raise ComtradeHttpError("Comtrade returned 429 Too Many Requests — back off and retry later.")
    if resp.status_code >= 500:
        raise ComtradeHttpError(f"Comtrade server error {resp.status_code}: {resp.text[:500]}")

    if resp.status_code != 200:
        raise ComtradeHttpError(f"Comtrade HTTP {resp.status_code}: {resp.text[:800]}")

    try:
        payload = resp.json()
    except json.JSONDecodeError as e:
        raise ComtradeHttpError("Comtrade response was not valid JSON.") from e

    if not isinstance(payload, dict):
        raise ComtradeHttpError("Comtrade JSON root must be an object.")
    return payload


def comtrade_to_pipeline_df(rows: list[dict[str, Any]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(
            columns=[
                "trade_month",
                "reporter_iso",
                "partner_iso",
                "hs_chapter",
                "trade_flow",
                "trade_value_usd",
            ]
        )

    df = pd.json_normalize(rows)
    # API field names vary slightly; pick the first present candidate per role
    def col(*names: str) -> pd.Series:
        for n in names:
            if n in df.columns:
                return df[n]
        return pd.Series([None] * len(df))

    period = col("period", "refPeriodId")
    primary_value = col("primaryValue", "TradeValue", "tradeValue", "primary_value")

    out = pd.DataFrame(
        {
            "trade_month": pd.to_datetime(period.astype(str).str.replace(r"\.0$", "", regex=True), format="%Y%m", errors="coerce").dt.date,
            "reporter_iso": col("reporterCode").map(_m49_to_iso3),
            "partner_iso": col("partnerCode").map(_m49_to_iso3),
            "hs_chapter": col("cmdCode", "commodityCode").map(_hs_chapter_from_cmd),
            "trade_flow": col("flowCode").map(_flow_to_label),
            "trade_value_usd": pd.to_numeric(primary_value, errors="coerce"),
        }
    )
    out = out.dropna(subset=["trade_month", "trade_flow", "trade_value_usd"])
    out = out[out["trade_value_usd"] > 0]
    return out


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(description="Fetch UN Comtrade preview/final slice and write pipeline Parquet.")
    parser.add_argument("--out", type=str, default="data/raw/trade_monthly.parquet")
    parser.add_argument("--period", type=str, default="202312", help="YYYYMM for monthly freq")
    parser.add_argument("--reporter", type=str, default="842", help="Comtrade reporter M49 code (842 = USA)")
    parser.add_argument(
        "--cmd-codes",
        type=str,
        default="27,84,85,30,87,39,71,90",
        help="Comma-separated HS commodity codes (digits). Broader pulls need a subscription key.",
    )
    parser.add_argument("--flow", type=str, default="M", help="Comtrade flow code, e.g. M (imports) or X (exports)")
    parser.add_argument("--partner", type=str, default="0", help="Partner M49; 0 = world (as supported by API)")
    parser.add_argument("--max-records", type=int, default=500)
    parser.add_argument("--timeout", type=int, default=120)
    args = parser.parse_args()

    key = (os.getenv("COMTRADE_SUBSCRIPTION_KEY") or os.getenv("COMTRADE_API_KEY") or "").strip() or None

    params: dict[str, Any] = {
        "reportercode": args.reporter,
        "period": args.period,
        "cmdCode": args.cmd_codes,
        "flowCode": args.flow,
        "partnerCode": args.partner,
        "partner2Code": None,
        "customsCode": None,
        "motCode": None,
        "maxRecords": args.max_records,
        "aggregateBy": None,
        "breakdownMode": "classic",
        "countOnly": None,
        "includeDesc": "false",
    }

    payload = _request_comtrade(
        subscription_key=key,
        type_code="C",
        freq_code="M",
        cl_code="HS",
        params=params,
        timeout_s=args.timeout,
    )

    rows = payload.get("data")
    if rows is None:
        err = payload.get("error") or payload.get("message") or payload
        raise ComtradeHttpError(f"Unexpected Comtrade payload (no 'data' key): {err}")

    if not isinstance(rows, list):
        raise ComtradeHttpError("Comtrade payload['data'] must be a list.")

    out_df = comtrade_to_pipeline_df(rows)
    if out_df.empty:
        print("Warning: normalized DataFrame is empty — check cmd/partner/period for this reporter.", file=sys.stderr)

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    out_df.to_parquet(args.out, index=False)
    print(f"Wrote {len(out_df):,} rows to {args.out} (Comtrade rows in: {len(rows):,})")


if __name__ == "__main__":
    try:
        main()
    except ComtradeHttpError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(2)
