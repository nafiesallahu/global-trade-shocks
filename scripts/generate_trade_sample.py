"""
Generate a reproducible monthly trade panel (synthetic but realistic schema).

Mirrors UN Comtrade-style fields: reporter, partner, HS2 chapter, flow, USD value.
Embeds a configurable 'shock' month so dashboards show visible partner/product shifts.
"""
from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("data/raw/trade_monthly.parquet"),
        help="Output parquet path",
    )
    parser.add_argument("--months", type=int, default=48)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    rng = np.random.default_rng(args.seed)

    reporters = ["USA"]
    partners = ["CHN", "MEX", "DEU", "JPN", "GBR", "KOR", "VNM", "IND", "CAN", "BRA"]
    hs_chapters = ["01", "02", "27", "28", "39", "71", "84", "85", "87", "90"]
    flows = ["Import", "Export"]

    months = pd.date_range(end=date.today().replace(day=1), periods=args.months, freq="MS")
    shock_idx = int(args.months * 0.75)

    rows: list[dict] = []
    for i, trade_month in enumerate(months):
        shock = i >= shock_idx
        for reporter in reporters:
            for partner in partners:
                for hs in hs_chapters:
                    for flow in flows:
                        base = rng.lognormal(12, 0.35)
                        if partner == "CHN" and hs in ("85", "84"):
                            base *= 1.35
                        if shock and partner == "DEU" and flow == "Import":
                            base *= 0.82
                        if shock and hs == "27" and flow == "Import":
                            base *= 1.18
                        noise = rng.normal(1.0, 0.08)
                        trade_value_usd = max(50_000.0, float(base * 25_000 * noise))
                        rows.append(
                            {
                                "trade_month": trade_month.date(),
                                "reporter_iso": reporter,
                                "partner_iso": partner,
                                "hs_chapter": hs,
                                "trade_flow": flow,
                                "trade_value_usd": round(trade_value_usd, 2),
                            }
                        )

    df = pd.DataFrame(rows)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(args.out, index=False)
    print(f"Wrote {len(df):,} rows to {args.out}")


if __name__ == "__main__":
    main()
