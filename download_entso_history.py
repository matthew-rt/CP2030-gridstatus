"""
download_entso_history.py
─────────────────────────
Backfill ENTSO-E day-ahead prices from a start date to now, writing (or
merging into) the entso_prices.json cache used by rerun_history.py.

The ENTSO-E Transparency API limits each request to a 1-year window, so
this script fetches in monthly chunks to stay well within that limit and
avoid timeouts.

Usage:
    python download_entso_history.py [--start 2026-01-01] [--out /path/to/entso_prices.json]

Reads ENTSO_E_API_KEY from environment (or ~/.env).
"""

import json
import os
import sys
import time
import argparse
import xml.etree.ElementTree as ET
import re
from datetime import datetime, timezone, timedelta
from pathlib import Path

# ── Load .env ─────────────────────────────────────────────────────────────────
_env_path = Path(__file__).parent / ".env"
if os.path.exists(_env_path):
    with open(_env_path) as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _v = _line.split("=", 1)
                os.environ.setdefault(_k.strip(), _v.strip())

import requests

sys.path.insert(0, os.path.dirname(__file__))
from cp2030price import ENTSO_E_URL, ENTSO_E_AREAS, fetch_eur_to_gbp

DEFAULT_OUT = os.environ.get("ENTSO_PRICES_FILE", "/var/www/cp2030/entso_prices.json")
RATE_LIMIT_PAUSE = 2.0  # seconds between requests to avoid 429s


def _fetch_area_prices_eur_window(area_code, api_key, period_start_str, period_end_str):
    """
    Fetch day-ahead prices for one area over an explicit window.
    period_start_str / period_end_str: "YYYYMMDDHHMM" UTC strings.
    Returns {utc_iso: price_eur}.
    """
    resp = requests.get(
        ENTSO_E_URL,
        params={
            "securityToken": api_key,
            "documentType": "A44",
            "in_Domain": area_code,
            "out_Domain": area_code,
            "periodStart": period_start_str,
            "periodEnd": period_end_str,
        },
        timeout=30,
    )
    resp.raise_for_status()

    root = ET.fromstring(resp.text)
    ns_uri = root.tag.split("}")[0].lstrip("{") if "}" in root.tag else ""
    ns = {"ns": ns_uri} if ns_uri else {}
    pf = "ns:" if ns_uri else ""

    prices = {}
    for ts in root.findall(f"{pf}TimeSeries", ns):
        for period in ts.findall(f"{pf}Period", ns):
            interval = period.find(f"{pf}timeInterval", ns)
            if interval is None:
                continue
            start_el = interval.find(f"{pf}start", ns)
            resolution_el = period.find(f"{pf}resolution", ns)
            if start_el is None or resolution_el is None:
                continue
            rm = re.match(r"PT(\d+)M", resolution_el.text or "")
            if not rm:
                continue
            interval_minutes = int(rm.group(1))
            period_start_dt = datetime.fromisoformat(
                start_el.text.replace("Z", "+00:00")
            )
            for point in period.findall(f"{pf}Point", ns):
                pos_el = point.find(f"{pf}position", ns)
                price_el = point.find(f"{pf}price.amount", ns)
                if pos_el is not None and price_el is not None:
                    slot_dt = period_start_dt + timedelta(
                        minutes=interval_minutes * (int(pos_el.text) - 1)
                    )
                    prices[slot_dt.strftime("%Y-%m-%dT%H:%M:%SZ")] = float(
                        price_el.text
                    )
    return prices


def fetch_history(start_dt, end_dt, api_key, eur_to_gbp):
    """
    Fetch all areas from start_dt to end_dt in monthly chunks.
    Returns merged {area_key: {utc_iso: price_gbp}}.
    """
    result = {area: {} for area in ENTSO_E_AREAS}

    # Build list of monthly windows
    windows = []
    cursor = start_dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    while cursor < end_dt:
        # next month
        if cursor.month == 12:
            next_month = cursor.replace(year=cursor.year + 1, month=1)
        else:
            next_month = cursor.replace(month=cursor.month + 1)
        window_end = min(next_month, end_dt)
        windows.append((cursor, window_end))
        cursor = next_month

    total = len(windows) * len(ENTSO_E_AREAS)
    done = 0

    for area_key, area_code in ENTSO_E_AREAS.items():
        for w_start, w_end in windows:
            ps = w_start.strftime("%Y%m%d%H%M")
            pe = w_end.strftime("%Y%m%d%H%M")
            try:
                prices_eur = _fetch_area_prices_eur_window(area_code, api_key, ps, pe)
                for ts, p in prices_eur.items():
                    result[area_key][ts] = round(p * eur_to_gbp, 2)
                done += 1
                print(
                    f"  [{done}/{total}] {area_key} {w_start.strftime('%Y-%m')}:"
                    f" {len(prices_eur)} points"
                )
            except Exception as e:
                done += 1
                print(
                    f"  [{done}/{total}] {area_key} {w_start.strftime('%Y-%m')}: ERROR — {e}"
                )
            time.sleep(RATE_LIMIT_PAUSE)

    return result


def main():
    parser = argparse.ArgumentParser(
        description="Backfill ENTSO-E day-ahead price history."
    )
    parser.add_argument(
        "--start",
        default="2026-01-01",
        help="Start date (YYYY-MM-DD, UTC). Default: 2026-01-01",
    )
    parser.add_argument(
        "--out", default=DEFAULT_OUT, help=f"Output JSON file. Default: {DEFAULT_OUT}"
    )
    args = parser.parse_args()

    api_key = os.environ.get("ENTSO_E_API_KEY")
    if not api_key:
        print("ERROR: ENTSO_E_API_KEY not set in environment or ~/.env")
        sys.exit(1)

    start_dt = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
    end_dt = datetime.now(timezone.utc) + timedelta(days=1)

    print(f"Fetching EUR/GBP exchange rate...")
    eur_to_gbp = fetch_eur_to_gbp()
    print(f"  EUR/GBP = {eur_to_gbp}")

    print(f"Fetching ENTSO-E history from {start_dt.date()} to {end_dt.date()}...")
    new_data = fetch_history(start_dt, end_dt, api_key, eur_to_gbp)

    # Merge into existing file (preserves any live data already cached)
    existing = {}
    if os.path.exists(args.out):
        try:
            with open(args.out) as f:
                existing = json.load(f)
            print(f"Merging into existing {args.out}")
        except Exception:
            print(f"Could not read existing {args.out}, starting fresh")

    for area_key, prices in new_data.items():
        if area_key not in existing:
            existing[area_key] = {}
        existing[area_key].update(prices)

    # Write atomically
    tmp = args.out + ".tmp"
    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(tmp, "w") as f:
        json.dump(existing, f)
    os.replace(tmp, args.out)

    total_points = sum(len(v) for v in existing.values())
    print(f"\nDone. {total_points} total price points written to {args.out}")
    for area_key, prices in existing.items():
        if prices:
            times = sorted(prices)
            print(
                f"  {area_key}: {len(prices)} points, "
                f"£{min(prices.values()):.1f}–£{max(prices.values()):.1f}/MWh, "
                f"latest {times[-1]}"
            )


if __name__ == "__main__":
    main()
