#!/usr/bin/env python3
"""
Build a historical daily gas price time-series from ONS SAP data.

The ONS publishes "System Average Price (SAP) of gas" as daily data in
pence per kWh. This script downloads the current year's Excel file, converts
to pence per therm (× 29.3071), and writes gas_price.json as {YYYY-MM-DD: price}.

Safe to re-run: entries newer than the ONS data (e.g. from OilPriceAPI) are preserved.

Run once on the server. After that, nightly_refresh.py appends each day's price.

Usage:
    python download_gas_price_history.py
    GAS_PRICE_FILE=/tmp/gas_price.json python download_gas_price_history.py
"""

import io
import json
import os
import re
from datetime import date, datetime

import openpyxl
import requests

GAS_PRICE_FILE = os.environ.get("GAS_PRICE_FILE", "/var/www/cp2030/gas_price.json")

ONS_LANDING = (
    "https://www.ons.gov.uk/economy/economicoutputandproductivity"
    "/output/datasets/systemaveragepricesapofgas"
)
ONS_BASE = "https://www.ons.gov.uk"

KWH_PER_THERM = 29.3071   # 1 therm = 29.3071 kWh
DAILY_SHEET   = "1.Daily SAP Gas"
START_DATE    = date(2026, 1, 1)


def find_xlsx_url():
    """Scrape the ONS landing page and return the URL of the latest xlsx file."""
    resp = requests.get(ONS_LANDING, timeout=30)
    resp.raise_for_status()
    # The download button href contains the versioned xlsx path
    match = re.search(
        r'href="(/file\?uri=[^"]+systemaveragepriceofgasdataset[^"]+\.xlsx)"',
        resp.text,
    )
    if not match:
        raise RuntimeError("Could not find xlsx download link on ONS page.")
    return ONS_BASE + match.group(1)


def download_and_parse(url):
    """Download the xlsx and return {YYYY-MM-DD: p_per_therm} for START_DATE onwards."""
    print(f"Downloading {url} ...")
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()

    wb = openpyxl.load_workbook(io.BytesIO(resp.content), data_only=True)
    ws = wb[DAILY_SHEET]

    prices = {}
    for row in ws.iter_rows(min_row=2, values_only=True):
        date_val, price_val = row[0], row[1]

        if not isinstance(date_val, datetime):
            continue
        if not isinstance(price_val, (int, float)):
            continue

        d = date_val.date()
        if d < START_DATE:
            continue

        p_per_therm = round(price_val * KWH_PER_THERM, 2)
        prices[d.isoformat()] = p_per_therm

    return prices


def main():
    url = find_xlsx_url()
    prices = download_and_parse(url)

    if not prices:
        print("ERROR: no prices parsed for 2026 onwards. Check the sheet structure.")
        return

    dates = sorted(prices)
    print(f"Parsed {len(dates)} daily prices  ({dates[0]} → {dates[-1]})")
    print(f"Latest: {prices[dates[-1]]:.1f} p/therm  ({dates[-1]})")

    # Load existing file — preserve entries newer than the ONS data
    existing = {}
    try:
        with open(GAS_PRICE_FILE) as f:
            data = json.load(f)
        if isinstance(data, dict) and set(data.keys()) != {"p_per_therm"}:
            existing = {k: v for k, v in data.items() if k > dates[-1]}
            if existing:
                print(f"Preserving {len(existing)} entries newer than ONS data.")
    except (FileNotFoundError, json.JSONDecodeError):
        pass

    merged = dict(sorted({**prices, **existing}.items()))

    os.makedirs(os.path.dirname(GAS_PRICE_FILE) or ".", exist_ok=True)
    tmp = GAS_PRICE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(merged, f, indent=2)
    os.replace(tmp, GAS_PRICE_FILE)

    print(f"Written {len(merged)} entries to {GAS_PRICE_FILE}")


if __name__ == "__main__":
    main()
