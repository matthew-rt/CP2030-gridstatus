#!/usr/bin/env python3
"""
Nightly cache refresh: fetches and caches ENTSO-E day-ahead prices and the UK
natural gas price for the price model.

History DB export (history.json) is now handled by cp2030.py on every run.

Intended to run nightly via cron:
  0 23 * * * /opt/cp2030/venv/bin/python3 /opt/cp2030/nightly_refresh.py >> /var/log/cp2030_nightly.log 2>&1

API keys are loaded from /opt/cp2030/.env (not in git). Copy via:
  scp .env user@server:/opt/cp2030/.env
"""

import json
import os
import sys
from pathlib import Path

# Load .env from the same directory as this script (not committed to git)
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    for _line in _env_path.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip())

sys.path.insert(0, os.path.dirname(__file__))
from cp2030price import fetch_entso_prices, fetch_gas_price, fetch_eur_to_gbp

ENTSO_PRICES_FILE = os.environ.get("ENTSO_PRICES_FILE", "/var/www/cp2030/entso_prices.json")
GAS_PRICE_FILE    = os.environ.get("GAS_PRICE_FILE",    "/var/www/cp2030/gas_price.json")

errors = []

# ── 1. Fetch and cache ENTSO-E prices ─────────────────────────────────────────
try:
    eur_to_gbp = fetch_eur_to_gbp()
    print(f"EUR/GBP rate: {eur_to_gbp}")

    api_key = os.environ.get("ENTSO_E_API_KEY")
    prices = fetch_entso_prices(api_key, eur_to_gbp=eur_to_gbp)

    if prices:
        # Merge fresh 3-day window into existing cache so historical data
        # accumulates instead of being overwritten each night.
        existing = {}
        if os.path.exists(ENTSO_PRICES_FILE):
            try:
                with open(ENTSO_PRICES_FILE) as f:
                    existing = json.load(f)
            except Exception as e:
                print(f"WARNING: could not read existing cache ({e}) — starting fresh")
                existing = {}

        added = 0
        for area_key, area_prices in prices.items():
            if area_key not in existing:
                existing[area_key] = {}
            before = len(existing[area_key])
            existing[area_key].update(area_prices)
            added += len(existing[area_key]) - before

        tmp = ENTSO_PRICES_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(existing, f)
        os.replace(tmp, ENTSO_PRICES_FILE)
        total = sum(len(v) for v in existing.values())
        print(
            f"Merged ENTSO-E prices into {ENTSO_PRICES_FILE} "
            f"(+{added} new points, {total} total across {len(existing)} zones)"
        )
    elif api_key:
        print(f"WARNING: ENTSO-E returned no data — keeping existing cache at {ENTSO_PRICES_FILE}")
        errors.append("entso_empty")
    else:
        print("No ENTSO_E_API_KEY set — skipping price cache update")
except Exception as e:
    print(f"ERROR: ENTSO-E fetch failed: {e} — keeping existing cache")
    errors.append("entso_fetch")

# ── 2. Fetch and append gas price ─────────────────────────────────────────────
try:
    from datetime import date as _date
    gas_api_key = os.environ.get("OIL_PRICE_API_KEY")
    gas_price_p_per_therm, gas_price_source = fetch_gas_price(gas_api_key)

    try:
        with open(GAS_PRICE_FILE) as f:
            gas_prices = json.load(f)
        if not isinstance(gas_prices, dict) or set(gas_prices.keys()) == {"p_per_therm"}:
            gas_prices = {}
    except (FileNotFoundError, json.JSONDecodeError):
        gas_prices = {}

    gas_prices[_date.today().isoformat()] = gas_price_p_per_therm
    tmp = GAS_PRICE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(dict(sorted(gas_prices.items())), f)
    os.replace(tmp, GAS_PRICE_FILE)
    print(f"Appended gas price ({gas_price_source}) {gas_price_p_per_therm}p/therm for {_date.today()} to {GAS_PRICE_FILE}")
    if gas_price_source not in ("live", "no_key"):
        print(f"WARNING: gas price API call failed — used fallback default. Check OIL_PRICE_API_KEY and API status.")
except Exception as e:
    print(f"ERROR: gas price fetch failed: {e}")
    errors.append("gas_price")

if errors:
    print(f"nightly_refresh completed with errors: {errors}")
    sys.exit(1)
