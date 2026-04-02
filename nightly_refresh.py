#!/usr/bin/env python3
"""
Nightly cache refresh: exports CP2030 history DB to history.json for the
replay page, and fetches + caches ENTSO-E day-ahead prices and the UK natural
gas price for the price model.

Intended to run nightly via cron:
  0 0 * * * /opt/cp2030/venv/bin/python3 /opt/cp2030/nightly_refresh.py >> /var/log/cp2030_nightly.log 2>&1

API keys are loaded from /opt/cp2030/.env (not in git). Copy via:
  scp .env user@server:/opt/cp2030/.env

Writes atomically to avoid the replay page reading a partial file.
Override paths with env vars for local testing:
  DB_FILE=/tmp/test.db OUT_FILE=/tmp/history.json python nightly_refresh.py
"""

import json
import os
import sqlite3
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

DB_FILE          = os.environ.get("DB_FILE",          "/var/www/cp2030/history.db")
OUT_FILE         = os.environ.get("OUT_FILE",          "/var/www/cp2030/history.json")
ENTSO_PRICES_FILE = os.environ.get("ENTSO_PRICES_FILE", "/var/www/cp2030/entso_prices.json")
GAS_PRICE_FILE    = os.environ.get("GAS_PRICE_FILE",    "/var/www/cp2030/gas_price.json")

errors = []

# ── 1. Export history DB to JSON ──────────────────────────────────────────────
try:
    with sqlite3.connect(DB_FILE) as con:
        con.row_factory = sqlite3.Row
        rows = con.execute("SELECT * FROM history ORDER BY timestamp").fetchall()
    data = [dict(r) for r in rows]
    tmp = OUT_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f)
    os.replace(tmp, OUT_FILE)
    print(f"Exported {len(data)} rows to {OUT_FILE}")
except Exception as e:
    print(f"ERROR: DB export failed: {e}")
    errors.append("db_export")

# ── 2. Fetch and cache ENTSO-E prices ─────────────────────────────────────────
try:
    eur_to_gbp = fetch_eur_to_gbp()
    print(f"EUR/GBP rate: {eur_to_gbp}")

    api_key = os.environ.get("ENTSO_E_API_KEY")
    prices = fetch_entso_prices(api_key, eur_to_gbp=eur_to_gbp)

    if prices:
        tmp = ENTSO_PRICES_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(prices, f)
        os.replace(tmp, ENTSO_PRICES_FILE)
        print(f"Cached ENTSO-E prices (live) to {ENTSO_PRICES_FILE}")
    elif api_key:
        print(f"WARNING: ENTSO-E returned no data — keeping existing cache at {ENTSO_PRICES_FILE}")
        errors.append("entso_empty")
    else:
        print("No ENTSO_E_API_KEY set — skipping price cache update")
except Exception as e:
    print(f"ERROR: ENTSO-E fetch failed: {e} — keeping existing cache")
    errors.append("entso_fetch")

# ── 3. Fetch and append gas price ─────────────────────────────────────────────
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
