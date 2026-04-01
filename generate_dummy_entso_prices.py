#!/usr/bin/env python3
"""
Generate a dummy entso_prices.json for testing before the ENTSO-E API key arrives.

Produces 24 hourly prices for today (UTC) for each bidding zone, with mild
random variation around typical values. Run once on the server:

    python generate_dummy_entso_prices.py

Override output path with:
    ENTSO_PRICES_FILE=/tmp/entso_prices.json python generate_dummy_entso_prices.py
"""

import json
import math
import os
from datetime import datetime, timedelta, timezone

OUT_FILE = os.environ.get("ENTSO_PRICES_FILE", "/var/www/cp2030/entso_prices.json")

# Typical day-ahead prices in GBP/MWh per zone (roughly consistent with
# the defaults in cp2030price.py INTERCONNECTORS list).
# A simple daily shape is applied: lower overnight, peak morning/evening.
ZONE_BASE_PRICES = {
    "FR":  65,
    "BE":  64,
    "NL":  63,
    "NO2": 50,
    "DK1": 60,
    "DE":  62,
    "IE":  68,
}

def daily_shape(hour):
    """Simple price shape: trough at 04:00, peaks at 08:00 and 18:00 UTC."""
    return (
        -5 * math.cos(2 * math.pi * (hour - 4) / 24)          # overnight trough
        + 3 * math.exp(-0.5 * ((hour - 8) / 2) ** 2)          # morning peak
        + 4 * math.exp(-0.5 * ((hour - 18) / 2) ** 2)         # evening peak
    )

now_utc = datetime.now(timezone.utc)
today_midnight = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)

prices = {}
for zone, base in ZONE_BASE_PRICES.items():
    zone_prices = {}
    for h in range(24):
        ts = today_midnight + timedelta(hours=h)
        price = round(base + daily_shape(h), 2)
        zone_prices[ts.strftime("%Y-%m-%dT%H:%M:%SZ")] = price
    prices[zone] = zone_prices

os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
tmp = OUT_FILE + ".tmp"
with open(tmp, "w") as f:
    json.dump(prices, f, indent=2)
os.replace(tmp, OUT_FILE)

print(f"Wrote dummy ENTSO-E prices ({len(prices)} zones × 24 hours) to {OUT_FILE}")
for zone, zp in prices.items():
    vals = list(zp.values())
    print(f"  {zone:4s}  min={min(vals):.1f}  max={max(vals):.1f}  GBP/MWh")
