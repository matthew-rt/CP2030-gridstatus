#!/usr/bin/env python3
"""
Generate a dummy entso_prices.json covering every hour from 2026-01-01 to the
current hour, for testing before the ENTSO-E API key arrives.

Uses the same daily price shape as generate_dummy_entso_prices.py so re-simulation
gets realistic time-of-day variation rather than a flat price.

Run once on the server (overwrites any existing entso_prices.json):

    python generate_dummy_entso_history.py

Override output path with:
    ENTSO_PRICES_FILE=/tmp/entso_prices.json python generate_dummy_entso_history.py

Once the real ENTSO-E API key arrives, run nightly_refresh.py instead.
"""

import json
import math
import os
from datetime import datetime, timedelta, timezone

OUT_FILE   = os.environ.get("ENTSO_PRICES_FILE", "/var/www/cp2030/entso_prices.json")
START_DATE = datetime(2026, 1, 1, tzinfo=timezone.utc)

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
    """Simple price shape: trough at 04:00 UTC, peaks at 08:00 and 18:00 UTC."""
    return (
        -5 * math.cos(2 * math.pi * (hour - 4) / 24)
        + 3 * math.exp(-0.5 * ((hour - 8) / 2) ** 2)
        + 4 * math.exp(-0.5 * ((hour - 18) / 2) ** 2)
    )


now_utc = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)

prices = {zone: {} for zone in ZONE_BASE_PRICES}

ts = START_DATE
while ts <= now_utc:
    shape = daily_shape(ts.hour)
    for zone, base in ZONE_BASE_PRICES.items():
        prices[zone][ts.strftime("%Y-%m-%dT%H:%M:%SZ")] = round(base + shape, 2)
    ts += timedelta(hours=1)

total_hours = sum(len(v) for v in prices.values()) // len(prices)
print(f"Generating {total_hours} hourly entries per zone "
      f"({START_DATE.date()} → {now_utc.date()})...")

os.makedirs(os.path.dirname(OUT_FILE) or ".", exist_ok=True)
tmp = OUT_FILE + ".tmp"
with open(tmp, "w") as f:
    json.dump(prices, f)
os.replace(tmp, OUT_FILE)

print(f"Wrote dummy ENTSO-E prices ({len(prices)} zones × {total_hours} hours) to {OUT_FILE}")
for zone, zp in prices.items():
    vals = list(zp.values())
    print(f"  {zone:4s}  min={min(vals):.1f}  max={max(vals):.1f}  GBP/MWh")
