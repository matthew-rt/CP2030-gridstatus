#!/usr/bin/env python3
"""
Download raw historical generation data from START_DATE to yesterday and store
in raw_history.db for future re-simulation.

Safe to re-run: dates already present in the DB are skipped.
The regular cron job (cp2030.py) keeps the DB current from today onwards.

Usage:
    python download_raw_history.py
    RAW_DB_FILE=/tmp/raw_history.db python download_raw_history.py

API notes
---------
Generation (by fuel type): Elexon FUELINST dataset (5-minute intervals).
    GET /datasets/FUELINST?publishDateTimeFrom=...&publishDateTimeTo=...
    Last reading per settlement period is used — same snapshot the live cron captures.

Interconnectors: needed for accurate actual demand (domestic gen + net IC + embedded).
    The CP2030 dispatch model is price-based and ignores these, but the demand
    calculation uses them to determine how much the UK actually consumed.

Embedded wind/solar: NESO datastore, bulk SQL query.
"""

import os
import sqlite3
import sys
import time
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib import parse
from zoneinfo import ZoneInfo

import requests

# ── Config ────────────────────────────────────────────────────────────────────

RAW_DB_FILE = os.environ.get("RAW_DB_FILE", "/var/www/cp2030/raw_history.db")
START_DATE  = date(2026, 1, 1)
DELAY       = 0.4   # seconds between API calls (be polite)

UK_TZ = ZoneInfo("Europe/London")

ELEXON_FUEL_URL = "https://data.elexon.co.uk/bmrs/api/v1/datasets/FUELINST"
ELEXON_IC_URL   = "https://data.elexon.co.uk/bmrs/api/v1/generation/outturn/interconnectors"
NESO_URL        = "https://api.neso.energy/api/3/action/datastore_search_sql"
NESO_DATASET    = "db6c038f-98af-4570-ab60-24d71ebd0ae5"

# ── DB init ───────────────────────────────────────────────────────────────────


def init_db(db_path):
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    with sqlite3.connect(db_path) as con:
        con.executescript("""
            CREATE TABLE IF NOT EXISTS raw_generation (
                settlement_date   TEXT,
                settlement_period INTEGER,
                fuel_type         TEXT,
                generation_mw     REAL,
                PRIMARY KEY (settlement_date, settlement_period, fuel_type)
            );
            CREATE TABLE IF NOT EXISTS raw_embedded (
                settlement_date            TEXT,
                settlement_period          INTEGER,
                embedded_wind_mw           REAL,
                embedded_wind_capacity_mw  REAL,
                embedded_solar_mw          REAL,
                embedded_solar_capacity_mw REAL,
                PRIMARY KEY (settlement_date, settlement_period)
            );
            CREATE TABLE IF NOT EXISTS raw_interconnectors (
                settlement_date   TEXT,
                settlement_period INTEGER,
                ic_name           TEXT,
                generation_mw     REAL,
                PRIMARY KEY (settlement_date, settlement_period, ic_name)
            );
        """)


def stored_generation_dates(db_path):
    """Return set of settlement_date strings already in raw_generation."""
    with sqlite3.connect(db_path) as con:
        rows = con.execute(
            "SELECT DISTINCT settlement_date FROM raw_generation"
        ).fetchall()
    return {row[0] for row in rows}


def stored_embedded_dates(db_path):
    with sqlite3.connect(db_path) as con:
        rows = con.execute(
            "SELECT DISTINCT settlement_date FROM raw_embedded"
        ).fetchall()
    return {row[0] for row in rows}


# ── Elexon generation (FUELINST 5-min data → grouped by settlement period) ────


def fetch_generation_day(d):
    """Fetch FUELINST (5-min data) for one day, grouped by settlement period.
    Takes the last reading per (SP, fuel type) — same snapshot the live cron captures.
    Returns {sp: {fuel_type: generation_mw}}.
    """
    from_dt = f"{d.isoformat()}T00:00:00Z"
    to_dt   = f"{(d + timedelta(days=1)).isoformat()}T00:00:00Z"

    all_records = []
    url    = ELEXON_FUEL_URL
    params = {"publishDateTimeFrom": from_dt, "publishDateTimeTo": to_dt,
              "format": "json"}

    while url:
        resp = requests.get(url, params=params, timeout=60)
        resp.raise_for_status()
        body   = resp.json()
        chunk  = body if isinstance(body, list) else body.get("data", [])
        all_records.extend(chunk)
        url    = body.get("_links", {}).get("next") if isinstance(body, dict) else None
        params = {}  # next link is already a full URL
        if url:
            time.sleep(DELAY)

    if not all_records:
        raise ValueError(f"No FUELINST data for {d.isoformat()}")

    # Group by (sp, publishTime) → {fuel: mw}; keep latest publishTime per SP
    sp_pub_fuels = defaultdict(lambda: defaultdict(float))  # (sp, pub) -> {fuel: mw}
    sp_latest    = {}  # sp -> latest publishTime string

    for r in all_records:
        sp = r.get("settlementPeriod")
        if sp is None:
            # Compute from startTime
            raw_start = r.get("startTime", "")
            if not raw_start:
                continue
            start = datetime.fromisoformat(raw_start.replace("Z", "+00:00"))
            uk    = start.astimezone(UK_TZ)
            if uk.date() != d:
                continue
            sp = uk.hour * 2 + uk.minute // 30 + 1

        pub  = r.get("publishTime", "")
        fuel = r.get("fuelType", "")
        mw   = float(r.get("generation") or r.get("currentUsage") or 0.0)

        sp_pub_fuels[(sp, pub)][fuel] = mw
        if pub > sp_latest.get(sp, ""):
            sp_latest[sp] = pub

    return {sp: dict(sp_pub_fuels[(sp, pub)]) for sp, pub in sp_latest.items()}


# ── Elexon interconnectors ────────────────────────────────────────────────────


def fetch_interconnectors_day(d):
    """Fetch interconnector flows for one day.
    Returns {sp: [{"interconnectorName": ..., "generation": ...}]}.
    """
    date_str = d.isoformat()
    resp = requests.get(
        ELEXON_IC_URL,
        params={"settlementDateFrom": date_str, "settlementDateTo": date_str},
        timeout=60,
    )
    resp.raise_for_status()
    records = resp.json().get("data", [])

    result = defaultdict(list)
    for r in records:
        sp = r.get("settlementPeriod")
        if sp:
            result[int(sp)].append({
                "interconnectorName": r["interconnectorName"],
                "generation":         float(r["generation"]),
            })
    return dict(result)


# ── NESO embedded wind/solar ──────────────────────────────────────────────────


def fetch_neso_bulk(start_date):
    """Fetch all NESO embedded wind/solar records from start_date onwards.
    Returns a list of raw records (one per settlement period).
    Paginates automatically if the API returns fewer records than expected.
    """
    start_str = f"{start_date.isoformat()}T00:00:00"
    all_records = []
    offset = 0
    page_size = 5000

    while True:
        sql = (
            f'SELECT * FROM "{NESO_DATASET}" '
            f'WHERE "SETTLEMENT_DATE" >= \'{start_str}\' '
            f'ORDER BY "SETTLEMENT_DATE", "SETTLEMENT_PERIOD" '
            f'LIMIT {page_size} OFFSET {offset}'
        )
        resp = requests.get(
            NESO_URL, params=parse.urlencode({"sql": sql}), timeout=60
        )
        resp.raise_for_status()
        page = resp.json()["result"]["records"]
        all_records.extend(page)
        if len(page) < page_size:
            break
        offset += page_size
        time.sleep(DELAY)

    return all_records


# ── Persistence ───────────────────────────────────────────────────────────────


def save_generation(db_path, d, sp_data):
    date_str = d.isoformat()
    with sqlite3.connect(db_path) as con:
        for sp, fuels in sp_data.items():
            for fuel, mw in fuels.items():
                con.execute(
                    "INSERT OR REPLACE INTO raw_generation VALUES (?,?,?,?)",
                    (date_str, sp, fuel, mw),
                )


def save_interconnectors(db_path, d, sp_data):
    date_str = d.isoformat()
    with sqlite3.connect(db_path) as con:
        for sp, records in sp_data.items():
            for r in records:
                con.execute(
                    "INSERT OR REPLACE INTO raw_interconnectors VALUES (?,?,?,?)",
                    (date_str, sp, r["interconnectorName"], r["generation"]),
                )


def save_embedded(db_path, records):
    with sqlite3.connect(db_path) as con:
        for r in records:
            date_str = r["SETTLEMENT_DATE"][:10]
            sp = int(r["SETTLEMENT_PERIOD"])
            con.execute(
                "INSERT OR REPLACE INTO raw_embedded VALUES (?,?,?,?,?,?)",
                (date_str, sp,
                 r["EMBEDDED_WIND_FORECAST"],
                 r["EMBEDDED_WIND_CAPACITY"],
                 r["EMBEDDED_SOLAR_FORECAST"],
                 r["EMBEDDED_SOLAR_CAPACITY"]),
            )


# ── Main ──────────────────────────────────────────────────────────────────────


def main():
    print(f"Raw DB: {RAW_DB_FILE}")
    init_db(RAW_DB_FILE)

    today = date.today()

    # ── Elexon generation ────────────────────────────────────────────────────
    have_gen = stored_generation_dates(RAW_DB_FILE)
    dates_needed = [
        d for d in (START_DATE + timedelta(days=i)
                    for i in range((today - START_DATE).days + 1))
        if d.isoformat() not in have_gen
    ]

    print(f"Generation dates already stored: {len(have_gen)}")
    print(f"Generation dates to download:    {len(dates_needed)}")

    failed_gen = []
    for i, d in enumerate(dates_needed):
        print(f"  [{i+1}/{len(dates_needed)}] {d.isoformat()}...", end=" ", flush=True)
        try:
            gen = fetch_generation_day(d)
            save_generation(RAW_DB_FILE, d, gen)

            ic = fetch_interconnectors_day(d)
            save_interconnectors(RAW_DB_FILE, d, ic)

            print(f"OK ({len(gen)} SPs)")
        except Exception as e:
            print(f"FAILED: {e}")
            failed_gen.append(d)
        time.sleep(DELAY)

    # ── NESO embedded ────────────────────────────────────────────────────────
    have_emb = stored_embedded_dates(RAW_DB_FILE)
    # Re-fetch if any needed date is missing
    emb_missing = [
        d for d in (START_DATE + timedelta(days=i)
                    for i in range((today - START_DATE).days + 1))
        if d.isoformat() not in have_emb
    ]

    if emb_missing:
        print(f"\nFetching NESO embedded data ({len(emb_missing)} dates missing)...")
        try:
            neso_records = fetch_neso_bulk(START_DATE)
            save_embedded(RAW_DB_FILE, neso_records)
            print(f"  Stored {len(neso_records)} NESO records.")
        except Exception as e:
            print(f"  NESO fetch FAILED: {e}")
    else:
        print("\nNESO embedded data: all dates present, skipping.")

    if failed_gen:
        print(f"\nWARNING: {len(failed_gen)} dates failed generation download:")
        for d in failed_gen:
            print(f"  {d.isoformat()}")
        print("Re-run the script to retry these dates.")
    else:
        print("\nAll dates downloaded successfully.")


if __name__ == "__main__":
    main()
