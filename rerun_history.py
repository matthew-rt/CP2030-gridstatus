#!/usr/bin/env python3
"""
Re-simulate the full CP2030 model from 2026-01-01 using stored raw data.

Steps:
  1. Deletes history.db, records.json and state.json (asks for confirmation).
  2. Reads every settlement period from raw_history.db in chronological order.
  3. Runs the price-based dispatch model for each period.
  4. Writes results to a fresh history.db and leaves a state.json ready for
     the regular cron job to continue from.

Prerequisites:
  • raw_history.db must have been populated by download_raw_history.py.
  • entso_prices.json must exist (run generate_dummy_entso_prices.py if needed).
  • gas_price.json must exist (run nightly_refresh.py or create manually).

Usage:
    python rerun_history.py [--yes]        # --yes skips the confirmation prompt
    DB_FILE=/tmp/history.db RAW_DB_FILE=/tmp/raw_history.db python rerun_history.py
"""

import json
import os
import sqlite3
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

# Load .env (same as cp2030.py)
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    for _line in _env_path.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip())

sys.path.insert(0, os.path.dirname(__file__))

from cp2030 import (
    init_db, log_entry, update_records, save_records,
    run_model, load_gas_price, save_state,
    CP2030_BATTERY_ENERGY_MWH, CP2030_LDES_ENERGY_MWH,
)

# ── Config ────────────────────────────────────────────────────────────────────

STATE_FILE        = os.environ.get("STATE_FILE",        "/var/www/cp2030/state.json")
DB_FILE           = os.environ.get("DB_FILE",           "/var/www/cp2030/history.db")
RECORDS_FILE      = os.environ.get("RECORDS_FILE",      "/var/www/cp2030/records.json")
RAW_DB_FILE       = os.environ.get("RAW_DB_FILE",       "/var/www/cp2030/raw_history.db")
ENTSO_PRICES_FILE = os.environ.get("ENTSO_PRICES_FILE", "/var/www/cp2030/entso_prices.json")

UK_TZ = ZoneInfo("Europe/London")

PRINT_EVERY = 96  # print a progress line every N settlement periods (48 = 1 day)

# ── Helpers ───────────────────────────────────────────────────────────────────


def sp_to_utc(settlement_date_str, sp):
    """Convert a settlement date (YYYY-MM-DD) + period (1-based) to UTC datetime.
    Handles BST/GMT transitions via ZoneInfo.
    """
    d = date.fromisoformat(settlement_date_str)
    total_minutes = (sp - 1) * 30
    hours, minutes = divmod(total_minutes, 60)
    # On fall-back days there can be >48 SPs; fold=1 selects the second occurrence.
    fold = 0
    if hours >= 24:
        hours -= 24
        fold = 1
    uk_dt = datetime(d.year, d.month, d.day, hours, minutes, fold=fold, tzinfo=UK_TZ)
    return uk_dt.astimezone(timezone.utc)


def diagnose_raw_db(raw_db):
    """Print row counts per table so missing data is obvious."""
    with sqlite3.connect(raw_db) as con:
        for table in ("raw_generation", "raw_embedded", "raw_interconnectors"):
            n = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            dates = con.execute(
                f"SELECT COUNT(DISTINCT settlement_date) FROM {table}"
            ).fetchone()[0]
            print(f"  {table}: {n} rows across {dates} dates")


def load_raw_periods(raw_db):
    """Return all (settlement_date, settlement_period) pairs that have generation data.
    Embedded and interconnector data are optional — fallbacks are applied in load_raw_sp.
    """
    with sqlite3.connect(raw_db) as con:
        rows = con.execute("""
            SELECT DISTINCT settlement_date, settlement_period
            FROM raw_generation
            ORDER BY settlement_date, settlement_period
        """).fetchall()
    return rows


def load_raw_sp(raw_db, settlement_date, sp):
    """Load one settlement period of raw data and return (elexon, neso, ic_records).

    Embedded data may be sparse (NESO API has a short rolling window). If the exact
    date is missing, falls back to the nearest available record with the same SP number
    (same time of day) to preserve the diurnal solar/wind pattern.

    Interconnector data falls back to empty list if missing (small demand error).
    """
    with sqlite3.connect(raw_db) as con:
        gen_rows = con.execute(
            "SELECT fuel_type, generation_mw FROM raw_generation "
            "WHERE settlement_date=? AND settlement_period=?",
            (settlement_date, sp),
        ).fetchall()

        # Exact embedded match first
        emb_row = con.execute(
            "SELECT embedded_wind_mw, embedded_wind_capacity_mw, "
            "       embedded_solar_mw, embedded_solar_capacity_mw "
            "FROM raw_embedded WHERE settlement_date=? AND settlement_period=?",
            (settlement_date, sp),
        ).fetchone()

        # Fallback: nearest date with the same settlement period
        if emb_row is None:
            emb_row = con.execute(
                "SELECT embedded_wind_mw, embedded_wind_capacity_mw, "
                "       embedded_solar_mw, embedded_solar_capacity_mw "
                "FROM raw_embedded WHERE settlement_period=? "
                "ORDER BY ABS(JULIANDAY(settlement_date) - JULIANDAY(?)) LIMIT 1",
                (sp, settlement_date),
            ).fetchone()

        ic_rows = con.execute(
            "SELECT ic_name, generation_mw FROM raw_interconnectors "
            "WHERE settlement_date=? AND settlement_period=?",
            (settlement_date, sp),
        ).fetchall()

    elexon = {fuel: mw for fuel, mw in gen_rows}

    if emb_row is not None:
        neso = {
            "embedded_wind_mw":           emb_row[0],
            "embedded_wind_capacity_mw":  emb_row[1],
            "embedded_solar_mw":          emb_row[2],
            "embedded_solar_capacity_mw": emb_row[3],
        }
    else:
        # No embedded data at all — use zeros (model will still run)
        neso = {
            "embedded_wind_mw": 0, "embedded_wind_capacity_mw": 1,
            "embedded_solar_mw": 0, "embedded_solar_capacity_mw": 1,
        }

    ic_records = [{"interconnectorName": name, "generation": mw} for name, mw in ic_rows]
    return elexon, neso, ic_records


# ── Main ──────────────────────────────────────────────────────────────────────


def main():
    skip_confirm = "--yes" in sys.argv

    # Sanity checks
    if not os.path.exists(RAW_DB_FILE):
        print(f"ERROR: raw_history.db not found at {RAW_DB_FILE}")
        print("Run download_raw_history.py first.")
        sys.exit(1)

    if not os.path.exists(ENTSO_PRICES_FILE):
        print(f"ERROR: {ENTSO_PRICES_FILE} not found.")
        print("Run generate_dummy_entso_prices.py first.")
        sys.exit(1)

    print("Raw DB contents:")
    diagnose_raw_db(RAW_DB_FILE)

    periods = load_raw_periods(RAW_DB_FILE)
    if not periods:
        print("ERROR: raw DB has no periods with both generation and embedded data.")
        print("Re-run download_raw_history.py and check for errors.")
        sys.exit(1)

    first_sp = f"{periods[0][0]}  SP{periods[0][1]}"
    last_sp  = f"{periods[-1][0]}  SP{periods[-1][1]}"
    print(f"Raw data spans {len(periods)} settlement periods")
    print(f"  First: {first_sp}")
    print(f"  Last:  {last_sp}")
    print()

    if not skip_confirm:
        print(f"This will DELETE and rebuild:")
        print(f"  {DB_FILE}")
        print(f"  {RECORDS_FILE}")
        print(f"  {STATE_FILE}")
        answer = input("Proceed? [y/N] ").strip().lower()
        if answer != "y":
            print("Aborted.")
            sys.exit(0)

    # ── Reset output files ───────────────────────────────────────────────────
    for path in [DB_FILE, RECORDS_FILE, STATE_FILE]:
        if os.path.exists(path):
            os.remove(path)
            print(f"Deleted {path}")

    init_db(DB_FILE)

    # ── Load price inputs ────────────────────────────────────────────────────
    # run_model() loads gas_price.json and entso_prices.json from the standard
    # file paths on every call. Ensure they exist before starting.
    gas_p = load_gas_price()
    if gas_p is None:
        print("WARNING: gas_price.json not found — model will use its default gas price.")

    # ── Initial state ────────────────────────────────────────────────────────
    state = {
        "battery_soc_mwh": CP2030_BATTERY_ENERGY_MWH // 2,
        "ldes_soc_mwh":    CP2030_LDES_ENERGY_MWH // 2,
        "current": {},
        "history": [],
        "last_updated": None,
    }

    records_snapshot = None
    print(f"\nSimulating {len(periods)} periods...")

    # ── Main loop ────────────────────────────────────────────────────────────
    for i, (settlement_date, sp) in enumerate(periods):
        elexon, neso, ic_records = load_raw_sp(RAW_DB_FILE, settlement_date, sp)

        # Historical timestamp: start of this settlement period in UTC
        ts = sp_to_utc(settlement_date, sp)

        # Run the model. interactive=False so it uses INSERT OR IGNORE logic,
        # but timestamp overrides the clock so the historical time is used.
        # gas price and ENTSO prices are loaded from their files by run_model().
        state = run_model(
            elexon, neso, ic_records, state,
            interactive=False,
            timestamp=ts,
        )

        # Log to DB (INSERT OR REPLACE since we own this fresh DB)
        log_entry(DB_FILE, state["current"], overwrite=True)

        # Update records table
        records_snapshot = update_records(DB_FILE, state["current"])

        if (i + 1) % PRINT_EVERY == 0 or i == len(periods) - 1:
            entry = state["current"]
            pct   = (i + 1) / len(periods) * 100
            print(
                f"  {pct:5.1f}%  {entry['timestamp'][:16]}  "
                f"price={entry.get('wholesale_price_gbp', '?'):.0f} £/MWh  "
                f"gas={entry.get('gas_mw', 0):5.0f} MW  "
                f"bat={state['battery_soc_mwh']/1000:.1f} GWh"
            )

    # ── Save outputs ─────────────────────────────────────────────────────────
    if records_snapshot:
        save_records(records_snapshot, RECORDS_FILE)

    save_state(state)
    print(f"\nDone. State written to {STATE_FILE}")
    print(f"History DB: {DB_FILE}  ({len(periods)} rows)")


if __name__ == "__main__":
    main()
