#!/usr/bin/env python3
"""
CP2030 Grid Tracker
Estimates what the GB electricity grid mix would look like with CP2030 capacity targets,
using live generation data from Elexon BMRS and NESO.
Runs every 30 minutes via cron and writes a JSON file served by Caddy.
"""

import json
import os
import sqlite3
import sys
import time
import requests
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib import parse
from zoneinfo import ZoneInfo

# Load .env from the same directory (not committed to git)
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    for _line in _env_path.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip())

sys.path.insert(0, os.path.dirname(__file__))
from cp2030price import (
    estimate_wholesale_price,
    load_entso_prices,
    INTERCONNECTORS,
    IC_AREA,
    CP2030_BIOMASS_MW,
    CP2030_GAS_MW,
    CP2030_BATTERY_POWER_MW,
    CP2030_BATTERY_ENERGY_MWH,
    CP2030_LDES_POWER_MW,
    CP2030_LDES_ENERGY_MWH,
    BATTERY_EFFICIENCY,
    LDES_EFFICIENCY,
)

UK_TZ = ZoneInfo("Europe/London")

# ── CP2030 Target Capacities ─────────────────────────────────────────────────
# Storage, gas and biomass capacities live in cp2030price.py (imported above).
CP2030_OFFSHORE_WIND_MW = 47_000
CP2030_ONSHORE_WIND_MW = 28_000
CP2030_SOLAR_MW = 46_000
CP2030_NUCLEAR_MW = 3_800
CP2030_HYDRO_MW = 1_870

# ── Current (2026) Capacities ────────────────────────────────────────────────
# Used to calculate load factors. Update periodically as new capacity is built.
CURRENT_NUCLEAR_CAPACITY_MW = 4_685  # Heysham 1&2, Torness, Sizewell B
CURRENT_OFFSHORE_WIND_CAPACITY_MW = 14_279
CURRENT_TOTAL_ONSHORE_WIND_CAPACITY_MW = 15_270

# New-build offshore wind (capacity beyond today's fleet) is assumed to have a
# 15% higher load factor than the current fleet, reflecting larger modern turbines.
# The resulting load factor is capped at 1.0.
NEW_OFFSHORE_LF_MULTIPLIER = 1.15

# ── Model Parameters ─────────────────────────────────────────────────────────
DEMAND_UPLIFT_MW = 4_000  # Additional CP2030 demand vs 2026 (electrification)

# ── Runtime Config ───────────────────────────────────────────────────────────
# Override with env vars for local testing: STATE_FILE=/tmp/state.json python cp2030.py
STATE_FILE = os.environ.get("STATE_FILE", "/var/www/cp2030/state.json")
DB_FILE = os.environ.get("DB_FILE", "/var/www/cp2030/history.db")
RECORDS_FILE = os.environ.get("RECORDS_FILE", "/var/www/cp2030/records.json")
ENTSO_PRICES_FILE = os.environ.get(
    "ENTSO_PRICES_FILE", "/var/www/cp2030/entso_prices.json"
)
GAS_PRICE_FILE = os.environ.get("GAS_PRICE_FILE", "/var/www/cp2030/gas_price.json")
RAW_DB_FILE = os.environ.get("RAW_DB_FILE", "/var/www/cp2030/raw_history.db")
HISTORY_FILE = os.environ.get("HISTORY_FILE", "/var/www/cp2030/history.json")
HISTORY_SIZE = 48  # 24 hours of half-hourly readings

# ── API ──────────────────────────────────────────────────────────────────────
ELEXON_URL = (
    "https://data.elexon.co.uk/bmrs/api/v1/generation/outturn/current?format=json"
)
ELEXON_IC_URL = (
    "https://data.elexon.co.uk/bmrs/api/v1/generation/outturn/interconnectors"
)
NESO_URL = "https://api.neso.energy/api/3/action/datastore_search_sql"
NESO_DATASET = "db6c038f-98af-4570-ab60-24d71ebd0ae5"

# Used only for demand calculation fallback from the main Elexon endpoint.
INTERCONNECTOR_FUELS = {
    "INTELEC",
    "INTEW",
    "INTFR",
    "INTGRNL",
    "INTIFA2",
    "INTIRL",
    "INTNED",
    "INTNEM",
    "INTNSL",
    "INTVKL",
}

# Physical capacity of each interconnector in MW, matched by substring of the
# interconnectorName field returned by the INTOUTHH endpoint.
# Order matters: IFA2 must appear before IFA to avoid partial match.
IC_NAME_CAPACITY = [
    ("INTELEC", 1_000),  # ElecLink (France)
    ("East-West", 500),  # East-West (Ireland)
    ("IFA2", 1_000),  # IFA2 (France)
    ("IFA", 2_000),  # IFA (France)
    ("Moyle", 500),  # Moyle (N. Ireland)
    ("BritNed", 1_000),  # BritNed (Netherlands)
    ("Nemolink", 1_000),  # NemoLink (Belgium)
    ("North Sea", 1_400),  # North Sea Link (Norway)
    ("Viking", 1_400),  # Viking Link (Denmark)
    ("Greenlink", 500),  # Greenlink (Ireland)
]


def ic_capacity(name):
    """Return physical capacity in MW for an interconnector by name substring."""
    for substr, cap in IC_NAME_CAPACITY:
        if substr in name:
            return cap
    return 0


# ── Timestamp helpers ─────────────────────────────────────────────────────────


def _round_timestamp(dt, floor=False):
    """Round a UTC datetime to the nearest half hour.
    If floor=True, truncate down (used for interactive runs targeting the current period).
    """
    secs = dt.minute * 60 + dt.second + dt.microsecond / 1_000_000
    if floor or secs < 15 * 60:
        new_min = 0 if dt.minute < 30 else 30
        return dt.replace(minute=new_min, second=0, microsecond=0)
    elif secs < 45 * 60:
        return dt.replace(minute=30, second=0, microsecond=0)
    else:
        return dt.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)


# ── Data Fetching ─────────────────────────────────────────────────────────────


def _with_retry(fn, retries=3, backoff=2):
    """Call fn(), retrying up to `retries` times with exponential backoff."""
    for attempt in range(retries):
        try:
            return fn()
        except Exception as e:
            if attempt == retries - 1:
                raise
            wait = backoff**attempt  # 1s, 2s, 4s
            print(f"Attempt {attempt + 1} failed: {e} — retrying in {wait}s")
            time.sleep(wait)


def current_settlement_period():
    """Return (iso_date_str, settlement_period) for the current UK local time.
    Settlement periods are defined in UK local time (GMT/BST), so we must convert
    regardless of where the server is located."""
    now = datetime.now(UK_TZ)
    sp = now.hour * 2 + now.minute // 30 + 1
    date_str = now.strftime("%Y-%m-%dT00:00:00.000Z")
    return date_str, sp


def fetch_elexon():
    """Fetch current generation outturn from Elexon BMRS.
    Returns a dict of {fuelType: currentUsage_MW}."""

    def _fetch():
        resp = requests.get(ELEXON_URL, timeout=30)
        resp.raise_for_status()
        return {item["fuelType"]: item["currentUsage"] for item in resp.json()}

    return _with_retry(_fetch)


def _neso_query(sql):
    def _fetch():
        resp = requests.get(NESO_URL, params=parse.urlencode({"sql": sql}), timeout=30)
        resp.raise_for_status()
        return resp.json()["result"]["records"]

    return _with_retry(_fetch)


def fetch_neso(date_str, sp):
    """Fetch embedded wind and solar forecast from NESO.
    Falls back to the most recent available record if the current SP isn't published yet.
    """
    records = _neso_query(
        f'SELECT * FROM "{NESO_DATASET}" '
        f"WHERE \"SETTLEMENT_DATE\" >= '{date_str}' AND \"SETTLEMENT_PERIOD\" = '{sp}' "
        f'ORDER BY "_id" DESC LIMIT 1'
    )
    if not records:
        records = _neso_query(
            f'SELECT * FROM "{NESO_DATASET}" '
            f'ORDER BY "SETTLEMENT_DATE" DESC, "SETTLEMENT_PERIOD" DESC LIMIT 1'
        )
    if not records:
        raise RuntimeError("No NESO embedded generation data available")

    r = records[0]
    return {
        "embedded_wind_mw": r["EMBEDDED_WIND_FORECAST"],
        "embedded_wind_capacity_mw": r["EMBEDDED_WIND_CAPACITY"],
        "embedded_solar_mw": r["EMBEDDED_SOLAR_FORECAST"],
        "embedded_solar_capacity_mw": r["EMBEDDED_SOLAR_CAPACITY"],
    }


def fetch_interconnectors(date_str, sp):
    """Fetch per-interconnector flows from the INTOUTHH endpoint.
    Returns a list of records for the current settlement period, falling back
    to the most recent available period if current SP isn't published yet.
    Each record has 'interconnectorName' and 'generation' (MW, negative = exporting).
    """
    date_only = date_str[:10]

    def _fetch():
        resp = requests.get(
            ELEXON_IC_URL,
            params={"settlementDateFrom": date_only, "settlementDateTo": date_only},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()["data"]

    all_records = _with_retry(_fetch)

    period_records = [r for r in all_records if r["settlementPeriod"] == sp]
    if not period_records and all_records:
        latest_sp = max(r["settlementPeriod"] for r in all_records)
        period_records = [r for r in all_records if r["settlementPeriod"] == latest_sp]

    if not period_records:
        raise RuntimeError("No interconnector data available")
    return period_records


# ── Model ─────────────────────────────────────────────────────────────────────


def actual_demand(elexon, neso, ic_records):
    """Calculate actual GB system demand in MW.
    demand = domestic generation (incl. PS net) + net interconnectors + embedded gen.
    Uses the INTOUTHH endpoint for interconnectors as the main Elexon endpoint
    caps flows at zero and misses exports."""
    domestic = {
        "BIOMASS",
        "CCGT",
        "COAL",
        "NPSHYD",
        "NUCLEAR",
        "OCGT",
        "OIL",
        "OTHER",
        "PS",
        "WIND",
    }
    net_ic = sum(r["generation"] for r in ic_records)
    return (
        sum(elexon.get(f, 0) for f in domestic)
        + net_ic
        + neso["embedded_wind_mw"]
        + neso["embedded_solar_mw"]
    )


def cp2030_generation(elexon, neso):
    """Scale current generation to CP2030 capacities using load factors."""
    # Onshore wind LF: use embedded (distribution-level) wind as a proxy,
    # assuming all embedded wind is onshore and shares the same load factor
    # as transmission-connected onshore.
    emb_wind_capacity = neso["embedded_wind_capacity_mw"]
    onshore_lf = (
        neso["embedded_wind_mw"] / emb_wind_capacity if emb_wind_capacity > 0 else 0
    )

    # Offshore wind LF: subtract estimated transmission onshore output from
    # total Elexon wind to isolate the offshore contribution.
    # Note: emb_wind_capacity varies by settlement period (it's a forecast availability
    # figure, not fixed installed capacity), so this split will fluctuate slightly.
    trans_onshore_capacity = CURRENT_TOTAL_ONSHORE_WIND_CAPACITY_MW - emb_wind_capacity
    trans_onshore_output = onshore_lf * trans_onshore_capacity
    offshore_output = elexon.get("WIND", 0) - trans_onshore_output

    offshore_lf = (
        offshore_output / CURRENT_OFFSHORE_WIND_CAPACITY_MW
        if CURRENT_OFFSHORE_WIND_CAPACITY_MW
        else 0
    )

    # New-build offshore capacity uses a higher load factor (modern larger turbines).
    # Cap both at 1.0 to avoid physically impossible output.
    new_offshore_mw = CP2030_OFFSHORE_WIND_MW - CURRENT_OFFSHORE_WIND_CAPACITY_MW
    offshore_mw = (
        offshore_lf * CURRENT_OFFSHORE_WIND_CAPACITY_MW
        + min(offshore_lf * NEW_OFFSHORE_LF_MULTIPLIER, 1.0) * new_offshore_mw
    )

    onshore_mw = onshore_lf * CP2030_ONSHORE_WIND_MW
    wind_mw = onshore_mw + offshore_mw
    # Solar: embedded only, scaled to CP2030 solar capacity
    solar_lf = (
        neso["embedded_solar_mw"] / neso["embedded_solar_capacity_mw"]
        if neso["embedded_solar_capacity_mw"] > 0
        else 0
    )
    solar_mw = solar_lf * CP2030_SOLAR_MW

    # Nuclear: same load factor, applied to CP2030 nuclear capacity
    nuclear_lf = elexon.get("NUCLEAR", 0) / CURRENT_NUCLEAR_CAPACITY_MW
    nuclear_mw = nuclear_lf * CP2030_NUCLEAR_MW

    # Hydro: same as 2026 (no new capacity assumed)
    hydro_mw = elexon.get("NPSHYD", 0)

    return {
        "offshore_mw": round(offshore_mw),
        "onshore_mw": round(onshore_mw),
        "wind_mw": round(wind_mw),
        "solar_mw": round(solar_mw),
        "nuclear_mw": round(nuclear_mw),
        "hydro_mw": round(hydro_mw),
    }


def load_gas_price(reference_date=None):
    """Return gas price in p/therm for a given date, or None if unavailable.

    reference_date: a date object or YYYY-MM-DD string. Defaults to today.
    Looks up the exact date first; falls back to the most recent earlier entry.
    Accepts the legacy single-value format {"p_per_therm": X} for compatibility.
    """
    try:
        with open(GAS_PRICE_FILE) as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None

    # Legacy format written by old nightly_refresh
    if isinstance(data, dict) and set(data.keys()) == {"p_per_therm"}:
        return data["p_per_therm"]

    if not data:
        return None

    if reference_date is None:
        ref = datetime.now(timezone.utc).date().isoformat()
    elif hasattr(reference_date, "isoformat"):
        ref = reference_date.isoformat()
    else:
        ref = str(reference_date)

    if ref in data:
        return data[ref]

    # Most recent date on or before reference date
    candidates = [d for d in data if d <= ref]
    if candidates:
        return data[max(candidates)]

    # Reference date is before all data — use earliest available
    return data[min(data.keys())]


def run_model(elexon, neso, ic_records, state, interactive=False, timestamp=None):
    """Run one settlement period of the CP2030 model. Mutates and returns state."""
    demand_actual = actual_demand(elexon, neso, ic_records)
    demand_cp2030 = demand_actual + DEMAND_UPLIFT_MW

    gen = cp2030_generation(elexon, neso)

    battery_soc = state["battery_soc_mwh"]
    ldes_soc = state["ldes_soc_mwh"]

    # Compute timestamp now so we can use it for both the entry and price lookup.
    ts = (
        timestamp
        if timestamp is not None
        else _round_timestamp(datetime.now(timezone.utc), floor=interactive)
    )

    # ── Price-based dispatch ──────────────────────────────────────────────────
    price_kwargs = {}
    gas_p = load_gas_price(reference_date=ts.date())
    if gas_p is not None:
        price_kwargs["gas_p"] = gas_p

    wholesale_price, marginal_tech, ic_exports, storage_flows, dispatch, ic_foreign_prices = (
        estimate_wholesale_price(
            offshore_mw=gen["offshore_mw"],
            onshore_mw=gen["onshore_mw"],
            solar_mw=gen["solar_mw"],
            nuclear_mw=gen["nuclear_mw"],
            hydro_mw=gen["hydro_mw"],
            demand_mw=demand_cp2030,
            battery_soc_mwh=battery_soc,
            ldes_soc_mwh=ldes_soc,
            foreign_prices=load_entso_prices(ENTSO_PRICES_FILE, reference_dt=ts),
            **price_kwargs,
        )
    )

    # ── Per-technology dispatch and curtailment ───────────────────────────────
    offshore_dispatched = round(
        dispatch.get("offshore_wind_ro", 0) + dispatch.get("offshore_wind_cfd", 0)
    )
    onshore_dispatched = round(
        dispatch.get("onshore_wind_ro", 0) + dispatch.get("onshore_wind_cfd", 0)
    )
    solar_dispatched = round(
        dispatch.get("solar_legacy", 0) + dispatch.get("solar_cfd", 0)
    )
    nuclear_dispatched = round(dispatch.get("nuclear", 0))
    hydro_dispatched = round(dispatch.get("hydro", 0))
    biomass_mw = round(dispatch.get("biomass", 0))
    gas_mw = round(dispatch.get("gas_ccgt", 0) + dispatch.get("gas_ocgt", 0))
    battery_discharge_mw = round(dispatch.get("battery_discharge", 0))
    ldes_discharge_mw = round(dispatch.get("ldes_discharge", 0))

    offshore_curtailed = round(max(0, gen["offshore_mw"] - offshore_dispatched))
    onshore_curtailed = round(max(0, gen["onshore_mw"] - onshore_dispatched))
    solar_curtailed = round(max(0, gen["solar_mw"] - solar_dispatched))
    nuclear_curtailed = round(max(0, gen["nuclear_mw"] - nuclear_dispatched))
    hydro_curtailed = round(max(0, gen["hydro_mw"] - hydro_dispatched))

    ic_import_mw = sum(dispatch.get(f"ic_{name}", 0) for name, *_ in INTERCONNECTORS)
    ic_export_mw = sum(ic_exports.values())
    net_ic = round(ic_import_mw - ic_export_mw)

    # Per-country net flows (positive = import into UK, negative = export from UK)
    # and the foreign wholesale price used for each zone
    ic_by_country = {}
    ic_price_by_zone = {}
    for name, *_ in INTERCONNECTORS:
        zone = IC_AREA.get(name, name)
        imp = dispatch.get(f"ic_{name}", 0)
        exp = ic_exports.get(name, 0)
        ic_by_country[zone] = ic_by_country.get(zone, 0) + round(imp - exp)
        ic_price_by_zone[zone] = ic_foreign_prices.get(name, None)
    ic_flows_json = json.dumps(ic_by_country)
    ic_prices_json = json.dumps(ic_price_by_zone)

    # ── Update storage SoC ────────────────────────────────────────────────────
    battery_charge_mw = storage_flows["battery_charge_mw"]
    ldes_charge_mw = storage_flows["ldes_charge_mw"]

    battery_soc -= battery_discharge_mw * 0.5 / BATTERY_EFFICIENCY
    battery_soc += battery_charge_mw * 0.5 * BATTERY_EFFICIENCY
    battery_soc = max(0.0, min(CP2030_BATTERY_ENERGY_MWH, battery_soc))

    ldes_soc -= ldes_discharge_mw * 0.5 / LDES_EFFICIENCY
    ldes_soc += ldes_charge_mw * 0.5 * LDES_EFFICIENCY
    ldes_soc = max(0.0, min(CP2030_LDES_ENERGY_MWH, ldes_soc))

    state["battery_soc_mwh"] = round(battery_soc)
    state["ldes_soc_mwh"] = round(ldes_soc)

    entry = {
        "timestamp": ts.isoformat(),
        "demand_mw": round(demand_cp2030),
        "actual_demand_mw": round(demand_actual),
        "offshore_dispatched_mw": offshore_dispatched,
        "onshore_dispatched_mw": onshore_dispatched,
        "solar_dispatched_mw": solar_dispatched,
        "nuclear_dispatched_mw": nuclear_dispatched,
        "hydro_dispatched_mw": hydro_dispatched,
        "biomass_mw": biomass_mw,
        "gas_mw": gas_mw,
        "offshore_curtailed_mw": offshore_curtailed,
        "onshore_curtailed_mw": onshore_curtailed,
        "solar_curtailed_mw": solar_curtailed,
        "nuclear_curtailed_mw": nuclear_curtailed,
        "hydro_curtailed_mw": hydro_curtailed,
        "battery_charge_mw": round(battery_charge_mw),
        "battery_discharge_mw": battery_discharge_mw,
        "ldes_charge_mw": round(ldes_charge_mw),
        "ldes_discharge_mw": ldes_discharge_mw,
        "interconnector_mw": net_ic,
        "ic_flows_json": ic_flows_json,
        "ic_prices_json": ic_prices_json,
        "battery_soc_mwh": round(battery_soc),
        "ldes_soc_mwh": round(ldes_soc),
        "wholesale_price_gbp": wholesale_price,
        "marginal_tech": marginal_tech,
    }

    history = state.get("history", [])
    history.append(entry)
    state["history"] = history[-HISTORY_SIZE:]
    state["current"] = entry
    state["last_updated"] = entry["timestamp"]
    return state


# ── Database Logging ──────────────────────────────────────────────────────────


def init_db(db_path):
    """Create tables if they don't exist."""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    with sqlite3.connect(db_path) as con:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS history (
                timestamp              TEXT PRIMARY KEY,
                demand_mw              INTEGER,
                actual_demand_mw       INTEGER,
                offshore_dispatched_mw INTEGER,
                onshore_dispatched_mw  INTEGER,
                solar_dispatched_mw    INTEGER,
                nuclear_dispatched_mw  INTEGER,
                hydro_dispatched_mw    INTEGER,
                biomass_mw             INTEGER,
                gas_mw                 INTEGER,
                offshore_curtailed_mw  INTEGER,
                onshore_curtailed_mw   INTEGER,
                solar_curtailed_mw     INTEGER,
                nuclear_curtailed_mw   INTEGER,
                hydro_curtailed_mw     INTEGER,
                battery_charge_mw      INTEGER,
                battery_discharge_mw   INTEGER,
                ldes_charge_mw         INTEGER,
                ldes_discharge_mw      INTEGER,
                interconnector_mw      INTEGER,
                ic_flows_json          TEXT,
                battery_soc_mwh        INTEGER,
                ldes_soc_mwh           INTEGER,
                wholesale_price_gbp    REAL,
                marginal_tech          TEXT
            )
        """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS records (
                id                   INTEGER PRIMARY KEY CHECK (id = 1),
                max_curtailment_mw   INTEGER,
                max_curtailment_ts   TEXT,
                peak_renewables_mw   INTEGER,
                peak_renewables_ts   TEXT,
                longest_streak_hours REAL,
                longest_streak_start TEXT,
                longest_streak_end   TEXT,
                current_streak_start TEXT
            )
        """
        )


def log_entry(db_path, entry, overwrite=False):
    """Insert one settlement period row.
    overwrite=True replaces an existing row with the same timestamp (interactive use).
    overwrite=False silently skips duplicates (cron use).
    """
    verb = "INSERT OR REPLACE" if overwrite else "INSERT OR IGNORE"
    with sqlite3.connect(db_path) as con:
        con.execute(
            f"""
            {verb} INTO history VALUES (
                :timestamp, :demand_mw, :actual_demand_mw,
                :offshore_dispatched_mw, :onshore_dispatched_mw,
                :solar_dispatched_mw, :nuclear_dispatched_mw, :hydro_dispatched_mw,
                :biomass_mw, :gas_mw,
                :offshore_curtailed_mw, :onshore_curtailed_mw,
                :solar_curtailed_mw, :nuclear_curtailed_mw, :hydro_curtailed_mw,
                :battery_charge_mw, :battery_discharge_mw,
                :ldes_charge_mw, :ldes_discharge_mw,
                :interconnector_mw, :ic_flows_json,
                :battery_soc_mwh, :ldes_soc_mwh,
                :wholesale_price_gbp, :marginal_tech
            )
        """,
            entry,
        )


# ── Records & Derived Stats ───────────────────────────────────────────────────


def hours_since_gas(db_path):
    """Return hours since gas_mw was last > 0, or None if gas has never been used."""
    if not os.path.exists(db_path):
        return None
    with sqlite3.connect(db_path) as con:
        row = con.execute(
            "SELECT timestamp FROM history WHERE gas_mw > 0 ORDER BY timestamp DESC LIMIT 1"
        ).fetchone()
    if not row:
        return None
    last_gas = datetime.fromisoformat(row[0])
    return round((datetime.now(timezone.utc) - last_gas).total_seconds() / 3600, 1)


def update_records(db_path, entry):
    """Update the records table incrementally from the current entry. O(1)."""
    ts = entry["timestamp"]
    curtailment = (
        entry["offshore_curtailed_mw"]
        + entry["onshore_curtailed_mw"]
        + entry["solar_curtailed_mw"]
        + entry["nuclear_curtailed_mw"]
        + entry["hydro_curtailed_mw"]
    )
    renewables = (
        entry["offshore_dispatched_mw"]
        + entry["onshore_dispatched_mw"]
        + entry["solar_dispatched_mw"]
        + entry["hydro_dispatched_mw"]
    )

    with sqlite3.connect(db_path) as con:
        con.row_factory = sqlite3.Row
        rec = con.execute("SELECT * FROM records WHERE id = 1").fetchone()

        if rec is None:
            con.execute(
                """
                INSERT INTO records VALUES (1, ?, ?, ?, ?, 0, NULL, NULL, NULL)
            """,
                (curtailment, ts, renewables, ts),
            )
            rec = con.execute("SELECT * FROM records WHERE id = 1").fetchone()

        rec = dict(rec)

        if curtailment > (rec["max_curtailment_mw"] or 0):
            rec["max_curtailment_mw"] = curtailment
            rec["max_curtailment_ts"] = ts

        if renewables > (rec["peak_renewables_mw"] or 0):
            rec["peak_renewables_mw"] = renewables
            rec["peak_renewables_ts"] = ts

        if entry["gas_mw"] == 0:
            if rec["current_streak_start"] is None:
                rec["current_streak_start"] = ts
            streak_hours = (
                datetime.fromisoformat(ts)
                - datetime.fromisoformat(rec["current_streak_start"])
            ).total_seconds() / 3600
            if streak_hours > (rec["longest_streak_hours"] or 0):
                rec["longest_streak_hours"] = round(streak_hours, 1)
                rec["longest_streak_start"] = rec["current_streak_start"]
                rec["longest_streak_end"] = ts
        else:
            rec["current_streak_start"] = None

        con.execute(
            """
            UPDATE records SET
                max_curtailment_mw   = :max_curtailment_mw,
                max_curtailment_ts   = :max_curtailment_ts,
                peak_renewables_mw   = :peak_renewables_mw,
                peak_renewables_ts   = :peak_renewables_ts,
                longest_streak_hours = :longest_streak_hours,
                longest_streak_start = :longest_streak_start,
                longest_streak_end   = :longest_streak_end,
                current_streak_start = :current_streak_start
            WHERE id = 1
        """,
            rec,
        )

    return {
        "last_computed": ts,
        "max_curtailment": {
            "value_mw": rec["max_curtailment_mw"],
            "timestamp": rec["max_curtailment_ts"],
        },
        "peak_renewables": {
            "value_mw": rec["peak_renewables_mw"],
            "timestamp": rec["peak_renewables_ts"],
        },
        "longest_gas_free_streak": (
            {
                "value_hours": rec["longest_streak_hours"],
                "start": rec["longest_streak_start"],
                "end": rec["longest_streak_end"],
            }
            if rec["longest_streak_start"]
            else None
        ),
    }


def save_records(records, records_file):
    """Write records.json atomically."""
    os.makedirs(os.path.dirname(records_file), exist_ok=True)
    tmp = records_file + ".tmp"
    with open(tmp, "w") as f:
        json.dump(records, f)
    os.replace(tmp, records_file)


# ── Raw data store (for future re-simulation) ─────────────────────────────────


def init_raw_db(db_path):
    """Create raw history tables if they don't exist."""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    with sqlite3.connect(db_path) as con:
        con.executescript(
            """
            CREATE TABLE IF NOT EXISTS raw_generation (
                settlement_date   TEXT,
                settlement_period INTEGER,
                fuel_type         TEXT,
                generation_mw     REAL,
                PRIMARY KEY (settlement_date, settlement_period, fuel_type)
            );
            CREATE TABLE IF NOT EXISTS raw_embedded (
                settlement_date          TEXT,
                settlement_period        INTEGER,
                embedded_wind_mw         REAL,
                embedded_wind_capacity_mw REAL,
                embedded_solar_mw        REAL,
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
        """
        )


def save_raw_data(db_path, date_str, sp, elexon, neso, ic_records):
    """Persist one settlement period of raw API data for future re-simulation."""
    with sqlite3.connect(db_path) as con:
        for fuel_type, mw in elexon.items():
            con.execute(
                "INSERT OR REPLACE INTO raw_generation VALUES (?,?,?,?)",
                (date_str, sp, fuel_type, float(mw)),
            )
        con.execute(
            "INSERT OR REPLACE INTO raw_embedded VALUES (?,?,?,?,?,?)",
            (
                date_str,
                sp,
                neso["embedded_wind_mw"],
                neso["embedded_wind_capacity_mw"],
                neso["embedded_solar_mw"],
                neso["embedded_solar_capacity_mw"],
            ),
        )
        for r in ic_records:
            con.execute(
                "INSERT OR REPLACE INTO raw_interconnectors VALUES (?,?,?,?)",
                (date_str, sp, r["interconnectorName"], float(r["generation"])),
            )


# ── State I/O ─────────────────────────────────────────────────────────────────


def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    # First run: initialise storage at 50% SoC
    return {
        "battery_soc_mwh": CP2030_BATTERY_ENERGY_MWH // 2,
        "ldes_soc_mwh": CP2030_LDES_ENERGY_MWH // 2,
        "current": {},
        "history": [],
        "last_updated": None,
    }


def export_history(db_path, out_file):
    """Export full history DB to JSON for the replay page. Atomic write."""
    with sqlite3.connect(db_path) as con:
        con.row_factory = sqlite3.Row
        rows = con.execute("SELECT * FROM history ORDER BY timestamp").fetchall()
    data = [dict(r) for r in rows]
    tmp = out_file + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f)
    os.replace(tmp, out_file)
    print(f"Exported {len(data)} rows to {out_file}")


def save_state(state):
    """Write atomically so a web client never reads a partial file."""
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f)
    os.replace(tmp, STATE_FILE)


# ── Entry Point ───────────────────────────────────────────────────────────────


def main():
    date_str, sp = current_settlement_period()

    try:
        elexon = fetch_elexon()
    except Exception as e:
        print(f"ERROR fetching Elexon data: {e}")
        return

    try:
        neso = fetch_neso(date_str, sp)
    except Exception as e:
        print(f"ERROR fetching NESO data: {e}")
        return

    try:
        ic_records = fetch_interconnectors(date_str, sp)
    except Exception as e:
        print(f"ERROR fetching interconnector data: {e}")
        return

    interactive = sys.stdin.isatty()

    state = load_state()
    state = run_model(elexon, neso, ic_records, state, interactive=interactive)

    init_db(DB_FILE)
    init_raw_db(RAW_DB_FILE)
    log_entry(DB_FILE, state["current"], overwrite=interactive)
    save_raw_data(RAW_DB_FILE, date_str[:10], sp, elexon, neso, ic_records)

    state["hours_since_gas"] = hours_since_gas(DB_FILE)

    records = update_records(DB_FILE, state["current"])
    save_records(records, RECORDS_FILE)

    export_history(DB_FILE, HISTORY_FILE)
    save_state(state)
    print(f"[{state['last_updated']}] Updated {STATE_FILE}")


if __name__ == "__main__":
    main()
