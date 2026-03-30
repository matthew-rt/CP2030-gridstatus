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
import requests
from datetime import datetime, timezone
from urllib import parse
from zoneinfo import ZoneInfo

UK_TZ = ZoneInfo("Europe/London")

# ── CP2030 Target Capacities ─────────────────────────────────────────────────
CP2030_OFFSHORE_WIND_MW = 47_000
CP2030_ONSHORE_WIND_MW = 28_000
CP2030_SOLAR_MW = 46_000
CP2030_NUCLEAR_MW = 3_800
CP2030_BIOMASS_MW = 2_600
CP2030_GAS_MW = 25_000
CP2030_HYDRO_MW = 1_870
CP2030_BATTERY_POWER_MW = 25_000
CP2030_BATTERY_ENERGY_MWH = 50_000
CP2030_LDES_POWER_MW = 5_000
CP2030_LDES_ENERGY_MWH = 40_000

# ── Current (2026) Capacities ────────────────────────────────────────────────
# Used to calculate load factors. Update periodically as new capacity is built.
CURRENT_NUCLEAR_CAPACITY_MW = 4_685  # Heysham 1&2, Torness, Sizewell B
CURRENT_OFFSHORE_WIND_CAPACITY_MW = 14279
CURRENT_TOTAL_ONSHORE_WIND_CAPACITY_MW = 15270

# ── Model Parameters ─────────────────────────────────────────────────────────
DEMAND_UPLIFT_MW = 4_000  # Additional CP2030 demand vs 2026 (electrification)
BATTERY_EFFICIENCY = 0.95  # One-way charge/discharge efficiency
LDES_EFFICIENCY = 0.70  # One-way charge/discharge efficiency

# ── Runtime Config ───────────────────────────────────────────────────────────
# Override STATE_FILE with env var for local testing: STATE_FILE=/tmp/state.json python cp2030.py
STATE_FILE = os.environ.get("STATE_FILE", "/var/www/cp2030/state.json")
DB_FILE = os.environ.get("DB_FILE", "/var/www/cp2030/history.db")
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


# ── Data Fetching ─────────────────────────────────────────────────────────────


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
    resp = requests.get(ELEXON_URL, timeout=30)
    resp.raise_for_status()
    return {item["fuelType"]: item["currentUsage"] for item in resp.json()}


def _neso_query(sql):
    resp = requests.get(NESO_URL, params=parse.urlencode({"sql": sql}), timeout=30)
    resp.raise_for_status()
    return resp.json()["result"]["records"]


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
    resp = requests.get(
        ELEXON_IC_URL,
        params={"settlementDateFrom": date_only, "settlementDateTo": date_only},
        timeout=30,
    )
    resp.raise_for_status()
    all_records = resp.json()["data"]

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

    onshore_mw = onshore_lf * CP2030_ONSHORE_WIND_MW
    offshore_mw = offshore_lf * CP2030_OFFSHORE_WIND_MW
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
        "wind_mw": round(wind_mw),
        "solar_mw": round(solar_mw),
        "nuclear_mw": round(nuclear_mw),
        "hydro_mw": round(hydro_mw),
    }


def run_model(elexon, neso, ic_records, state):
    """Run one settlement period of the CP2030 model. Mutates and returns state."""
    demand_actual = actual_demand(elexon, neso, ic_records)
    demand_cp2030 = demand_actual + DEMAND_UPLIFT_MW

    gen = cp2030_generation(elexon, neso)
    clean_gen = gen["wind_mw"] + gen["solar_mw"] + gen["nuclear_mw"] + gen["hydro_mw"]

    # For each interconnector link currently flowing, use its full physical
    # capacity in CP2030. A link at exactly 0 is assumed unavailable.
    max_import_mw = sum(
        ic_capacity(r["interconnectorName"]) for r in ic_records if r["generation"] > 0
    )
    max_export_mw = -sum(
        ic_capacity(r["interconnectorName"]) for r in ic_records if r["generation"] < 0
    )

    balance = clean_gen - demand_cp2030

    battery_soc = state["battery_soc_mwh"]
    ldes_soc = state["ldes_soc_mwh"]

    battery_charge_mw = battery_discharge_mw = 0
    ldes_charge_mw = ldes_discharge_mw = 0
    biomass_mw = gas_mw = curtailment_mw = 0
    net_ic = 0

    if balance >= 0:
        surplus = float(balance)

        # Charge batteries from surplus
        charge_power = min(CP2030_BATTERY_POWER_MW, surplus)
        charge_energy_in = min(
            charge_power * 0.5,
            (CP2030_BATTERY_ENERGY_MWH - battery_soc) / BATTERY_EFFICIENCY,
        )
        battery_soc += charge_energy_in * BATTERY_EFFICIENCY
        battery_charge_mw = charge_energy_in * 2
        surplus -= battery_charge_mw

        # Charge LDES from remaining surplus
        ldes_charge_power = min(CP2030_LDES_POWER_MW, surplus)
        ldes_charge_energy_in = min(
            ldes_charge_power * 0.5,
            (CP2030_LDES_ENERGY_MWH - ldes_soc) / LDES_EFFICIENCY,
        )
        ldes_soc += ldes_charge_energy_in * LDES_EFFICIENCY
        ldes_charge_mw = ldes_charge_energy_in * 2
        surplus -= ldes_charge_mw

        # Export remaining surplus, capped at current export flows
        export_mw = max(
            max_export_mw, -surplus
        )  # both negative; less negative = less export
        net_ic = round(export_mw)
        surplus += export_mw  # export_mw is negative, reduces surplus

        curtailment_mw = surplus

    else:
        deficit = float(-balance)

        # Import up to available interconnector capacity
        net_ic = round(min(max_import_mw, deficit))
        deficit -= net_ic

        # Discharge batteries
        max_bat_power = min(
            battery_soc * 2 * BATTERY_EFFICIENCY, CP2030_BATTERY_POWER_MW
        )
        bat_discharge_power = min(max_bat_power, deficit)
        bat_energy_used = bat_discharge_power * 0.5 / BATTERY_EFFICIENCY
        battery_soc -= bat_energy_used
        battery_discharge_mw = bat_discharge_power
        deficit -= battery_discharge_mw

        # Discharge LDES
        max_ldes_power = min(ldes_soc * 2 * LDES_EFFICIENCY, CP2030_LDES_POWER_MW)
        ldes_discharge_power = min(max_ldes_power, deficit)
        ldes_energy_used = ldes_discharge_power * 0.5 / LDES_EFFICIENCY
        ldes_soc -= ldes_energy_used
        ldes_discharge_mw = ldes_discharge_power
        deficit -= ldes_discharge_mw

        # Biomass dispatched first
        biomass_mw = min(CP2030_BIOMASS_MW, deficit)
        deficit -= biomass_mw

        # Unabated gas last resort
        gas_mw = min(CP2030_GAS_MW, deficit)

    state["battery_soc_mwh"] = round(battery_soc)
    state["ldes_soc_mwh"] = round(ldes_soc)

    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "demand_mw": round(demand_cp2030),
        "actual_demand_mw": round(demand_actual),
        "wind_mw": gen["wind_mw"],
        "solar_mw": gen["solar_mw"],
        "nuclear_mw": gen["nuclear_mw"],
        "hydro_mw": gen["hydro_mw"],
        "biomass_mw": round(biomass_mw),
        "gas_mw": round(gas_mw),
        "battery_charge_mw": round(battery_charge_mw),
        "battery_discharge_mw": round(battery_discharge_mw),
        "ldes_charge_mw": round(ldes_charge_mw),
        "ldes_discharge_mw": round(ldes_discharge_mw),
        "interconnector_mw": net_ic,
        "curtailment_mw": round(curtailment_mw),
        "battery_soc_mwh": round(battery_soc),
        "ldes_soc_mwh": round(ldes_soc),
    }

    history = state.get("history", [])
    history.append(entry)
    state["history"] = history[-HISTORY_SIZE:]
    state["current"] = entry
    state["last_updated"] = entry["timestamp"]
    return state


# ── Database Logging ──────────────────────────────────────────────────────────


def init_db(db_path):
    """Create the history table if it doesn't exist."""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    with sqlite3.connect(db_path) as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS history (
                timestamp          TEXT PRIMARY KEY,
                demand_mw          INTEGER,
                actual_demand_mw   INTEGER,
                wind_mw            INTEGER,
                solar_mw           INTEGER,
                nuclear_mw         INTEGER,
                hydro_mw           INTEGER,
                biomass_mw         INTEGER,
                gas_mw             INTEGER,
                battery_charge_mw  INTEGER,
                battery_discharge_mw INTEGER,
                ldes_charge_mw     INTEGER,
                ldes_discharge_mw  INTEGER,
                interconnector_mw  INTEGER,
                curtailment_mw     INTEGER,
                battery_soc_mwh    INTEGER,
                ldes_soc_mwh       INTEGER
            )
        """)


def log_entry(db_path, entry):
    """Insert one settlement period row. Silently skips duplicate timestamps."""
    with sqlite3.connect(db_path) as con:
        con.execute("""
            INSERT OR IGNORE INTO history VALUES (
                :timestamp, :demand_mw, :actual_demand_mw,
                :wind_mw, :solar_mw, :nuclear_mw, :hydro_mw,
                :biomass_mw, :gas_mw,
                :battery_charge_mw, :battery_discharge_mw,
                :ldes_charge_mw, :ldes_discharge_mw,
                :interconnector_mw, :curtailment_mw,
                :battery_soc_mwh, :ldes_soc_mwh
            )
        """, entry)


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

    state = load_state()
    state = run_model(elexon, neso, ic_records, state)
    save_state(state)
    init_db(DB_FILE)
    log_entry(DB_FILE, state["current"])
    print(f"[{state['last_updated']}] Updated {STATE_FILE}")


if __name__ == "__main__":
    main()
