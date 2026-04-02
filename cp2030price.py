#!/usr/bin/env python3
"""
CP2030 Wholesale Price Model — standalone tester

Estimates GB wholesale electricity clearing price under CP2030 capacity
assumptions using a merit order simulation with smoothed bid distributions.

Each domestic technology is divided into N_BANDS equal-capacity bands with
prices drawn from evenly-spaced quantiles of N(mean, sigma) — deterministic
and smooth.

Storage discharge sits in the static merit order (capped by available SoC)
so it participates fully in price formation and can influence IC directions.
Interconnector directions and price are resolved iteratively. After convergence,
storage charging is computed analytically: the maximum MW that can charge
without pushing the clearing price above the storage max charge price.

Foreign electricity prices are fetched from the ENTSO-E Transparency Platform
(day-ahead A44 prices). Set ENTSO_E_API_KEY env var to enable; falls back to
hardcoded defaults if the key is absent or a fetch fails.

Integration note:
    cp2030.py currently returns combined wind_mw. When integrating, split
    cp2030_generation() to also return onshore_mw and offshore_mw separately,
    then call estimate_wholesale_price() from run_model().

Usage:
    python cp2030price.py                              # default foreign prices
    ENTSO_E_API_KEY=xxx python cp2030price.py          # live ENTSO-E prices
"""

import json
import os
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone

import requests
from scipy.stats import norm

# ── Gas / Carbon Assumptions ─────────────────────────────────────────────────
# These will eventually be updated from live price feeds.
GAS_PRICE_P_PER_THERM        = 70    # p/therm
CARBON_PRICE_GBP_PER_TONNE   = 50    # £/tCO2e

# ── CP2030 Dispatchable Capacities ───────────────────────────────────────────
# Non-dispatchable outputs are passed in as arguments to estimate_wholesale_price().
CP2030_BIOMASS_MW   = 2_600
CP2030_GAS_MW       = 35_000
CCGT_FRACTION       = 0.857          # 30 GW CCGT of total gas capacity
OCGT_FRACTION       = 0.143          # 5 GW OCGT

# ── Storage Parameters ────────────────────────────────────────────────────────
CP2030_BATTERY_POWER_MW   = 25_000
CP2030_BATTERY_ENERGY_MWH = 50_000
CP2030_LDES_POWER_MW      =  5_000
CP2030_LDES_ENERGY_MWH    = 40_000
BATTERY_EFFICIENCY        = 0.95     # one-way charge/discharge
LDES_EFFICIENCY           = 0.70     # one-way charge/discharge

# Storage discharge bids are derived inside estimate_wholesale_price() to guarantee
# batteries and LDES always clear *before* every gas and biomass band, regardless of the
# prevailing gas price.  The logic is:
#   1. Compute the floor of gas bands:    ccgt_srmc − GAS_CCGT_SIGMA × 3
#   2. Compute the floor of biomass bands: BIOMASS_BID[0] − BIOMASS_BID[1] × 3  (= 60 £/MWh)
#   3. dispatch_floor = min(gas_floor, biomass_floor)
#   4. bat_discharge_bid  = dispatch_floor − BATTERY_BELOW_FLOOR     (battery first)
#      ldes_discharge_bid = dispatch_floor − BATTERY_BELOW_FLOOR + 2 (LDES just above battery)
# Charge prices follow from round-trip efficiency so batteries don't buy at gas prices:
#   max_charge_price = discharge_bid × efficiency²
BATTERY_BELOW_FLOOR = 5   # £/MWh below the cheapest gas/biomass band

BATTERY_DISCHARGE_SIGMA = 2
LDES_DISCHARGE_SIGMA    = 3

# ── Interconnectors ───────────────────────────────────────────────────────────
# (name, capacity_mw, default_foreign_price_gbp, threshold_gbp)
# Default prices are used when ENTSO-E data is unavailable.
# Threshold represents transmission losses + friction; short links £3, long HVDC £5.
# Flow occurs when spread > threshold: import if GB > foreign + threshold,
# export if GB < foreign - threshold.
INTERCONNECTORS = [
    ("ElecLink",      1_000, 65, 3),   # France (EPEX)
    ("IFA",           2_000, 65, 3),   # France (EPEX)
    ("IFA2",          1_000, 65, 3),   # France (EPEX)
    ("NemoLink",      1_000, 64, 3),   # Belgium (EPEX BE)
    ("Nautilus",      1_400, 64, 3),   # Belgium (EPEX BE) — operational ~2030
    ("BritNed",       1_000, 63, 3),   # Netherlands
    ("NorthSeaLink",  1_400, 50, 5),   # Norway (Nord Pool NO2)
    ("VikingLink",    1_400, 60, 5),   # Denmark (Nord Pool DK1)
    ("NeuConnect",    1_400, 62, 5),   # Germany (EPEX DE-LU) — operational ~2028
    ("EastWest",        500, 68, 3),   # Ireland (I-SEM)
    ("Greenlink",       500, 68, 3),   # Ireland (I-SEM)
    ("Moyle",           500, 68, 3),   # N. Ireland (I-SEM)
]

MAX_ITERATIONS = 15  # cap on price/IC direction iterations

# ── ENTSO-E Price Fetching ────────────────────────────────────────────────────
ENTSO_E_URL      = "https://web-api.tp.entsoe.eu/api"
EUR_TO_GBP       = 0.855   # fallback; overridden at runtime by fetch_eur_to_gbp()
FRANKFURTER_URL  = "https://api.frankfurter.app/latest"


def fetch_eur_to_gbp():
    """
    Fetch the latest EUR/GBP rate from Frankfurter (ECB data, no API key required).
    Returns a float, or EUR_TO_GBP fallback if the request fails.
    """
    try:
        resp = requests.get(
            FRANKFURTER_URL,
            params={"from": "EUR", "to": "GBP"},
            timeout=10,
        )
        resp.raise_for_status()
        rate = resp.json()["rates"]["GBP"]
        return float(rate)
    except Exception as e:
        print(f"EUR/GBP fetch failed: {e} — using fallback {EUR_TO_GBP}")
        return EUR_TO_GBP

# ENTSO-E bidding zone area codes.
# NOTE: Ireland code (10YIE-1001A00010) should be verified once API key is active.
ENTSO_E_AREAS = {
    "FR":   "10YFR-RTE------C",   # France (EPEX)
    "BE":   "10YBE----------2",   # Belgium (EPEX BE)
    "NL":   "10YNL----------L",   # Netherlands
    "NO2":  "10YNO-2--------T",   # Norway NO2 (Nord Pool)
    "DK1":  "10YDK-1--------W",   # Denmark DK1 (Nord Pool)
    "DE":   "10Y1001A1001A82H",   # Germany/Luxembourg (EPEX DE-LU)
    "IE":   "10YIE-1001A00010",   # Ireland (I-SEM) — verify when key available
}

# Maps each interconnector to its foreign bidding zone
IC_AREA = {
    "ElecLink":     "FR",
    "IFA":          "FR",
    "IFA2":         "FR",
    "NemoLink":     "BE",
    "Nautilus":     "BE",
    "BritNed":      "NL",
    "NorthSeaLink": "NO2",
    "VikingLink":   "DK1",
    "NeuConnect":   "DE",
    "EastWest":     "IE",
    "Greenlink":    "IE",
    "Moyle":        "IE",
}


def _fetch_area_prices_eur(area_code, api_key):
    """
    Fetch all day-ahead prices in EUR/MWh for a bidding zone.

    Requests a window from yesterday 22:00 UTC to tomorrow 01:00 UTC to capture
    the full current day regardless of CET/CEST offsets. Extracts every hourly
    point and returns them as a dict of {utc_iso: price_eur}.
    """
    now_utc      = datetime.now(timezone.utc)
    period_start = (now_utc - timedelta(days=1)).strftime("%Y%m%d") + "2200"
    period_end   = (now_utc + timedelta(days=1)).strftime("%Y%m%d") + "0100"

    resp = requests.get(
        ENTSO_E_URL,
        params={
            "securityToken": api_key,
            "documentType":  "A44",
            "in_Domain":     area_code,
            "out_Domain":    area_code,
            "periodStart":   period_start,
            "periodEnd":     period_end,
        },
        timeout=15,
    )
    resp.raise_for_status()

    root = ET.fromstring(resp.text)

    # Extract namespace from root tag dynamically to avoid hardcoding version
    ns_uri = root.tag.split("}")[0].lstrip("{") if "}" in root.tag else ""
    ns     = {"ns": ns_uri} if ns_uri else {}
    pf     = "ns:" if ns_uri else ""

    prices = {}
    for ts in root.findall(f"{pf}TimeSeries", ns):
        for period in ts.findall(f"{pf}Period", ns):
            interval = period.find(f"{pf}timeInterval", ns)
            if interval is None:
                continue
            start_el = interval.find(f"{pf}start", ns)
            if start_el is None:
                continue
            resolution_el = period.find(f"{pf}resolution", ns)
            if resolution_el is None or resolution_el.text != "PT60M":
                continue   # skip non-hourly periods

            period_start_dt = datetime.fromisoformat(
                start_el.text.replace("Z", "+00:00")
            )
            for point in period.findall(f"{pf}Point", ns):
                pos_el   = point.find(f"{pf}position",    ns)
                price_el = point.find(f"{pf}price.amount", ns)
                if pos_el is not None and price_el is not None:
                    slot_dt = period_start_dt + timedelta(hours=int(pos_el.text) - 1)
                    prices[slot_dt.strftime("%Y-%m-%dT%H:%M:%SZ")] = float(price_el.text)

    return prices   # {utc_iso: price_eur}


def fetch_entso_prices(api_key=None, eur_to_gbp=EUR_TO_GBP):
    """
    Fetch all day-ahead prices from ENTSO-E for every bidding zone.
    Converts EUR/MWh → GBP/MWh using eur_to_gbp (pass the live rate from
    fetch_eur_to_gbp() when calling from nightly_refresh).

    Returns {area_key: {utc_iso: price_gbp}} for writing to the nightly cache.
    Areas that fail are omitted; load_entso_prices() falls back to defaults.
    If api_key is None or empty, returns an empty dict.
    """
    if not api_key:
        return {}

    result = {}
    for area_key, area_code in ENTSO_E_AREAS.items():
        try:
            prices_eur = _fetch_area_prices_eur(area_code, api_key)
            if prices_eur:
                result[area_key] = {
                    ts: round(p * eur_to_gbp, 2) for ts, p in prices_eur.items()
                }
            else:
                print(f"ENTSO-E: no data for {area_key} ({area_code})")
        except Exception as e:
            print(f"ENTSO-E fetch failed for {area_key}: {e}")

    return result


def load_entso_prices(prices_file, reference_dt=None):
    """
    Load cached ENTSO-E prices and return {ic_name: price_gbp} for the
    given time, picking the nearest hourly value per area.

    reference_dt: datetime to look up prices for (UTC). Defaults to now.
                  Pass the historical settlement period timestamp for reruns.

    Falls back to the hardcoded INTERCONNECTORS defaults for any area where
    the cache is missing or stale.
    """
    defaults = {name: fp for name, _, fp, _ in INTERCONNECTORS}

    try:
        with open(prices_file) as f:
            cache = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return defaults

    ref = reference_dt if reference_dt is not None else datetime.now(timezone.utc)

    def _nearest_price(area_data):
        best_price, best_diff = None, float("inf")
        for ts_str, price in area_data.items():
            ts   = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            diff = abs((ref - ts).total_seconds())
            if diff < best_diff:
                best_diff, best_price = diff, price
        return best_price

    return {
        name: _nearest_price(cache[IC_AREA[name]]) if IC_AREA.get(name) in cache else default_fp
        for name, _, default_fp, _ in INTERCONNECTORS
    }


# ── Gas Price Fetching ────────────────────────────────────────────────────────
OIL_PRICE_API_URL = "https://api.oilpriceapi.com/v1/prices/latest"

def fetch_gas_price(api_key=None):
    """
    Fetch the current UK natural gas price from oilpriceapi.com.

    Returns price in p/therm (NBP convention), matching GAS_PRICE_P_PER_THERM.
    The API commodity code NATURAL_GAS_GBP quotes NBP in p/therm.

    Falls back to GAS_PRICE_P_PER_THERM if api_key is absent or the fetch fails.
    """
    if not api_key:
        return GAS_PRICE_P_PER_THERM

    try:
        resp = requests.get(
            OIL_PRICE_API_URL,
            params={"by_code": "NATURAL_GAS_GBP"},
            headers={"Authorization": f"Token {api_key}"},
            timeout=10,
        )
        resp.raise_for_status()
        price = resp.json()["data"]["price"]
        return float(price)
    except Exception as e:
        print(f"Gas price fetch failed: {e} — using default {GAS_PRICE_P_PER_THERM}p/therm")
        return GAS_PRICE_P_PER_THERM


# ── Subsidy Scheme Fractions ─────────────────────────────────────────────────
# Share of CP2030 capacity assumed under legacy RO. Remainder = CfD or merchant.
# Approximate — will be updated with actual RO/CfD register data.
OFFSHORE_RO_FRACTION  = 0.140        # ~6.6 GW RO of 47 GW CP2030 offshore (Ofgem RO register SY22)
ONSHORE_RO_FRACTION   = 0.439        # ~12.3 GW RO of 28 GW CP2030 onshore (Ofgem RO register SY22)
SOLAR_LEGACY_FRACTION = 0.348        # ~16 GW legacy (FiT ~13 GW + RO ~3 GW) of 46 GW CP2030 solar
                                     # FiT included alongside RO: both have guaranteed income and
                                     # can bid negatively to ensure dispatch

# ── Bid Price Distributions ───────────────────────────────────────────────────
# (mean £/MWh, sigma £/MWh) for N(mean, sigma) bid distributions.
#
# RO generators receive ROC payments on top of market price:
#   Offshore: 2 ROCs × ~£50 = ~£100/MWh headroom → can bid deeply negative
#   Onshore:  1 ROC  × ~£50 = ~£50/MWh headroom
# CfD/merchant generators have near-zero marginal cost → bid near £0.

OFFSHORE_RO_BID     = ( -70, 20)   # 2 ROCs, aggressive negative bids
OFFSHORE_CfD_BID    = (   0,  5)
ONSHORE_RO_BID      = ( -30, 12)   # 1 ROC
ONSHORE_CfD_BID     = (   0,  5)
SOLAR_LEGACY_BID    = ( -10,  8)   # FiT/RO legacy, smaller headroom
SOLAR_CfD_BID       = (   0,  5)
NUCLEAR_BID         = (   0,  3)   # Must-run, near-zero marginal cost
HYDRO_BID           = (   8,  5)   # Small opportunity cost of water

BIOMASS_BID         = (  90, 10)   # Fuel costs dominate

GAS_CCGT_SIGMA      = 8            # Spread around calculated SRMC
GAS_OCGT_SIGMA      = 10

N_BANDS = 20                       # Bands per technology block


# ── SRMC Calculation ──────────────────────────────────────────────────────────

def _gas_gbp_per_mwh_thermal(p_per_therm):
    """Convert gas price from p/therm to £/MWh thermal. 1 therm = 0.029305 MWh."""
    return (p_per_therm / 100) / 0.029305


def ccgt_srmc(gas_p=GAS_PRICE_P_PER_THERM, carbon=CARBON_PRICE_GBP_PER_TONNE):
    """Short-run marginal cost of a CCGT in £/MWh. 52% efficiency, 0.37 tCO2/MWh."""
    fuel = _gas_gbp_per_mwh_thermal(gas_p) / 0.52
    return fuel + 0.37 * carbon + 3     # +£3/MWh variable O&M


def ocgt_srmc(gas_p=GAS_PRICE_P_PER_THERM, carbon=CARBON_PRICE_GBP_PER_TONNE):
    """Short-run marginal cost of an OCGT in £/MWh. 40% efficiency, 0.48 tCO2/MWh."""
    fuel = _gas_gbp_per_mwh_thermal(gas_p) / 0.40
    return fuel + 0.48 * carbon + 5     # +£5/MWh variable O&M


# ── Merit Order Builder ───────────────────────────────────────────────────────

def _normal_bands(capacity_mw, mean, sigma, label):
    """
    Divide capacity_mw into N_BANDS equal bands. Each band's price is drawn
    from an evenly-spaced quantile of N(mean, sigma). Deterministic; returns
    list of (mw, price, label) sorted ascending by price. Skips if capacity <= 0.
    """
    if capacity_mw <= 0:
        return []
    band_mw = capacity_mw / N_BANDS
    quantiles = [(i + 0.5) / N_BANDS for i in range(N_BANDS)]
    return [(band_mw, norm.ppf(q, mean, sigma), label) for q in quantiles]


def build_merit_order(
    offshore_mw, onshore_mw, solar_mw, nuclear_mw, hydro_mw,
    gas_p=GAS_PRICE_P_PER_THERM,
    carbon=CARBON_PRICE_GBP_PER_TONNE,
):
    """
    Build the domestic generation supply stack as a list of (mw, price, label)
    tuples, sorted ascending by price. Storage and interconnectors are not
    included here — storage discharge is added in estimate_wholesale_price(),
    interconnectors are resolved iteratively there.

    Non-dispatchable technologies offer only their actual output this period.
    Dispatchable technologies offer their full CP2030 capacity.
    """
    ccgt_mean = ccgt_srmc(gas_p, carbon)
    ocgt_mean = ocgt_srmc(gas_p, carbon)

    bands = []

    # Non-dispatchable: split by subsidy scheme, same load factor per scheme
    bands += _normal_bands(offshore_mw * OFFSHORE_RO_FRACTION,          *OFFSHORE_RO_BID,     "offshore_wind_ro")
    bands += _normal_bands(offshore_mw * (1 - OFFSHORE_RO_FRACTION),    *OFFSHORE_CfD_BID,    "offshore_wind_cfd")
    bands += _normal_bands(onshore_mw  * ONSHORE_RO_FRACTION,           *ONSHORE_RO_BID,      "onshore_wind_ro")
    bands += _normal_bands(onshore_mw  * (1 - ONSHORE_RO_FRACTION),     *ONSHORE_CfD_BID,     "onshore_wind_cfd")
    bands += _normal_bands(solar_mw    * SOLAR_LEGACY_FRACTION,         *SOLAR_LEGACY_BID,    "solar_legacy")
    bands += _normal_bands(solar_mw    * (1 - SOLAR_LEGACY_FRACTION),   *SOLAR_CfD_BID,       "solar_cfd")
    bands += _normal_bands(nuclear_mw,                                   *NUCLEAR_BID,         "nuclear")
    bands += _normal_bands(hydro_mw,                                     *HYDRO_BID,           "hydro")

    # Dispatchable: offer full CP2030 capacity
    bands += _normal_bands(CP2030_BIOMASS_MW,              *BIOMASS_BID,              "biomass")
    bands += _normal_bands(CP2030_GAS_MW * CCGT_FRACTION,  ccgt_mean, GAS_CCGT_SIGMA, "gas_ccgt")
    bands += _normal_bands(CP2030_GAS_MW * OCGT_FRACTION,  ocgt_mean, GAS_OCGT_SIGMA, "gas_ocgt")

    bands.sort(key=lambda x: x[1])
    return bands


# ── Price Clearing ────────────────────────────────────────────────────────────

def _find_clearing(bands, demand_mw):
    """
    Walk up a sorted supply stack until cumulative MW meets demand.
    Returns (price, marginal_label, dispatch) where dispatch is a dict of
    {label: mw_dispatched} for every band that was called upon.
    """
    cumulative = 0.0
    dispatch = {}
    for mw, price, label in bands:
        remaining = demand_mw - cumulative
        if remaining <= 0:
            break
        dispatched = min(mw, remaining)
        dispatch[label] = dispatch.get(label, 0) + dispatched
        cumulative += dispatched
        if cumulative >= demand_mw:
            return round(price, 2), label, dispatch
    total = sum(b[0] for b in bands)
    print(f"WARNING: demand {demand_mw:,.0f} MW exceeds total supply {total:,.0f} MW")
    return round(bands[-1][1], 2) if bands else 0.0, "unserved", dispatch


def _ic_mode(price, foreign_price, threshold):
    if price > foreign_price + threshold:
        return "import"
    if price < foreign_price - threshold:
        return "export"
    return "none"


# ── Price Estimation ──────────────────────────────────────────────────────────

def estimate_wholesale_price(
    offshore_mw, onshore_mw, solar_mw, nuclear_mw, hydro_mw,
    demand_mw,
    battery_soc_mwh=CP2030_BATTERY_ENERGY_MWH // 2,
    ldes_soc_mwh=CP2030_LDES_ENERGY_MWH // 2,
    foreign_prices=None,
    gas_p=GAS_PRICE_P_PER_THERM,
    carbon=CARBON_PRICE_GBP_PER_TONNE,
):
    """
    Estimate wholesale clearing price, marginal generator, interconnector flows,
    and storage dispatch for one settlement period.

    Storage discharge sits in the static merit order so it influences price and
    IC directions from the first iteration. IC directions are then resolved
    iteratively. After convergence, storage charging is computed analytically
    as the surplus supply available at or below the storage max charge price.

    Args:
        offshore_mw:      CP2030 offshore wind output this period (MW)
        onshore_mw:       CP2030 onshore wind output this period (MW)
        solar_mw:         CP2030 solar output this period (MW)
        nuclear_mw:       CP2030 nuclear output this period (MW)
        hydro_mw:         CP2030 hydro output this period (MW)
        demand_mw:        CP2030 system demand this period (MW)
        battery_soc_mwh:  Current battery state of charge (MWh); default 50%
        ldes_soc_mwh:     Current LDES state of charge (MWh); default 50%
        foreign_prices:   Dict of {ic_name: price_gbp}. If None, uses defaults
                          from INTERCONNECTORS. Populated by fetch_entso_prices().
        gas_p:            Gas price in p/therm
        carbon:           Carbon price in £/tCO2

    Returns:
        price:          Clearing price in £/MWh
        marginal:       Label of the marginal technology band
        iterations:     Number of iterations to convergence
        ic_flows:       Dict of {name: 'import'|'export'|'none'}
        storage_flows:  Dict with battery/ldes charge and discharge MW
    """
    # ── Storage bids: always below every gas and biomass band ────────────────
    # Gas band floor varies with gas price; biomass floor is fixed at ~60 £/MWh.
    # Using the lower of the two guarantees storage clears before all thermal bands.
    ccgt          = ccgt_srmc(gas_p, carbon)
    gas_floor     = ccgt - GAS_CCGT_SIGMA * 3.0
    biomass_floor = BIOMASS_BID[0] - BIOMASS_BID[1] * 3.0   # 90 − 30 = 60 £/MWh
    dispatch_floor = min(gas_floor, biomass_floor)

    bat_discharge_bid      = dispatch_floor - BATTERY_BELOW_FLOOR
    ldes_discharge_bid     = dispatch_floor - BATTERY_BELOW_FLOOR + 2  # LDES just above battery
    max_bat_charge_price   = bat_discharge_bid  * BATTERY_EFFICIENCY ** 2
    max_ldes_charge_price  = ldes_discharge_bid * LDES_EFFICIENCY    ** 2

    # ── Available storage discharge (capped by SoC) ───────────────────────────
    bat_discharge_avail_mw  = min(
        CP2030_BATTERY_POWER_MW,
        battery_soc_mwh * 2 * BATTERY_EFFICIENCY,
    )
    ldes_discharge_avail_mw = min(
        CP2030_LDES_POWER_MW,
        ldes_soc_mwh * 2 * LDES_EFFICIENCY,
    )

    # ── Static merit order: domestic generation + storage discharge ───────────
    storage_discharge_bands = (
        _normal_bands(bat_discharge_avail_mw,  bat_discharge_bid,  BATTERY_DISCHARGE_SIGMA, "battery_discharge")
        + _normal_bands(ldes_discharge_avail_mw, ldes_discharge_bid, LDES_DISCHARGE_SIGMA,  "ldes_discharge")
    )
    static_bands = sorted(
        build_merit_order(offshore_mw, onshore_mw, solar_mw, nuclear_mw, hydro_mw, gas_p, carbon)
        + storage_discharge_bands,
        key=lambda x: x[1],
    )

    # ── Iterative IC direction / price convergence ────────────────────────────
    # Use live foreign prices where available, falling back to INTERCONNECTORS defaults.
    ic_params = {
        name: (cap, (foreign_prices or {}).get(name, default_fp), thresh)
        for name, cap, default_fp, thresh in INTERCONNECTORS
    }

    # ── Available storage charge capacity (capped by SoC headroom) ───────────
    bat_charge_avail_mw  = min(
        CP2030_BATTERY_POWER_MW,
        (CP2030_BATTERY_ENERGY_MWH - battery_soc_mwh) / BATTERY_EFFICIENCY * 2,
    )
    ldes_charge_avail_mw = min(
        CP2030_LDES_POWER_MW,
        (CP2030_LDES_ENERGY_MWH - ldes_soc_mwh) / LDES_EFFICIENCY * 2,
    )

    ic_modes       = {name: "none" for name in ic_params}
    seen_states    = []
    battery_charge_mw = 0.0
    ldes_charge_mw    = 0.0

    for iteration in range(1, MAX_ITERATIONS + 1):
        export_mw = sum(
            cap for name, (cap, _, _) in ic_params.items()
            if ic_modes[name] == "export"
        )

        ic_import_bands = [
            (cap, foreign_p + thresh, f"ic_{name}")
            for name, (cap, foreign_p, thresh) in ic_params.items()
            if ic_modes[name] == "import"
        ]

        all_bands = sorted(static_bands + ic_import_bands, key=lambda x: x[1])

        # ── Analytical storage charging ───────────────────────────────────────
        # Charging adds to effective demand, so it must be included here to
        # allow IC modes to converge to the correct equilibrium.
        supply_at_bat_price  = sum(mw for mw, p, _ in all_bands if p <= max_bat_charge_price)
        bat_surplus          = max(0.0, supply_at_bat_price - demand_mw - export_mw)
        battery_charge_mw    = min(bat_charge_avail_mw, bat_surplus)

        supply_at_ldes_price = sum(mw for mw, p, _ in all_bands if p <= max_ldes_charge_price)
        ldes_surplus         = max(0.0, supply_at_ldes_price - demand_mw - export_mw - battery_charge_mw)
        ldes_charge_mw       = min(ldes_charge_avail_mw, ldes_surplus)

        effective_demand = demand_mw + export_mw + battery_charge_mw + ldes_charge_mw
        price, marginal, dispatch = _find_clearing(all_bands, effective_demand)

        new_modes = {
            name: _ic_mode(price, foreign_p, thresh)
            for name, (_, foreign_p, thresh) in ic_params.items()
        }

        if new_modes == ic_modes:
            break

        if new_modes in seen_states:
            break   # oscillation — IC on price boundary, current price is best estimate

        seen_states.append(dict(ic_modes))
        ic_modes = new_modes
    else:
        iteration = MAX_ITERATIONS

    storage_flows = {
        "battery_discharge_avail_mw": round(bat_discharge_avail_mw),
        "ldes_discharge_avail_mw":    round(ldes_discharge_avail_mw),
        "battery_charge_mw":          round(battery_charge_mw),
        "ldes_charge_mw":             round(ldes_charge_mw),
    }

    return price, marginal, iteration, ic_modes, storage_flows, dispatch


# ── Scenario Tests ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    api_key = os.environ.get("ENTSO_E_API_KEY")
    if api_key:
        print("Fetching live ENTSO-E day-ahead prices...")
        foreign_prices = fetch_entso_prices(api_key)
        live = {n: p for n, p in foreign_prices.items()
                if p != {name: fp for name, _, fp, _ in INTERCONNECTORS}[n]}
        defaults_used = {n: p for n, p in foreign_prices.items() if n not in live}
        if live:
            print(f"  Live prices (GBP): { {n: f'£{p}' for n, p in live.items()} }")
        if defaults_used:
            print(f"  Using defaults for: {list(defaults_used)}")
    else:
        print("No ENTSO_E_API_KEY set — using default foreign prices")
        foreign_prices = None

    print()
    print(f"Gas assumptions: {GAS_PRICE_P_PER_THERM}p/therm, £{CARBON_PRICE_GBP_PER_TONNE}/tCO2")
    print(f"CCGT SRMC: £{ccgt_srmc():.1f}/MWh    OCGT SRMC: £{ocgt_srmc():.1f}/MWh")
    _ccgt = ccgt_srmc()
    print(f"Battery discharge bid: £{_ccgt - BATTERY_BELOW_CCGT:.1f}/MWh  "
          f"LDES discharge bid: £{_ccgt - LDES_BELOW_CCGT:.1f}/MWh")
    total_ic_mw = sum(cap for _, cap, _, _ in INTERCONNECTORS)
    print(f"Total IC capacity: {total_ic_mw:,} MW across {len(INTERCONNECTORS)} links")
    print()

    scenarios = [
        {
            "name": "Windy summer afternoon (big surplus)",
            "offshore_mw":    38_000,
            "onshore_mw":     22_000,
            "solar_mw":       18_000,
            "nuclear_mw":      3_000,
            "hydro_mw":          600,
            "demand_mw":      28_000,
            "battery_soc_mwh": 10_000,
            "ldes_soc_mwh":    10_000,
        },
        {
            "name": "Moderate wind, typical winter day",
            "offshore_mw":    18_000,
            "onshore_mw":     10_000,
            "solar_mw":        2_000,
            "nuclear_mw":      3_000,
            "hydro_mw":          800,
            "demand_mw":      36_000,
            "battery_soc_mwh": 25_000,
            "ldes_soc_mwh":    20_000,
        },
        {
            "name": "Low wind, winter evening — storage full",
            "offshore_mw":     6_000,
            "onshore_mw":      3_000,
            "solar_mw":            0,
            "nuclear_mw":      3_000,
            "hydro_mw":          700,
            "demand_mw":      38_000,
            "battery_soc_mwh": 50_000,
            "ldes_soc_mwh":    40_000,
        },
        {
            "name": "Low wind, winter evening — storage empty",
            "offshore_mw":     6_000,
            "onshore_mw":      3_000,
            "solar_mw":            0,
            "nuclear_mw":      3_000,
            "hydro_mw":          700,
            "demand_mw":      38_000,
            "battery_soc_mwh":     0,
            "ldes_soc_mwh":        0,
        },
    ]

    for s in scenarios:
        price, marginal, iterations, ic_flows, storage, dispatch = estimate_wholesale_price(
            s["offshore_mw"], s["onshore_mw"], s["solar_mw"],
            s["nuclear_mw"],  s["hydro_mw"],   s["demand_mw"],
            battery_soc_mwh=s["battery_soc_mwh"],
            ldes_soc_mwh=s["ldes_soc_mwh"],
            foreign_prices=foreign_prices,
        )
        total_vre = s["offshore_mw"] + s["onshore_mw"] + s["solar_mw"]
        imports = [n for n, m in ic_flows.items() if m == "import"]
        exports = [n for n, m in ic_flows.items() if m == "export"]

        print(f"  {s['name']}")
        print(f"    VRE {total_vre:,} + nuclear {s['nuclear_mw']:,} + hydro {s['hydro_mw']:,} MW  |  "
              f"demand {s['demand_mw']:,} MW")
        print(f"    SoC: battery {s['battery_soc_mwh']:,} / {CP2030_BATTERY_ENERGY_MWH:,} MWh  "
              f"LDES {s['ldes_soc_mwh']:,} / {CP2030_LDES_ENERGY_MWH:,} MWh")
        print(f"    → £{price:.2f}/MWh  [marginal: {marginal}]  [{iterations} iteration(s)]")
        if imports:
            print(f"    IC imports:  {', '.join(imports)}")
        if exports:
            print(f"    IC exports:  {', '.join(exports)}")
        print(f"    storage discharge avail:  battery {storage['battery_discharge_avail_mw']:,} MW  "
              f"LDES {storage['ldes_discharge_avail_mw']:,} MW")
        print(f"    storage charging:         battery {storage['battery_charge_mw']:,} MW  "
              f"LDES {storage['ldes_charge_mw']:,} MW")
        print()
