#!/usr/bin/env python3
"""
CP2030 Wholesale Price Model

Estimates GB wholesale electricity clearing price under CP2030 capacity
assumptions using a two-sided merit order simulation with smoothed bid
distributions.

Each domestic technology is divided into N_BANDS equal-capacity bands with
prices drawn from evenly-spaced quantiles of N(mean, sigma) — deterministic
and smooth.

Storage discharge and IC imports sit in the supply stack (ascending by bid
price). IC exports and storage charging are resolved analytically as demand
that absorbs surplus below their respective price thresholds — no iteration
needed since import bids always sit above export thresholds.

Foreign electricity prices are fetched from the ENTSO-E Transparency Platform
(day-ahead A44 prices). Set ENTSO_E_API_KEY env var to enable; falls back to
hardcoded defaults if the key is absent or a fetch fails.

Usage:
    python cp2030price.py                              # default foreign prices
    ENTSO_E_API_KEY=xxx python cp2030price.py          # live ENTSO-E prices
"""

import json
import os
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import List, Tuple

import requests
from scipy.stats import norm

# ── Gas / Carbon Assumptions ─────────────────────────────────────────────────
# These will eventually be updated from live price feeds.
GAS_PRICE_P_PER_THERM = 70  # p/therm
CARBON_PRICE_GBP_PER_TONNE = 50  # £/tCO2e

# ── CP2030 Dispatchable Capacities ───────────────────────────────────────────
# Non-dispatchable outputs are passed in as arguments to estimate_wholesale_price().
CP2030_BIOMASS_MW = 2_600
CP2030_GAS_MW = 35_000
CCGT_FRACTION = 0.857  # 30 GW CCGT of total gas capacity
OCGT_FRACTION = 0.143  # 5 GW OCGT

# ── Storage Parameters ────────────────────────────────────────────────────────
CP2030_BATTERY_POWER_MW = 25_000
CP2030_BATTERY_ENERGY_MWH = 50_000
CP2030_LDES_POWER_MW = 5_000
CP2030_LDES_ENERGY_MWH = 40_000
BATTERY_EFFICIENCY = 0.95  # one-way charge/discharge
LDES_EFFICIENCY = 0.70  # one-way charge/discharge

# Storage operators bid their SRMC: the break-even discharge price given what they
# paid to charge and their round-trip losses.
#   discharge_bid = max_charge_price / efficiency²
# Max charge prices reflect the expected clearing price during surplus/low-price periods.
BATTERY_MAX_CHARGE_PRICE = 10  # £/MWh — charge during low-price/renewable-surplus periods
LDES_MAX_CHARGE_PRICE    = 10  # £/MWh — same assumption for LDES

BATTERY_DISCHARGE_SIGMA = 2
LDES_DISCHARGE_SIGMA = 3

# ── Interconnectors ───────────────────────────────────────────────────────────
# (name, capacity_mw, default_foreign_price_gbp, threshold_gbp)
# Default prices are used when ENTSO-E data is unavailable.
# Threshold represents transmission losses + friction; short links £3, long HVDC £5.
# Flow occurs when spread > threshold: import if GB > foreign + threshold,
# export if GB < foreign - threshold.
INTERCONNECTORS = [
    ("ElecLink", 1_000, 65, 3),  # France (EPEX)
    ("IFA", 2_000, 65, 3),  # France (EPEX)
    ("IFA2", 1_000, 65, 3),  # France (EPEX)
    ("NemoLink", 1_000, 64, 3),  # Belgium (EPEX BE)
    ("Nautilus", 1_400, 64, 3),  # Belgium (EPEX BE) — operational ~2030
    ("BritNed", 1_000, 63, 3),  # Netherlands
    ("NorthSeaLink", 1_400, 50, 5),  # Norway (Nord Pool NO2)
    ("VikingLink", 1_400, 60, 5),  # Denmark (Nord Pool DK1)
    ("NeuConnect", 1_400, 62, 5),  # Germany (EPEX DE-LU) — operational ~2028
    ("EastWest", 500, 68, 3),  # Ireland (I-SEM)
    ("Greenlink", 500, 68, 3),  # Ireland (I-SEM)
    ("Moyle", 500, 68, 3),  # N. Ireland (I-SEM)
]


# ── System Configuration ─────────────────────────────────────────────────────
# Bundles the things that differ between modelled scenarios (e.g. CP2030 vs
# real 2025 fleet). Bid distributions, sigmas, and efficiencies are assumed
# invariant and stay as module-level constants. Subsidy *fractions* differ
# because the fleet mix of RO vs CfD projects depends on the year modelled.
@dataclass
class SystemConfig:
    gas_mw: int
    biomass_mw: int
    battery_power_mw: int
    battery_energy_mwh: int
    ldes_power_mw: int
    ldes_energy_mwh: int
    offshore_ro_fraction: float
    onshore_ro_fraction: float
    solar_legacy_fraction: float
    interconnectors: List[Tuple[str, int, float, float]] = field(default_factory=list)


CP2030_CONFIG = SystemConfig(
    gas_mw=35_000,
    biomass_mw=2_600,
    battery_power_mw=25_000,
    battery_energy_mwh=50_000,
    ldes_power_mw=5_000,
    ldes_energy_mwh=40_000,
    offshore_ro_fraction=0.140,
    onshore_ro_fraction=0.439,
    solar_legacy_fraction=0.348,
    interconnectors=INTERCONNECTORS,
)


# ── ENTSO-E Price Fetching ────────────────────────────────────────────────────
ENTSO_E_URL = "https://web-api.tp.entsoe.eu/api"
EUR_TO_GBP = 0.855  # fallback; overridden at runtime by fetch_eur_to_gbp()
FRANKFURTER_URL = "https://api.frankfurter.app/latest"


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
    "FR": "10YFR-RTE------C",  # France (EPEX)
    "BE": "10YBE----------2",  # Belgium (EPEX BE)
    "NL": "10YNL----------L",  # Netherlands
    "NO2": "10YNO-2--------T",  # Norway NO2 (Nord Pool)
    "DK1": "10YDK-1--------W",  # Denmark DK1 (Nord Pool)
    "DE": "10Y1001A1001A82H",  # Germany/Luxembourg (EPEX DE-LU)
    "IE": "10Y1001A1001A59C",  # Ireland (I-SEM) SEM bidding zone
}

# Maps each interconnector to its foreign bidding zone
IC_AREA = {
    "ElecLink": "FR",
    "IFA": "FR",
    "IFA2": "FR",
    "NemoLink": "BE",
    "Nautilus": "BE",
    "BritNed": "NL",
    "NorthSeaLink": "NO2",
    "VikingLink": "DK1",
    "NeuConnect": "DE",
    "EastWest": "IE",
    "Greenlink": "IE",
    "Moyle": "IE",
}


def _fetch_area_prices_eur(area_code, api_key):
    """
    Fetch all day-ahead prices in EUR/MWh for a bidding zone.

    Requests a window from yesterday 22:00 UTC to tomorrow 01:00 UTC to capture
    the full current day regardless of CET/CEST offsets. Extracts every hourly
    point and returns them as a dict of {utc_iso: price_eur}.
    """
    now_utc = datetime.now(timezone.utc)
    period_start = (now_utc - timedelta(days=1)).strftime("%Y%m%d") + "2200"
    period_end = (now_utc + timedelta(days=1)).strftime("%Y%m%d") + "0100"

    resp = requests.get(
        ENTSO_E_URL,
        params={
            "securityToken": api_key,
            "documentType": "A44",
            "in_Domain": area_code,
            "out_Domain": area_code,
            "periodStart": period_start,
            "periodEnd": period_end,
        },
        timeout=15,
    )
    resp.raise_for_status()

    root = ET.fromstring(resp.text)

    # Extract namespace from root tag dynamically to avoid hardcoding version
    ns_uri = root.tag.split("}")[0].lstrip("{") if "}" in root.tag else ""
    ns = {"ns": ns_uri} if ns_uri else {}
    pf = "ns:" if ns_uri else ""

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
            if resolution_el is None:
                continue
            # Parse ISO 8601 duration: PT15M → 15, PT30M → 30, PT60M → 60
            import re as _re

            _rm = _re.match(r"PT(\d+)M", resolution_el.text or "")
            if not _rm:
                continue
            interval_minutes = int(_rm.group(1))

            period_start_dt = datetime.fromisoformat(
                start_el.text.replace("Z", "+00:00")
            )
            for point in period.findall(f"{pf}Point", ns):
                pos_el = point.find(f"{pf}position", ns)
                price_el = point.find(f"{pf}price.amount", ns)
                if pos_el is not None and price_el is not None:
                    slot_dt = period_start_dt + timedelta(
                        minutes=interval_minutes * (int(pos_el.text) - 1)
                    )
                    prices[slot_dt.strftime("%Y-%m-%dT%H:%M:%SZ")] = float(
                        price_el.text
                    )

    if not prices:
        # Extract Reason code/text from Acknowledgement_MarketDocument if present
        reason_bits = []
        for reason in root.findall(f"{pf}Reason", ns):
            code = reason.find(f"{pf}code", ns)
            text = reason.find(f"{pf}text", ns)
            reason_bits.append(
                f"code={code.text if code is not None else '?'} "
                f"text={text.text if text is not None else '?'}"
            )
        reason_str = " | ".join(reason_bits) if reason_bits else "(no Reason element)"
        print(f"ENTSO-E: no prices for {area_code} — {reason_str}")

    return prices  # {utc_iso: price_eur}


def fetch_entso_prices(api_key=None, eur_to_gbp=EUR_TO_GBP, retries=5, backoff=10):
    """
    Fetch all day-ahead prices from ENTSO-E for every bidding zone.
    Converts EUR/MWh → GBP/MWh using eur_to_gbp (pass the live rate from
    fetch_eur_to_gbp() when calling from nightly_refresh).

    Each area is retried up to `retries` times with exponential backoff.
    Returns {area_key: {utc_iso: price_gbp}} for writing to the nightly cache.
    Areas that fail all retries are omitted; load_entso_prices() falls back to defaults.
    If api_key is None or empty, returns an empty dict.
    """
    if not api_key:
        return {}

    import time as _time

    result = {}
    for area_key, area_code in ENTSO_E_AREAS.items():
        for attempt in range(retries):
            try:
                prices_eur = _fetch_area_prices_eur(area_code, api_key)
                if prices_eur:
                    result[area_key] = {
                        ts: round(p * eur_to_gbp, 2) for ts, p in prices_eur.items()
                    }
                else:
                    print(f"ENTSO-E: no data for {area_key} ({area_code})")
                break  # success or empty — move to next area
            except Exception as e:
                wait = backoff * (2 ** attempt)
                if attempt < retries - 1:
                    print(
                        f"ENTSO-E fetch failed for {area_key} "
                        f"(attempt {attempt + 1}/{retries}): {e} — retrying in {wait}s"
                    )
                    _time.sleep(wait)
                else:
                    print(
                        f"ENTSO-E fetch failed for {area_key} "
                        f"after {retries} attempts: {e} — skipping"
                    )

    return result


def _parse_entso_cache(prices_file):
    """Load and pre-parse the ENTSO-E JSON cache into sorted (timestamp, price) lists.
    Returns {area_key: [(datetime, price), ...]} sorted by datetime, or None on failure.
    """
    try:
        with open(prices_file) as f:
            cache = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None

    parsed = {}
    for area_key, area_data in cache.items():
        entries = []
        for ts_str, price in area_data.items():
            ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            entries.append((ts, price))
        entries.sort()
        parsed[area_key] = entries
    return parsed


# Module-level cache: (file_path, file_mtime) -> parsed data
_entso_cache = (None, None, None)


def load_entso_prices(prices_file, reference_dt=None):
    """
    Load cached ENTSO-E prices and return ({ic_name: price_gbp}, {zone: age_hours}).

    The age dict is keyed by zone code (IC_AREA value, e.g. "FR", "IE") and
    contains the age in hours of the nearest available price for that zone
    relative to reference_dt. Zones with no data present float('inf').

    Uses bisect for fast lookup into sorted timestamp lists. The file is
    parsed once and reused while its mtime is unchanged.

    reference_dt: datetime to look up prices for (UTC). Defaults to now.
                  Pass the historical settlement period timestamp for reruns.
    """
    import bisect

    global _entso_cache
    defaults = {name: fp for name, _, fp, _ in INTERCONNECTORS}
    all_zones = {IC_AREA.get(name, name) for name, *_ in INTERCONNECTORS}

    # Re-parse only when the file changes
    try:
        mtime = os.path.getmtime(prices_file)
    except OSError:
        return defaults, {z: float("inf") for z in all_zones}

    cache_path, cache_mtime, parsed = _entso_cache
    if cache_path != prices_file or cache_mtime != mtime:
        parsed = _parse_entso_cache(prices_file)
        _entso_cache = (prices_file, mtime, parsed)

    if parsed is None:
        return defaults, {z: float("inf") for z in all_zones}

    ref = reference_dt if reference_dt is not None else datetime.now(timezone.utc)
    HALF_HOUR = 1800

    def _lookup_price(entries):
        """Bisect into sorted entries to find the nearest price.
        Returns (price, distance_seconds).
        """
        if not entries:
            return None, float("inf")

        timestamps = [e[0] for e in entries]  # already sorted
        idx = bisect.bisect_left(timestamps, ref)

        # Check the two candidates bracketing ref
        best_price, best_diff = None, float("inf")
        for i in (idx - 1, idx):
            if 0 <= i < len(entries):
                diff = abs((ref - entries[i][0]).total_seconds())
                if diff < best_diff:
                    best_diff = diff
                    best_price = entries[i][1]

        # Classify: within 30min = exact, within 24.5h = yesterday fallback
        if best_diff <= HALF_HOUR:
            return best_price, 0
        elif best_diff <= 24.5 * 3600:
            return best_price, 24 * 3600
        else:
            return best_price, best_diff

    prices = {}
    zone_age_s = {}
    for name, _, default_fp, _ in INTERCONNECTORS:
        zone = IC_AREA.get(name, name)
        if zone in parsed:
            price, age_s = _lookup_price(parsed[zone])
            if price is not None:
                prices[name] = price
            else:
                prices[name] = default_fp
                age_s = float("inf")
        else:
            prices[name] = default_fp
            age_s = float("inf")
        # Same zone may be referenced by multiple ICs — keep worst (highest age)
        zone_age_s[zone] = max(zone_age_s.get(zone, 0), age_s)

    zone_age_h = {z: a / 3600 for z, a in zone_age_s.items()}
    return prices, zone_age_h


# ── Gas Price Fetching ────────────────────────────────────────────────────────
OIL_PRICE_API_URL = "https://api.oilpriceapi.com/v1/prices/latest"


def fetch_gas_price(api_key=None):
    """
    Fetch the current UK natural gas price from oilpriceapi.com.

    Returns (price_p_per_therm, source) where source is one of:
      'live'    — successfully fetched from API
      'no_key'  — no API key provided, using hardcoded default
      'failed'  — API call failed, using hardcoded default
    """
    if not api_key:
        return GAS_PRICE_P_PER_THERM, "no_key"

    try:
        resp = requests.get(
            OIL_PRICE_API_URL,
            params={"by_code": "NATURAL_GAS_GBP"},
            headers={"Authorization": f"Token {api_key}"},
            timeout=10,
        )
        resp.raise_for_status()
        price = resp.json()["data"]["price"]
        return float(price), "live"
    except Exception as e:
        print(
            f"Gas price fetch failed: {e} — using default {GAS_PRICE_P_PER_THERM}p/therm"
        )
        return GAS_PRICE_P_PER_THERM, "failed"


# ── Subsidy Scheme Fractions ─────────────────────────────────────────────────
# Share of CP2030 capacity assumed under legacy RO. Remainder = CfD or merchant.
# Approximate — will be updated with actual RO/CfD register data.
OFFSHORE_RO_FRACTION = (
    0.140  # ~6.6 GW RO of 47 GW CP2030 offshore (Ofgem RO register SY22)
)
ONSHORE_RO_FRACTION = (
    0.439  # ~12.3 GW RO of 28 GW CP2030 onshore (Ofgem RO register SY22)
)
SOLAR_LEGACY_FRACTION = (
    0.348  # ~16 GW legacy (FiT ~13 GW + RO ~3 GW) of 46 GW CP2030 solar
)
# FiT included alongside RO: both have guaranteed income and
# can bid negatively to ensure dispatch

# ── Bid Price Distributions ───────────────────────────────────────────────────
# (mean £/MWh, sigma £/MWh) for N(mean, sigma) bid distributions.
#
# RO generators receive ROC payments on top of market price:
#   Offshore: 2 ROCs × ~£50 = ~£100/MWh headroom → can bid deeply negative
#   Onshore:  1 ROC  × ~£50 = ~£50/MWh headroom
# CfD/merchant generators have near-zero marginal cost → bid near £0.

OFFSHORE_RO_BID = (-100, 5)   # 2 ROCs, aggressive negative bids
OFFSHORE_CfD_BID = (0, 5)
ONSHORE_RO_BID = (-50, 5)   # 1 ROC
ONSHORE_CfD_BID = (0, 5)
SOLAR_LEGACY_BID = (-10, 8)  # FiT/RO legacy, smaller headroom
SOLAR_CfD_BID = (0, 5)
NUCLEAR_BID = (0, 3)  # Must-run, near-zero marginal cost
HYDRO_BID = (8, 5)  # Small opportunity cost of water

BIOMASS_BID = (0, 5)    # CfD — bids near zero marginal cost

GAS_CCGT_SIGMA = 8  # Spread around calculated SRMC
GAS_OCGT_SIGMA = 10

N_BANDS = 20  # Bands per technology block


# ── SRMC Calculation ──────────────────────────────────────────────────────────


def _gas_gbp_per_mwh_thermal(p_per_therm):
    """Convert gas price from p/therm to £/MWh thermal. 1 therm = 0.029305 MWh."""
    return (p_per_therm / 100) / 0.029305


def ccgt_srmc(gas_p=GAS_PRICE_P_PER_THERM, carbon=CARBON_PRICE_GBP_PER_TONNE):
    """Short-run marginal cost of a CCGT in £/MWh. 52% efficiency, 0.37 tCO2/MWh."""
    fuel = _gas_gbp_per_mwh_thermal(gas_p) / 0.52
    return fuel + 0.37 * carbon + 3  # +£3/MWh variable O&M


def ocgt_srmc(gas_p=GAS_PRICE_P_PER_THERM, carbon=CARBON_PRICE_GBP_PER_TONNE):
    """Short-run marginal cost of an OCGT in £/MWh. 40% efficiency, 0.48 tCO2/MWh."""
    fuel = _gas_gbp_per_mwh_thermal(gas_p) / 0.40
    return fuel + 0.48 * carbon + 5  # +£5/MWh variable O&M


# ── Merit Order Builder ───────────────────────────────────────────────────────


def _normal_bands(capacity_mw, mean, sigma, label, n=None, floor=None):
    """
    Divide capacity_mw into n equal bands (defaults to N_BANDS). Each band's
    price is drawn from an evenly-spaced quantile of N(mean, sigma). Deterministic;
    returns list of (mw, price, label) sorted ascending by price. Skips if capacity <= 0.
    If floor is given, no band price will be set below it.
    """
    if capacity_mw <= 0:
        return []
    n = n or N_BANDS
    band_mw = capacity_mw / n
    quantiles = [(i + 0.5) / n for i in range(n)]
    bands = [(band_mw, norm.ppf(q, mean, sigma), label) for q in quantiles]
    if floor is not None:
        bands = [(mw, max(p, floor), l) for mw, p, l in bands]
    return bands


def build_merit_order(
    offshore_mw,
    onshore_mw,
    solar_mw,
    nuclear_mw,
    hydro_mw,
    gas_p=GAS_PRICE_P_PER_THERM,
    carbon=CARBON_PRICE_GBP_PER_TONNE,
    config: SystemConfig = CP2030_CONFIG,
):
    """
    Build the domestic generation supply stack as a list of (mw, price, label)
    tuples, sorted ascending by price. Storage and interconnectors are not
    included here — storage discharge is added in estimate_wholesale_price(),
    interconnectors are resolved iteratively there.

    Non-dispatchable technologies offer only their actual output this period.
    Dispatchable technologies (gas, biomass) offer their full configured capacity.
    """
    ccgt_mean = ccgt_srmc(gas_p, carbon)
    ocgt_mean = ocgt_srmc(gas_p, carbon)

    bands = []

    # Non-dispatchable: split by subsidy scheme, same load factor per scheme
    bands += _normal_bands(
        offshore_mw * config.offshore_ro_fraction, *OFFSHORE_RO_BID, "offshore_wind_ro"
    )
    bands += _normal_bands(
        offshore_mw * (1 - config.offshore_ro_fraction), *OFFSHORE_CfD_BID, "offshore_wind_cfd"
    )
    bands += _normal_bands(
        onshore_mw * config.onshore_ro_fraction, *ONSHORE_RO_BID, "onshore_wind_ro"
    )
    bands += _normal_bands(
        onshore_mw * (1 - config.onshore_ro_fraction), *ONSHORE_CfD_BID, "onshore_wind_cfd"
    )
    bands += _normal_bands(
        solar_mw * config.solar_legacy_fraction, *SOLAR_LEGACY_BID, "solar_legacy"
    )
    bands += _normal_bands(
        solar_mw * (1 - config.solar_legacy_fraction), *SOLAR_CfD_BID, "solar_cfd"
    )
    bands += _normal_bands(nuclear_mw, *NUCLEAR_BID, "nuclear", n=4)
    bands += _normal_bands(hydro_mw, *HYDRO_BID, "hydro")

    # Dispatchable: offer full configured capacity
    bands += _normal_bands(config.biomass_mw, *BIOMASS_BID, "biomass", n=4)
    bands += _normal_bands(
        config.gas_mw * CCGT_FRACTION, ccgt_mean, GAS_CCGT_SIGMA, "gas_ccgt"
    )
    bands += _normal_bands(
        config.gas_mw * OCGT_FRACTION, ocgt_mean, GAS_OCGT_SIGMA, "gas_ocgt"
    )

    bands.sort(key=lambda x: x[1])
    return bands


# ── Price Clearing ────────────────────────────────────────────────────────────


def _find_clearing(bands, demand_mw):
    """
    Walk up a sorted supply stack until cumulative MW meets demand.
    Returns (price, marginal_label, dispatch) where dispatch is a dict of
    {label: mw_dispatched} for every band that was called upon.

    A 1e-6 MW tolerance guards against floating point residuals at the boundary
    between charging and discharge bands. Without it, when effective_demand equals
    exactly supply_at_max_for_£10 (headroom-limited charging), sub-microwatt FP
    rounding can cause a discharge band to be dispatched for ε MW and incorrectly
    become the marginal technology.
    """
    _TOL = 1e-6  # MW — negligible compared to GW-scale dispatch
    cumulative = 0.0
    dispatch = {}
    last_price, last_label = 0.0, "none"
    for mw, price, label in bands:
        remaining = demand_mw - cumulative
        if remaining <= _TOL:  # demand met within float tolerance
            break
        dispatched = min(mw, remaining)
        dispatch[label] = dispatch.get(label, 0) + dispatched
        cumulative += dispatched
        last_price, last_label = price, label
        if cumulative >= demand_mw - _TOL:
            return round(price, 2), label, dispatch, 0.0
    # Loop exited via tolerance break — demand was met, float residual only
    if demand_mw - cumulative <= _TOL and last_label != "none":
        return round(last_price, 2), last_label, dispatch, 0.0
    total = sum(b[0] for b in bands)
    unserved_mw = max(0.0, demand_mw - cumulative)
    print(f"WARNING: demand {demand_mw:,.0f} MW exceeds total supply {total:,.0f} MW")
    return round(bands[-1][1], 2) if bands else 0.0, "unserved", dispatch, unserved_mw


# ── Price Estimation ──────────────────────────────────────────────────────────


def estimate_wholesale_price(
    offshore_mw,
    onshore_mw,
    solar_mw,
    nuclear_mw,
    hydro_mw,
    demand_mw,
    battery_soc_mwh=None,
    ldes_soc_mwh=None,
    foreign_prices=None,
    gas_p=GAS_PRICE_P_PER_THERM,
    carbon=CARBON_PRICE_GBP_PER_TONNE,
    config: SystemConfig = CP2030_CONFIG,
):
    """
    Estimate wholesale clearing price, marginal generator, interconnector flows,
    and storage dispatch for one settlement period.

    IC imports enter the supply stack as bands at (foreign_p + threshold).
    IC exports are computed analytically as surplus absorbed below each IC's
    export threshold (foreign_p − threshold), highest-value market first.
    A single IC cannot simultaneously import and export: its import bid always
    sits above its export threshold, so surplus at the export threshold implies
    the import band was not needed, and vice versa.

    Storage charging is computed analytically in the same way: absorb surplus
    below the max charge price before clearing.

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
        ic_exports:     Dict of {ic_name: export_mw} for ICs that exported
        storage_flows:  Dict with battery/ldes charge and discharge MW
        dispatch:       Dict of {label: mw_dispatched} for all dispatched bands
        ic_foreign_prices: Dict of {ic_name: foreign_price_gbp} actually used
        unserved_mw:    Capacity shortfall MW (0.0 if demand was fully met)
    """
    # Default SoC to 50% of the configured battery/LDES energy if not given
    if battery_soc_mwh is None:
        battery_soc_mwh = config.battery_energy_mwh // 2
    if ldes_soc_mwh is None:
        ldes_soc_mwh = config.ldes_energy_mwh // 2

    # ── Storage bids: SRMC-based, derived from max charge price ─────────────
    # Discharge bid = max_charge_price / efficiency² (break-even given round-trip losses).
    # Charge price ceiling = the max charge price constant directly.
    max_bat_charge_price  = BATTERY_MAX_CHARGE_PRICE
    max_ldes_charge_price = LDES_MAX_CHARGE_PRICE
    bat_discharge_bid  = BATTERY_MAX_CHARGE_PRICE / BATTERY_EFFICIENCY**2
    ldes_discharge_bid = LDES_MAX_CHARGE_PRICE    / LDES_EFFICIENCY**2

    # ── Available storage MW (capped by SoC) ─────────────────────────────────
    bat_discharge_avail_mw = min(
        config.battery_power_mw, battery_soc_mwh * 2 * BATTERY_EFFICIENCY
    )
    ldes_discharge_avail_mw = min(
        config.ldes_power_mw, ldes_soc_mwh * 2 * LDES_EFFICIENCY
    )
    bat_charge_avail_mw = max(0, min(
        config.battery_power_mw,
        (config.battery_energy_mwh - battery_soc_mwh) / BATTERY_EFFICIENCY * 2,
    ))
    ldes_charge_avail_mw = max(0, min(
        config.ldes_power_mw,
        (config.ldes_energy_mwh - ldes_soc_mwh) / LDES_EFFICIENCY * 2,
    ))

    # ── IC parameters ─────────────────────────────────────────────────────────
    ic_params = {
        name: (cap, (foreign_prices or {}).get(name, default_fp), thresh)
        for name, cap, default_fp, thresh in config.interconnectors
    }

    # ── Supply stack ──────────────────────────────────────────────────────────
    # All generation, storage discharge, and IC import bands sorted ascending.
    # Storage discharge is floored just above max_charge_price so that discharge
    # and charge bids cannot both be active at the same clearing price.
    ic_import_bands = [
        (cap, foreign_p + thresh, f"ic_{name}")
        for name, (cap, foreign_p, thresh) in ic_params.items()
    ]
    storage_discharge_bands = _normal_bands(
        bat_discharge_avail_mw, bat_discharge_bid, BATTERY_DISCHARGE_SIGMA,
        "battery_discharge", floor=max_bat_charge_price + 0.01,
    ) + _normal_bands(
        ldes_discharge_avail_mw, ldes_discharge_bid, LDES_DISCHARGE_SIGMA,
        "ldes_discharge", floor=max_ldes_charge_price + 0.01,
    )
    supply_bands = sorted(
        build_merit_order(offshore_mw, onshore_mw, solar_mw, nuclear_mw, hydro_mw, gas_p, carbon, config=config)
        + storage_discharge_bands
        + ic_import_bands,
        key=lambda x: x[1],
    )

    # ── Demand bids (flexible demand beyond base load) ────────────────────────
    # IC exports: willing to buy GB electricity up to (foreign_p - threshold).
    # Storage: willing to charge up to max_charge_price.
    # Sorted descending by willingness to pay so highest-value demand enters first.
    demand_bids = []
    for name, (cap, foreign_p, thresh) in ic_params.items():
        demand_bids.append((cap, foreign_p - thresh, f"ic_export_{name}"))
    demand_bids.append((bat_charge_avail_mw,  max_bat_charge_price,  "battery_charge"))
    demand_bids.append((ldes_charge_avail_mw, max_ldes_charge_price, "ldes_charge"))
    demand_bids.sort(key=lambda x: -x[1])

    # ── Two-sided clearing ────────────────────────────────────────────────────
    # Walk demand bids in descending order of willingness to pay. For each bid,
    # check how much headroom remains in the supply stack at or below the bid's
    # max price. Accept the full quantity if it fits; accept the residual headroom
    # and stop if not. This naturally enforces mutual exclusion between storage
    # charge (max £10) and discharge (floor £10.01): they cannot both clear at
    # the same price.
    effective_demand = demand_mw
    accepted = {}
    for bid_mw, bid_max_price, bid_label in demand_bids:
        if bid_mw <= 0:
            continue
        supply_at_max = sum(mw for mw, p, _ in supply_bands if p <= bid_max_price)
        if supply_at_max <= effective_demand:
            break  # clearing price already above this bid's max; stop
        headroom = supply_at_max - effective_demand
        accepted_mw = min(bid_mw, headroom)
        accepted[bid_label] = accepted_mw
        effective_demand += accepted_mw
        if accepted_mw < bid_mw:
            break  # headroom exhausted; no room for lower-priced bids

    # ── Final clearing pass ───────────────────────────────────────────────────
    price, marginal, dispatch, unserved_mw = _find_clearing(supply_bands, effective_demand)

    # ── Extract accepted flexible demand ──────────────────────────────────────
    battery_charge_mw = accepted.get("battery_charge", 0.0)
    ldes_charge_mw    = accepted.get("ldes_charge",    0.0)
    ic_exports = {
        name: round(accepted[f"ic_export_{name}"])
        for name in ic_params
        if f"ic_export_{name}" in accepted and accepted[f"ic_export_{name}"] > 0
    }

    storage_flows = {
        "battery_discharge_avail_mw": round(bat_discharge_avail_mw),
        "ldes_discharge_avail_mw": round(ldes_discharge_avail_mw),
        "battery_charge_mw": round(battery_charge_mw),
        "ldes_charge_mw": round(ldes_charge_mw),
    }

    # Per-link foreign prices actually used in dispatch (GBP, after EUR conversion)
    ic_foreign_prices = {name: round(fp, 2) for name, (_, fp, _) in ic_params.items()}

    return price, marginal, ic_exports, storage_flows, dispatch, ic_foreign_prices, round(unserved_mw, 2)


# ── Scenario Tests ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    api_key = os.environ.get("ENTSO_E_API_KEY")
    if api_key:
        print("Fetching live ENTSO-E day-ahead prices...")
        foreign_prices = fetch_entso_prices(api_key)
        live = {
            n: p
            for n, p in foreign_prices.items()
            if p != {name: fp for name, _, fp, _ in INTERCONNECTORS}[n]
        }
        defaults_used = {n: p for n, p in foreign_prices.items() if n not in live}
        if live:
            print(f"  Live prices (GBP): { {n: f'£{p}' for n, p in live.items()} }")
        if defaults_used:
            print(f"  Using defaults for: {list(defaults_used)}")
    else:
        print("No ENTSO_E_API_KEY set — using default foreign prices")
        foreign_prices = None

    print()
    _ccgt = ccgt_srmc()
    _ocgt = ocgt_srmc()
    _bat_bid  = BATTERY_MAX_CHARGE_PRICE / BATTERY_EFFICIENCY**2
    _ldes_bid = LDES_MAX_CHARGE_PRICE    / LDES_EFFICIENCY**2
    print(
        f"Gas assumptions: {GAS_PRICE_P_PER_THERM}p/therm, £{CARBON_PRICE_GBP_PER_TONNE}/tCO2"
    )
    print(f"CCGT SRMC: £{_ccgt:.1f}/MWh    OCGT SRMC: £{_ocgt:.1f}/MWh")
    print(
        f"Battery charge ceiling: £{BATTERY_MAX_CHARGE_PRICE}/MWh  "
        f"discharge bid: £{_bat_bid:.1f}/MWh"
    )
    print(
        f"LDES    charge ceiling: £{LDES_MAX_CHARGE_PRICE}/MWh  "
        f"discharge bid: £{_ldes_bid:.1f}/MWh"
    )
    total_ic_mw = sum(cap for _, cap, _, _ in INTERCONNECTORS)
    print(f"Total IC capacity: {total_ic_mw:,} MW across {len(INTERCONNECTORS)} links")
    print()

    scenarios = [
        # ── Normal operating conditions ───────────────────────────────────────
        {
            "name": "Windy summer afternoon — large renewable surplus",
            "offshore_mw": 38_000,
            "onshore_mw": 22_000,
            "solar_mw": 18_000,
            "nuclear_mw": 3_000,
            "hydro_mw": 600,
            "demand_mw": 28_000,
            "battery_soc_mwh": 10_000,
            "ldes_soc_mwh": 10_000,
            "expect": "Negative/near-zero price; batteries and LDES charging; heavy exports",
        },
        {
            "name": "Moderate wind, typical winter day",
            "offshore_mw": 18_000,
            "onshore_mw": 10_000,
            "solar_mw": 2_000,
            "nuclear_mw": 3_000,
            "hydro_mw": 800,
            "demand_mw": 36_000,
            "battery_soc_mwh": 25_000,
            "ldes_soc_mwh": 20_000,
            "expect": "Battery/LDES covers shortfall; no or minimal gas",
        },
        {
            "name": "Low wind, winter evening — storage full",
            "offshore_mw": 6_000,
            "onshore_mw": 3_000,
            "solar_mw": 0,
            "nuclear_mw": 3_000,
            "hydro_mw": 700,
            "demand_mw": 38_000,
            "battery_soc_mwh": 50_000,
            "ldes_soc_mwh": 40_000,
            "expect": "Storage discharges before gas; price set by battery bid",
        },
        {
            "name": "Low wind, winter evening — storage empty",
            "offshore_mw": 6_000,
            "onshore_mw": 3_000,
            "solar_mw": 0,
            "nuclear_mw": 3_000,
            "hydro_mw": 700,
            "demand_mw": 38_000,
            "battery_soc_mwh": 0,
            "ldes_soc_mwh": 0,
            "expect": "Gas/biomass marginal; high price; no storage discharge",
        },
        # ── Edge cases ────────────────────────────────────────────────────────
        {
            "name": "EDGE: nuclear offline, low wind — stress test",
            "offshore_mw": 5_000,
            "onshore_mw": 2_000,
            "solar_mw": 0,
            "nuclear_mw": 0,
            "hydro_mw": 500,
            "demand_mw": 38_000,
            "battery_soc_mwh": 5_000,
            "ldes_soc_mwh": 5_000,
            "expect": "Heavy gas dispatch; OCGT likely marginal; very high price",
        },
        {
            "name": "EDGE: massive surplus, all storage full",
            "offshore_mw": 47_000,
            "onshore_mw": 28_000,
            "solar_mw": 30_000,
            "nuclear_mw": 3_800,
            "hydro_mw": 1_870,
            "demand_mw": 25_000,
            "battery_soc_mwh": 50_000,  # full — can't charge
            "ldes_soc_mwh": 40_000,  # full — can't charge
            "expect": "Deep negative price; curtailment; no storage charging (full); heavy exports",
        },
        {
            "name": "EDGE: demand exactly met by renewables + nuclear",
            "offshore_mw": 15_000,
            "onshore_mw": 8_000,
            "solar_mw": 5_000,
            "nuclear_mw": 3_800,
            "hydro_mw": 1_000,
            "demand_mw": 32_800,  # = sum of above
            "battery_soc_mwh": 25_000,
            "ldes_soc_mwh": 20_000,
            "expect": "Price near zero (hydro or nuclear marginal); no gas; no curtailment",
        },
        {
            "name": "EDGE: zero renewables (dark doldrums)",
            "offshore_mw": 0,
            "onshore_mw": 0,
            "solar_mw": 0,
            "nuclear_mw": 3_800,
            "hydro_mw": 500,
            "demand_mw": 35_000,
            "battery_soc_mwh": 25_000,
            "ldes_soc_mwh": 20_000,
            "expect": "Gas dominant; OCGT likely marginal; storage discharges first",
        },
    ]

    for s in scenarios:
        price, marginal, ic_exports, storage, dispatch, _ic_fp, _unserved = estimate_wholesale_price(
            s["offshore_mw"],
            s["onshore_mw"],
            s["solar_mw"],
            s["nuclear_mw"],
            s["hydro_mw"],
            s["demand_mw"],
            battery_soc_mwh=s["battery_soc_mwh"],
            ldes_soc_mwh=s["ldes_soc_mwh"],
            foreign_prices=foreign_prices,
        )
        total_vre = s["offshore_mw"] + s["onshore_mw"] + s["solar_mw"]
        imports = {
            n: round(dispatch.get(f"ic_{n}", 0))
            for n in [name for name, *_ in INTERCONNECTORS]
            if dispatch.get(f"ic_{n}", 0) > 0
        }
        gas_mw = dispatch.get("gas_ccgt", 0) + dispatch.get("gas_ocgt", 0)
        bat_dis = dispatch.get("battery_discharge", 0)
        ldes_dis = dispatch.get("ldes_discharge", 0)

        print(f"  {s['name']}")
        print(f"    Expect: {s['expect']}")
        print(
            f"    VRE {total_vre:,} + nuclear {s['nuclear_mw']:,} + hydro {s['hydro_mw']:,} MW  |  "
            f"demand {s['demand_mw']:,} MW"
        )
        print(
            f"    SoC: battery {s['battery_soc_mwh']:,}/{CP2030_BATTERY_ENERGY_MWH:,} MWh  "
            f"LDES {s['ldes_soc_mwh']:,}/{CP2030_LDES_ENERGY_MWH:,} MWh"
        )
        print(f"    → £{price:.1f}/MWh  [marginal: {marginal}]")
        print(
            f"    gas: {gas_mw:,.0f} MW  |  "
            f"bat discharge: {bat_dis:,.0f} MW  charge: {storage['battery_charge_mw']:,} MW  |  "
            f"LDES discharge: {ldes_dis:,.0f} MW  charge: {storage['ldes_charge_mw']:,} MW"
        )
        if imports:
            print(
                f"    IC imports (MW): { {n: f'{mw:,.0f}' for n, mw in imports.items()} }"
            )
        if ic_exports:
            print(
                f"    IC exports (MW): { {n: f'{mw:,}' for n, mw in ic_exports.items()} }"
            )
        print()
