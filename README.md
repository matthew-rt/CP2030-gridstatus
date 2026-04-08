# CP2030 Grid Tracker

A live model of what the GB electricity grid would look like today if the Clean Power 2030 (CP2030) capacity targets were already in place. Inspired by [grid.iamkate.com](https://grid.iamkate.com) and using the same Elexon BMRS data source.

The model runs every 30 minutes, pulling live generation and demand data, scaling it to 2030 capacities, running a merit-order dispatch simulation, and estimating a wholesale clearing price.

## Pages

- **cp2030gridmix.html** — Live view: current generation mix, wholesale price, storage state of charge, interconnector flows, and 24-hour history chart.
- **cp2030replay.html** — Replay: scrub through the full history with playback controls and a filterable date range. Shows the same stats as the live view.
- **dispatch-explainer.html** — Explainer: animated walkthrough of how the merit order dispatch and wholesale price model works.

## Data Sources

| Source | Used for |
|---|---|
| [Elexon BMRS](https://www.bmreports.com) | Transmission-connected generation, interconnector flows |
| [NESO](https://www.neso.energy) | Embedded (distribution-level) wind and solar estimates |
| [ENTSO-E Transparency Platform](https://transparency.entsoe.eu) | Day-ahead wholesale prices for neighbouring countries |
| Oil Price API | UK natural gas price (NBP), used in gas SRMC calculation |

Results are updated every 30 minutes via a cron job. Price inputs (ENTSO-E, gas) are refreshed nightly.

## Generation Methodology

### Wind

Offshore and onshore wind are scaled separately.

The **onshore load factor** is derived from embedded wind generation and capacity reported by NESO (assumed to be all onshore). This load factor is applied to both embedded and transmission-connected onshore wind.

The **offshore load factor** is estimated by subtracting estimated onshore transmission output from total transmission-connected wind (Elexon), then dividing by current offshore capacity. A 15% uplift is applied to reflect the higher yield of larger modern turbines.

Both load factors are applied to their respective CP2030 target capacities.

### Solar

Total embedded solar generation from NESO is scaled to the CP2030 target capacity.

### Nuclear

Nuclear output is scaled to the projected 2030 capacity. We assume Heysham 2, Torness, and Sizewell B remain online. Hinkley Point C and Sizewell C are not assumed to be operational by 2030.

### Hydro

Hydroelectric capacity is assumed unchanged from current levels.

### Biomass

Biomass capacity is held fixed at 2.6 GW. Biomass generators are assumed to hold CfD contracts and bid near zero.

### Gas

35 GW total gas capacity (30 GW CCGT, 5 GW OCGT). Gas SRMC is calculated from the live NBP gas price and a carbon price of £50/tonne, using assumed thermal efficiencies (52% CCGT, 40% OCGT).

## Demand

A flat 4 GW uplift is applied to all settlement periods to account for increased electrification (heat pumps, EVs) projected under FES scenarios. This does not capture seasonal variation in electrification demand.

## Interconnectors

The following interconnectors are modelled. Existing links use their current capacities; two planned links are included for 2030.

| Link | Country | Capacity | Threshold |
|---|---|---|---|
| IFA / IFA2 / ElecLink | France | 4,000 MW | £3/MWh |
| NemoLink / Nautilus* | Belgium | 2,400 MW | £3/MWh |
| BritNed | Netherlands | 1,000 MW | £3/MWh |
| NorthSeaLink | Norway | 1,400 MW | £5/MWh |
| VikingLink | Denmark | 1,400 MW | £5/MWh |
| NeuConnect* | Germany | 1,400 MW | £5/MWh |
| EastWest / Greenlink / Moyle | Ireland | 1,500 MW | £3/MWh |

\* Planned, assumed operational by 2030.

Day-ahead prices for each zone are fetched nightly from ENTSO-E. The threshold represents transmission losses and friction; imports occur when the GB price exceeds the foreign price plus the threshold, exports when GB price falls below the foreign price minus the threshold. Prices are converted from EUR to GBP using a live exchange rate fetched at the same time.

## Storage

Battery and LDES state of charge is tracked as a running estimate, initialised at 50% on first run.

Storage operators are modelled using SRMC bidding: they set a maximum price at which they are willing to charge (reflecting expected low-price renewable surplus periods), and bid to discharge at the break-even price implied by their round-trip efficiency:

```
discharge_bid = max_charge_price / efficiency²
```

| Type | Power | Energy | One-way efficiency | Max charge price | Discharge bid |
|---|---|---|---|---|---|
| Battery | 25 GW | 50 GWh | 95% | £10/MWh | ~£11/MWh |
| LDES | 5 GW | 40 GWh | 70% | £10/MWh | ~£20/MWh |

## Dispatch and Price Model

Merit order dispatch is simulated across all technologies. Each technology is divided into **N=20 equal-capacity bands**, with bid prices drawn from a normal distribution around a representative mean. This introduces realistic spread within technology groups rather than treating all generators identically.

Subsidy schemes shift bid prices. Generators with legacy subsidies (Renewables Obligation, early CfD rounds) can bid negative because they receive the subsidy regardless of the market price. Later CfD rounds include a negative pricing rule, so those generators bid near zero.

| Technology | Subsidy | Representative bid |
|---|---|---|
| Offshore wind (RO, ~14%) | 2 ROCs ≈ £100/MWh | N(−100, 5) |
| Offshore wind (CfD round 1–3, ~20%) | CfD, no negative pricing rule | N(0–negative, 5) |
| Offshore wind (CfD round 4+ / merchant, ~66%) | Strict negative pricing rule / none | N(0, 5) |
| Onshore wind (RO, ~44%) | 1 ROC ≈ £50/MWh | N(−50, 5) |
| Onshore wind (CfD / merchant, ~56%) | Near zero | N(0, 5) |
| Solar (legacy FiT/RO, ~35%) | | N(−10, 8) |
| Solar (CfD / merchant, ~65%) | | N(0, 5) |
| Nuclear | Must-run | N(0, 3) — 4 bands |
| Biomass | CfD | N(0, 5) — 4 bands |
| Hydro | | N(8, 5) |
| Gas (CCGT) | | N(SRMC, 8) |
| Gas (OCGT) | | N(SRMC, 10) |

All bands are sorted by price to produce the final merit order. The market clears at the price of the most expensive band needed to meet demand (pay-as-clear). Interconnector import bands are inserted at the foreign price plus threshold. Storage discharge bands are inserted at their SRMC bid. Exports are computed analytically from surplus generation below the export threshold.

## Capacity Assumptions

| Technology | Assumed 2030 Capacity |
|---|---|
| Offshore Wind | 47 GW |
| Onshore Wind | 28 GW |
| Solar | 46 GW |
| Nuclear | 3.8 GW |
| Biomass | 2.6 GW |
| Gas (total) | 35 GW (30 GW CCGT + 5 GW OCGT) |
| Hydro | 1.87 GW |
| Battery Storage | 25 GW / 50 GWh |
| LDES | 5 GW / 40 GWh |

## Infrastructure

Scripts run on a server via cron:

- **`cp2030.py`** — Main data collection and dispatch simulation, runs every 30 minutes. Writes `state.json` (current snapshot + 24h history) and appends to `history.db`.
- **`nightly_refresh.py`** — Exports `history.db` to `history.json` (for the replay page), fetches and caches ENTSO-E day-ahead prices, and fetches the current gas price. Runs at 23:00 UTC.

API keys (`ENTSO_E_API_KEY`, `OIL_PRICE_API_KEY`) are loaded from a `.env` file not committed to the repository.

## Limitations

- Economic curtailment is tracked; grid curtailment is not modelled.
- Demand uplift is flat (no seasonal variation in electrification).
- Storage state of charge is approximate and path-dependent from initialisation.
- The model assumes a single day-ahead auction clearing mechanism; intraday trading, balancing markets, and bilateral contracts are not modelled.
- Load factors for future wind and solar are extrapolated from current generation; actual 2030 values will differ.
