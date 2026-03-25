# CP2030 Grid Tracker

This project predicts what the electricity mix would look like today, if we had the capacity mix outlined in the CP2030 action plan. This is to try and give a sense of what the grid of the future will look lke. This work is directly inspired by grid.iamkate.com and borrows from the methodology presented there. 

We track the total amount of economic curtailment in a given hour, but do not track grid curtailment, as this would require more sophisticated modelling. 

## Overarching assumptions 

This does not explicitly model the economics of each potential generator, and assumes the economic incentives of the system are aligned with the goal of a low carbon grid. We also don't model any flexibility, which will be important in the future. 

## Data Sources

We pull data from Elexon's BMRS for transmission connected assets, and NESO for estimates of embedded generation. Results are updated every 30 minutes. 

## Generation 

### Wind

Offshore and onshore wind are scaled separately, as offshore capacity is expected to grow faster than onshore to 2030.

The onshore load factor is derived from the embedded (distribution-level) wind generation and capacity reported by NESO, assuming all embedded wind is onshore. This load factor is applied to both embedded and transmission-connected onshore wind, on the assumption that they share a similar spatial distribution.

The offshore load factor is estimated by subtracting the estimated transmission onshore output (onshore load factor × current transmission onshore capacity) from the total transmission-connected wind reported by Elexon, then dividing by current offshore capacity.

Both load factors are then applied to their respective CP2030 target capacities. This approach will likely underestimate actual future load factors, as newer turbines tend to have higher yields.

### Solar

We take the total embedded generation of solar and scale it up to the predicted future capacity. 

### Hydroelectric 

We assume that the hydroelectric capacity in 2030 is the same as the 2026 capacity.

### Nuclear

Nuclear load factor is extrapolated to the projected capacity. We assume that Heysham 2, Torness and Sizewell B remain online. Hinkley Point C and Sizewell C are not assumed to be online. 

### Dispatchable sources of power

We assume that Biomass and unabated gas are dispatched to meet demand, with Biomass dispatched first.

## Demand

We make a simple assumption that overall demand is 4 GW higher in each hour, based on FES. This will not properly capture heating demand in the winter (and varying patterns of demand).

## Storage

We keep a running estimate of the total energy stored in batteries and longer duration storage. Batteries are charged off surplus renewables and nuclear. We do not charge batteries off interconnectors or dispatchable power (this is potentially unrealistic). Storage is discharged to meet demand, before biomass and unabated gas. Batteries are assumed to be 2 hour batteries, and LDES is assumed to be 12 hours. We assume a charge and discharge efficiency of 95% each way for batteries, and 70% each way for LDES (for a rough estimate of a mix of pumped hydro and other sources).

## Interconnectors

This section makes several assumptions. If there is a surplus of electricity, we export to any country which we are exporting to in this hour, but not to any country we are exporting from. If there is a deficit of electricity, we import from any country which we are importing from in this hour and don't export at all. Interconnectors are capped at their physical size. We assume no new interconnectors are added by 2030 for simplicity (although some will be).

## How it works

Every 30 minutes we pull the latest data, and update a json with the results. This json also contains state information about the batteries, and the generation mix for the last 24 hours.

## Capacity Assumptions

| Technology | Assumed 2030 Capacity (GW) |
|---|---|
| Offshore Wind | 47 |
| Onshore Wind | 28 |
| Solar | 46 |
| Nuclear | 3.8 |
| Biomass | 2.6 |
| Unabated Gas | 25 |
| Hydro | 1.87 |
| Battery Storage (power) | 25 |
| Battery Storage (energy, GWh) | 50 |
| LDES (power) | 5 |
| LDES (energy, GWh) | 40 |