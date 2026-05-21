# Control Strategies — User Reference

Control strategies modify input tables before emission calculators consume them.
This page documents the TOML schema and CSV input formats for each Phase 6
control strategy, with worked examples and common-pitfall notes.

## Framework overview

An internal control strategy implements the `InternalControlStrategy` trait and
runs at priority 1000 (`INTERNAL_CONTROL_STRATEGY`), which is higher than
generators (100) and calculators (10). Strategies fire before calculators at
every iteration. Each strategy declares the tables it modifies via
`modified_tables()` so the engine can invalidate and reload those tables after
`pre_run` returns.

### Lifecycle

The engine calls each registered strategy in registration order:

1. **`pre_run`** — once before the first master-loop iteration. Used for global
   table transformations (all current strategies run entirely here).
2. **`execute`** — once per subscribed iteration at the strategy's declared
   granularity. No current strategy uses this; it defaults to a no-op.
3. **`post_run`** — once after all iterations complete. Defaults to a no-op.

### Registration order (canonical)

Canonical MOVES registers the four Phase 6 strategies in this order:

1. `AvftControlStrategy` — modifies `AVFT`
2. `RateOfProgressControlStrategy` — modifies `ratepollutantprocessmodelyeargroup`, `sourceTypeModelYear`
3. `OnRoadRetrofitStrategy` — modifies `emissionRateAdjustment`
4. `NonRoadRetrofitStrategy` — inline per-SCC reduction (no shared table)

The combined on-road emission scaling for a vehicle is:

```
emission_rate_final = base_rate × ROP_scale × OnRoadRetrofit_factor
```

All four strategies touch independent tables and coexist without conflict.

---

## 1. AVFT Control Strategy

**Crate:** `moves-avft`  
**Java origin:** `internalcontrolstrategies/avft/AVFTStrategy.java`  
**Modifies:** `AVFT` table

The Alternative Vehicle Fuel Technology (AVFT) control strategy replaces the
`AVFT` fleet-composition table so downstream calculators see user-specified
electric/alternative-fuel adoption fractions instead of model defaults.

The strategy runs entirely in `pre_run`. AVFT fractions are global — not
location- or time-varying — so no per-iteration subscription is needed.

### Input files

Two inputs are required; both are CSVs.

#### User AVFT CSV

Columns (order flexible, header required):

| Column | Type | Description |
|--------|------|-------------|
| `sourceTypeID` | integer | Vehicle source-use type (e.g., 11 = passenger cars) |
| `modelYearID` | integer | Model year |
| `fuelTypeID` | integer | Fuel type (1 = gasoline, 2 = diesel, 3 = CNG, …) |
| `engTechID` | integer | Engine technology (1 = conventional, 30 = EV, …) |
| `fuelEngFraction` | float | Fraction of source-type population in this fuel/eng combination |

Example:

```csv
sourceTypeID,modelYearID,fuelTypeID,engTechID,fuelEngFraction
11,2022,1,1,0.80
11,2022,2,1,0.20
11,2023,1,1,0.75
11,2023,2,1,0.25
21,2022,1,1,0.60
21,2022,2,1,0.30
21,2022,3,1,0.10
```

Validation rules (ported from `database/AVFTImporter.sql`):

- **Error**: any `(sourceTypeID, modelYearID)` group whose fraction sum
  (rounded to four decimals) exceeds 1.0.
- **Error**: any row with `fuelEngFraction < 0` (rounded to four decimals).
- **Warning** (non-fatal): any group whose fraction sum is strictly between 0 and 1
  — the tool will renormalize and continue.

#### AVFT Tool spec TOML

The spec controls gap-filling and projection. It is passed to the
`moves-avft tool` subcommand:

```bash
moves-avft tool --spec spec.toml \
                --input user_avft.csv \
                --default-avft default_avft.csv \
                --output-parquet completed_avft.parquet
```

The TOML format:

```toml
last_complete_model_year = 2022   # last year from user input
analysis_year            = 2050   # projection target

[[method]]
source_type_id = 11               # passenger cars
enabled        = true
gap_filling    = "automatic"
projection     = "proportional"

[[method]]
source_type_id = 21               # light commercial trucks
enabled        = true
gap_filling    = "defaults-preserve-inputs"
projection     = "constant"
```

**`gap_filling` values:**

| Value | Meaning |
|-------|---------|
| `"automatic"` | Fill missing model years with zeros (renormalizing first), fall back to defaults for gaps |
| `"defaults-renormalize-inputs"` | Fill from default AVFT, then rescale user rows to sum to 1 |
| `"defaults-preserve-inputs"` | Fill from default AVFT, then rescale *default* rows — user values kept as-is |

**`projection` values:**

| Value | Meaning |
|-------|---------|
| `"constant"` | Carry the last-complete-model-year row forward unchanged |
| `"national"` | Use model-supplied default AVFT for projection years |
| `"proportional"` | Scale each fuel/eng row by the user-vs-default ratio at `last_complete_model_year` |
| `"known-fractions"` | Read explicit projection rows from a known-fractions CSV (pass via `--known-fractions`) |

**Fields:**

| Key | Type | Description |
|-----|------|-------------|
| `last_complete_model_year` | integer ≥ 1950 | Latest model year supplied in user input |
| `analysis_year` | integer ≥ `last_complete_model_year` | Year to project through |
| `[[method]]` | array of tables | One entry per source type to process |
| `method.source_type_id` | integer | Source-use type this entry applies to |
| `method.enabled` | boolean (default `true`) | `false` skips this source type entirely |
| `method.gap_filling` | enum | Gap-fill algorithm (see table above) |
| `method.projection` | enum | Projection algorithm (see table above) |

### Constructing the strategy

From a pre-built completed table (e.g., the Parquet output of `moves-avft tool`):

```rust
let table = parquet_io::read_parquet("completed_avft.parquet")?;
let strategy = AvftControlStrategy::from_completed(table);
```

From raw inputs at construction time:

```rust
let strategy = AvftControlStrategy::from_tool_inputs(&spec, &user, &default, &known)?;
```

### Pitfalls

- **Fraction sums ≠ 1.0** — Rows missing from the user file leave the
  remaining rows un-renormalized. Use `gap_filling = "defaults-preserve-inputs"`
  or `"defaults-renormalize-inputs"` to fill and normalize automatically.
- **`analysis_year < last_complete_model_year`** — Rejected at spec validation.
  Set `analysis_year` to the latest year you want modeled.
- **Projection method `known-fractions` without `--known-fractions`** — The CLI
  will fail if any source type requests `known-fractions` projection but no
  known-fractions CSV is supplied.

---

## 2. Rate-of-Progress Control Strategy

**Crate:** `moves-rate-of-progress`  
**Java origin:** `internalcontrolstrategies/rateofprogress/RateOfProgressStrategy.java`  
**Modifies:** `ratepollutantprocessmodelyeargroup`, `sourceTypeModelYear`

The Rate-of-Progress (ROP) control strategy applies emission-reduction
percentages by pollutant, source type, regulatory class, and model year to model
the effect of new emissions regulations (for example, Clean Air Act Title I
Rate-of-Progress requirements). The strategy runs entirely in `pre_run`.

For each matching row the downstream emission scaling factor is:

```
scale = 1.0 - reductionFraction
```

A `reductionFraction` of `0.25` means a 25% reduction (scale = 0.75).

### Input CSV

Columns (order flexible, header required):

| Column | Type | Description |
|--------|------|-------------|
| `pollutantID` | integer | Pollutant being regulated (e.g., 3 = NOx) |
| `sourceTypeID` | integer | Vehicle source-use type (e.g., 11 = passenger cars) |
| `regClassID` | integer | Regulatory class (e.g., 10 = LDV, 20 = LDTG1) |
| `modelYearID` | integer | Model year this reduction applies to |
| `reductionFraction` | float [0, 1] | Fraction of emissions to remove |

Example:

```csv
pollutantID,sourceTypeID,regClassID,modelYearID,reductionFraction
3,11,10,2022,0.25
3,11,10,2023,0.30
3,21,10,2022,0.15
2,11,10,2022,0.40
1,11,20,2022,0.50
```

Records with `reductionFraction` outside `[0.0, 1.0]` are rejected by the
reader. Duplicate keys (same `pollutantID`, `sourceTypeID`, `regClassID`,
`modelYearID`) result in last-write-wins and are reported as non-fatal
warnings in the `ReadReport`.

### Loading and registering

```rust
use moves_rate_of_progress::csv_io;

let report = csv_io::read_csv("rop.csv")?;
let strategy = RateOfProgressControlStrategy::new(report.table);
registry.register(|| Box::new(strategy.clone()));
```

### Pitfalls

- **Key `(pollutantID, sourceTypeID, regClassID, modelYearID)` must be unique.**
  A ROP file rarely has duplicates in practice, but the reader silently takes
  the last row — check `report.duplicate_keys` to detect them.
- **`reductionFraction = 1.0` eliminates all emissions.** This is valid input
  but should be intentional; a common mistake is copying a fraction-of-fleet
  value (like `0.25` = 25% of fleet) instead of a reduction percentage.
- **`regClassID` must match the execution database.** Source-type-to-regclass
  mapping is in the MOVES default DB. A mismatch produces no error but also
  no reduction (the unmatched key simply doesn't appear in the output table).

---

## 3. OnRoadRetrofit Control Strategy

**Crate:** `moves-onroad-retrofit`  
**Java origin:** `internalcontrolstrategies/onroadretrofit/OnRoadRetrofit.java`  
**Modifies:** `emissionRateAdjustment`

The OnRoadRetrofit control strategy models retrofit emission-control programs
for on-road vehicles. A retrofit program specifies: which source types and
model-year range are targeted, what fraction of the fleet was retrofitted by a
given year, and how effective the device is at reducing emissions for a specific
pollutant/process pair.

The combined emission adjustment factor for a given
`(sourceType, modelYear, pollutant, process)` combination is:

```
factor = ∏ over active programs p of (1 - p.cumulativeRetrofitFraction × p.retrofitEffectiveness)
```

where "active" means `p.retrofitYearID ≤ analysis_year` and the model year
falls within `[p.startModelYear, p.endModelYear]`.

A factor of `1.0` means no reduction; `0.6` means 40% fewer emissions.

The strategy runs entirely in `pre_run`.

### Input CSV

The `onRoadRetrofit` table — columns (order flexible, header required):

| Column | Type | Description |
|--------|------|-------------|
| `sourceTypeID` | integer | Vehicle source-use type |
| `startModelYear` | integer | First model year in range (inclusive) |
| `endModelYear` | integer | Last model year in range (inclusive) |
| `retrofitYearID` | integer | Calendar year through which this cumulative fraction applies |
| `pollutantID` | integer | Pollutant being reduced |
| `processID` | integer | Emission process being reduced (1 = running exhaust, …) |
| `cumulativeRetrofitFraction` | float [0, 1] | Fraction of matching fleet retrofitted by `retrofitYearID` |
| `retrofitEffectiveness` | float [0, 1] | Emission reduction per retrofitted vehicle (0 = none, 1 = complete) |

Example — a diesel particulate filter (DPF) retrofit program for heavy-duty
diesel trucks (source type 52), reducing PM (pollutant 110) running exhaust
(process 1):

```csv
sourceTypeID,startModelYear,endModelYear,retrofitYearID,pollutantID,processID,cumulativeRetrofitFraction,retrofitEffectiveness
52,1990,2006,2010,110,1,0.25,0.90
52,1990,2006,2015,110,1,0.50,0.90
52,1990,2006,2020,110,1,0.75,0.90
```

By analysis year 2020, 75% of 1990–2006 model-year heavy trucks have been
retrofitted with a device that achieves 90% PM reduction. The combined factor
for those model years in 2020 is:

```
(1 - 0.25 × 0.90) × (1 - 0.50 × 0.90) × (1 - 0.75 × 0.90)
= 0.775 × 0.55 × 0.325
≈ 0.139
```

meaning approximately 86% fewer PM emissions from the retrofitted portion.

### Loading and registering

```rust
use moves_onroad_retrofit::{RetrofitRecord, RetrofitTable, OnRoadRetrofitStrategy};

let programs: RetrofitTable = csv_records.into_iter().collect();
let strategy = OnRoadRetrofitStrategy::new(programs);
registry.register(|| Box::new(strategy.clone()));
```

### Pitfalls

- **Multiple programs compound multiplicatively, not additively.** If two
  programs both target the same `(sourceType, modelYear, pollutant, process)`,
  their factors multiply — they do not add. This matches canonical MOVES.
- **`retrofitYearID` controls activation.** A program with
  `retrofitYearID = 2025` has no effect in a run with `analysis_year = 2020`.
  Ensure your `retrofitYearID` values cover the analysis years you care about.
- **Model-year range is inclusive on both ends.** `startModelYear = 2005,
  endModelYear = 2010` matches model years 2005, 2006, 2007, 2008, 2009, 2010.
- **`retrofitEffectiveness = 0.0` produces no reduction.** A common import
  error is loading the fraction-retrofitted column into the effectiveness column;
  always verify the column mapping.

---

## 4. NonRoadRetrofit Control Strategy

**Crate:** `moves-nonroad-retrofit`  
**Java origin:** NONROAD Fortran `clcrtrft.f` (Task 108)  
**Modifies:** (none — inline per-SCC calculation)

The NonRoadRetrofit control strategy wraps NONROAD's existing retrofit
calculation (ported from `clcrtrft.f`) in the unified control-strategy
framework. A single RunSpec retrofit declaration can then drive both the
on-road and nonroad calculators where applicable.

Unlike the on-road strategies, NonRoadRetrofit does not write into a shared
execution-database table. The reduction is applied inline during the per-SCC
geography loop by
`moves_nonroad::emissions::retrofit::calculate_retrofit_reduction`.

### Input format — RTR file

NONROAD retrofit records are loaded from `.RTR` files by
`moves_nonroad::input::retrofit::read_rtr` (ported from `rdrtrft.f`).
Each record specifies:

| Field | Type | Description |
|-------|------|-------------|
| `id` | integer | Record index (internal) |
| `scc` | string | 8-digit SCC code, or `"ALL"` to match every SCC |
| `tech_type` | string | Technology type, or `"ALL"` |
| `hp_min` | float | Lower HP bound (non-inclusive) |
| `hp_max` | float | Upper HP bound (inclusive) |
| `year_model_start` | integer | First model year in range |
| `year_model_end` | integer | Last model year in range |
| `year_retrofit_start` | integer | First calendar year the program is active |
| `year_retrofit_end` | integer | Last calendar year the program is active |
| `pollutant` | string | Pollutant code: `"HC"`, `"CO"`, `"NOX"`, `"PM"` |
| `annual_frac_or_count` | float | Annual fraction or count retrofitted |
| `effectiveness` | float [0, 1] | Emission reduction effectiveness |

### Pitfalls

- **Pollutant codes are strings, not IDs.** The NONROAD retrofit uses
  `"HC"`, `"CO"`, `"NOX"`, `"PM"` string codes, not the integer pollutant IDs
  used by the on-road strategies. This is an inherent difference between the
  NONROAD and onroad input models.
- **`"ALL"` wildcard is broad.** Using `scc = "ALL"` applies the program to
  every piece of NONROAD equipment in the run, which may not be intended.
  Specify concrete SCC codes when targeting a particular equipment class.
- **No interaction with OnRoadRetrofit.** The NonRoadRetrofit strategy is
  independent — it does not compound with `OnRoadRetrofitStrategy`, which
  only applies to on-road source types.

---

## Multiple strategies — combined effect

When multiple strategies are registered, the engine runs them in registration
order at the same lifecycle phase (`pre_run`). The canonical order is AVFT →
ROP → OnRoadRetrofit → NonRoadRetrofit.

For an on-road vehicle the downstream emission rate is:

```
emission_rate_final = base_rate
                    × (1 - ROP reductionFraction)
                    × ∏(1 - retrofit_fraction × retrofit_effectiveness)
```

AVFT modifies the `AVFT` fleet-composition table, which feeds into which
calculators are active — it does not directly scale an emission rate. ROP and
OnRoadRetrofit both feed into the final per-row emission rate but via different
tables (`ratepollutantprocessmodelyeargroup` vs `emissionRateAdjustment`), so
they multiply rather than compose additively.

---

## Behavioral divergences from canonical MOVES

### Data-plane write deferred

All four strategies currently complete their `pre_run` hook without writing
into the execution database. The actual table mutations are gated on Task 50
(`DataFrameStore` / `ExecutionTables` mutable write API). Until that task
lands, strategies compute and hold their results in memory — the `modified_tables`
declaration signals the engine correctly, but the downstream calculators
receive the *unmodified* default tables.

**Practical effect:** In the current build, control strategies validate their
inputs and confirm registration order but do not yet affect emissions output.
The `moves-control-strategy-validation` crate (Task 124) verifies the
mathematical formulas against the canonical Java constants independently of
the data-plane write.

### `has_rate_of_progress` flag not wired

`ExecutionRunSpec::has_rate_of_progress` returns `false` unconditionally
(the RunSpec model does not yet carry the flag — Task 12 follow-up). Some
downstream calculators gate behavior on this flag; those code paths follow
the "no ROP" branch until the flag is wired.

### NonRoadRetrofit uses string pollutant codes

Canonical MOVES NONROAD uses Fortran string codes (`HC`, `CO`, `NOX`, `PM`);
the on-road strategies use integer `pollutantID` values. The Rust port preserves
this difference. Future work (Task 128 / user guide) will document the mapping
from NONROAD string codes to MOVES integer IDs.

---

## Phase 6 closing checkpoint

### Completion criteria

| Criterion | Status |
|-----------|--------|
| `InternalControlStrategy` trait + `ControlStrategyRegistry` | ✓ Task 119 |
| `AvftControlStrategy` (gap-fill + projection + pre_run stub) | ✓ Task 120 |
| `RateOfProgressControlStrategy` (pre_run stub + CSV I/O) | ✓ Task 121 |
| `OnRoadRetrofitStrategy` (factor math + pre_run stub) | ✓ Task 122 |
| `NonRoadRetrofitStrategy` (RTR adapter + framework wiring) | ✓ Task 123 |
| Validation fixtures (59 tests — all four strategies) | ✓ Task 124 |
| Cross-strategy order-of-application tests | ✓ Task 124 |
| NONROAD retrofits unified with onroad framework | ✓ Task 123 |
| This documentation | ✓ Task 125 |

### Blocking items for Phase 7

- **Task 50 (`DataFrameStore`)** — mutable execution-table write API required
  before strategies affect output. All four `pre_run` bodies contain a `TODO`
  comment marking the exact call site.
- **Task 12 follow-up** — `has_rate_of_progress` flag in `RunSpec` model,
  needed to wire the ROP flag through to downstream calculators that gate on it.
