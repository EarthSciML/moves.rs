# Implementing MOVES/NONROAD in ESI — Detailed Plan

**Status:** draft · 2026-06-03
**Goal:** re-express the MOVES (onroad) + NONROAD computation that `moves.rs` currently
implements as a set of [ESI](https://github.com/EarthSciML/EarthSciInventory) documents
(`.esi`), executed by an ESI engine, reproducing `moves.rs`'s own outputs within the
existing per-table tolerance budget.

**Scope target:** *at least as much as `moves.rs` implements today* — i.e. the full
onroad calculator/generator chain (functionally complete, under canonical regression) and
the NONROAD pipeline (structurally complete; numerically gated on unported fixed-width
loaders). Items `moves.rs` itself defers (§7) are out of scope here too.

This plan is grounded in a full read of the repo: the calculator DAG
(`characterization/calculator-chains/calculator-dag.json`), the wiring audit
(`characterization/calculator-wiring-audit.md`), the orchestration spine
(`crates/moves-framework/src/execution/engine.rs`), the NONROAD crate
(`crates/moves-nonroad/`), and the fidelity audit (`AUDIT-canonical-fidelity.md`).

---

## 0. Why ESI fits, and the one conceptual shift

`moves.rs` is ~98% a **select–multiply–aggregate contraction over a categorical catalog**:
tabulated rates and fractions, combined by joins / disaggregations / aggregations, with a
thin film of closed-form arithmetic on top. That is exactly ESI's domain (`index_sets`,
`tables`, `pipelines` over a closed relational-algebra + scalar-expression AST).

**The one shift:** `moves.rs` executes via the **MasterLoop** — a nested iteration
`iteration → process → state → county → zone → link → year → month → day → hour`
(`crates/moves-framework/src/masterloop/master_loop.rs`) that streams one cell at a time.
That loop is an *execution strategy, not semantics*. In ESI those nested dimensions become
**key columns**, and the whole loop collapses into one relational contraction over the
keyed product — you don't iterate, you `join` + `aggregate`. The genuine exceptions are the
handful of **data-dependent recurrences** (population growth, age forward-shift, multi-day
soak), which is precisely what the new `scan` operator (ESI
[PR #1](https://github.com/EarthSciML/EarthSciInventory/pull/1)) exists for.

ESI's operator set is **closed — no host escape hatch**. Every `moves.rs` formula is
expressible: the math is all `+ − × ÷ ^ exp log min max floor` etc. (even NONROAD's dense
`wadeeq` is nested arithmetic). Where the vocabulary genuinely needs help, it gets a
first-class operator (`scan`, `window`, `mod`, `round` were added for exactly this), never
an opaque callout.

---

## 1. The mapping (moves.rs → ESI)

| moves.rs concept | ESI construct |
|---|---|
| Default-DB / snapshot Parquet table | `tables` with `data: { source }` + a registered **data-source loader** (pushdown — §3). |
| Categorical dimension (sourceType, fuelType, opMode, county, polProcess…) | `index_sets` (with `ordered`, `range`, `rollup`). |
| Packed surrogate key — `polProcessID = pollutant*100 + process`; `sourceTypeModelYearID = sourceType*10000 + modelYear`; `sourceBinID` (decimal-slot pack) | `index_sets.composite` with a `pack` expression; **decode in-pipeline** via `mod`/`floor` (e.g. `pollutant = floor(polProcessID/100)`, `process = mod(polProcessID,100)`), or loader-side. |
| Select-multiply-aggregate kernel (class **a**) | `join` → `derive (×)` → `aggregate (sum / weighted_mean)`. |
| Closed-form scalar math (class **b**): temp/humidity/AC adjust, deterioration, SO2, HC speciation, `wadeeq` | `derive` with the Expression AST (`ifelse` for the CASE branches). |
| Data-dependent recurrence (class **c**): population growth, age forward-shift, multi-day TVV, cumulative vapor-vented | `scan` (ordered fold; `prev_<state>`). |
| Ordered/positional access (drive-cycle Δspeed, cumulative distance) | `window` (`lag`/`cumsum`/`row_number`/…). |
| Rate → inventory multiply (`BaseRateCalculator` `universalActivity`) | `join` (rate × activity) → `derive`. |
| Chained-calculator pool (`MOVESWorkerOutput` append + relabel) | `union` of pipeline outputs; downstream pipelines read the union. |
| Final `MOVESOutput` aggregation | `aggregate` (`sum`) with `by`/`over`; ANY_VALUE(sourceType), weeksPerMonth rescale, synthetic/replaced-pollutant drops as `filter`s. |
| Generator scratch table (e.g. `BaseRate`, `SourceBinDistribution`) | a named pipeline output consumed by later pipelines (cross-document via `provides`). |

**Decode example** (`polProcessID` → components), pure AST, no escape hatch:

```json
{ "id": "decode_pp", "op": "derive", "input": "rates", "column": "pollutant",
  "expr": { "op": "floor", "args": [ { "op": "/", "args": ["polProcessID", 100] } ] } }
```

---

## 2. Phase plan

Each phase is independently validatable against the characterization snapshots
(§4). Phases 1–3 are the onroad backbone; 4 is the hard evaporative state; 5 is NONROAD;
6 is aggregation + fidelity. The POC on branch `poc/esi-calculator-port` (`esi-poc/`)
already proved phases 1–4's *shapes* on real data (see §8).

### Phase 0 — Foundations & harness (prerequisite)

1. **Full-tier ESI engine with pushdown.** The ESI reference engines are eager/in-memory;
   MOVES data is not (`EmissionRateByAge` ~590k rows, `ZoneMonthHour` multi-GB). Build (or
   host) a lazy engine that pushes `select`/`filter` projection+predicate into the scan.
   `moves.rs` *already has this substrate*: `moves-data-default` returns Polars `LazyFrame`s
   with partition pruning (`crates/moves-data-default/src/lib.rs`). **Recommended: host the
   ESI Full-tier engine in `moves.rs`** as a new crate (`moves-esi`?) wrapping the existing
   Polars reader — this is the single biggest infra item and the one true prerequisite.
2. **Catalog bridge.** Generate ESI `tables` schemas + `index_sets` from the 187-entry
   `MergeTableSpec` registry (`crates/moves-framework/src/input/input_data_manager.rs`) and
   the schema registry (`crates/moves-framework/src/data/schema_registry.rs`). Emit the
   packed-key `composite`/`pack` definitions from `crates/moves-data/src/pollutant_process.rs`
   and `source_bin_distribution_generator.rs:1196`.
3. **Data-source loader** over snapshot Parquet (`characterization/snapshots/*/tables/…`)
   and the default-DB Parquet tree — reuse `esi.sources.parquet_dir`-style loaders; for
   the Full tier return a `LazyFrame` (mirrors `esi-libraries-spec.md` §4.7).
4. **Validation harness.** Reuse `characterization/` snapshots + `tolerance.toml`. Diff each
   ESI pipeline output against the captured `MOVESOutput` (per-pollutant sums within
   tolerance) — the same comparison `full_suite_regression.rs::canonical_snapshot_diff` does.

### Phase 1 — Onroad rate core (classes a + b; the heart)

Pipelines, in dependency order:

- **SourceBinDistributionGenerator** → `SourceBin`, `SourceBinDistribution` (post-AVFT
  source-bin weights). Pure relational.
- **Operating-mode distribution generators** (8 of them, all class a) →
  `OpModeDistribution` / `RatesOpModeDistribution`. Joins of `avgSpeedDistribution` ×
  drive-schedule × `opModePolProcAssoc`.
- **BaseRateGenerator** (`crates/moves-calculators/src/generators/baserategenerator/`) →
  `BaseRate`, `BaseRateByAge`. Source-bin-weighted + distance-weighted rates via
  `weighted_mean`. The **drive-cycle physics** path (VSP operating-mode binning,
  `sourceUseTypePhysicsMapping`) is class b — `derive` expressions; the `window` op covers
  the second-by-second Δspeed if the project-domain path is in scope.
  ⚠ **Porting hazard:** the generator's `sumSBD` normalization / model-year-group keying
  (the SBWeighted POC found this) must be ported exactly, or intermediates won't match.
- **BaseRateCalculator** (`crates/moves-calculators/src/calculators/baseratecalculator/`)
  — the fat one. The adjustment chain (`adjust.rs process_fuel_block`), each a `derive`:
  start-temp (3 closed forms by polProcess), general temp+humidity (4 CASE branches via
  `ifelse`), NOx humidity-k, general-fuel-ratio, criteria-ratio, A/C addition, I/M blend,
  EmissionRateAdjustment, E85 THC duplication, EV divisor. Then op-mode `aggregate`
  (`Σ meanBaseRate·marketShare`), then the **activity weight + rate→inventory multiply**
  (`aggregate.rs aggregate_and_apply_activity:191`) as a `join` of rate × `universalActivity`.
  ⚠ Preserve the shorepower 91→93 process relabel without recomputing `polProcessID`
  (`adjust.rs:17`), and the regClass-collapse timing bug fix (audit theme 3).

The criteria-running POC (`esi-poc/criteria_running.esi`, 8/8) already reproduced the
multiplicative adjustment chain + I/M blend + A/C quadratic shape.

### Phase 2 — Activity (first big `scan`)

- **TotalActivityGenerator** (`generators/totalactivitygenerator/`) → `SHO`, `SourceHours`,
  `Starts`, `Population`, `hotellingHours`. The class-c core:
  - **Population growth recurrence** (`population.rs:115`), per cohort, year-over-year:
    `pop[a,y] = pop[a-1,y-1]·survival[a]·migration[y]` (and the age-0 / age-40-terminal
    special cases). See the `scan` sketch in §5.
  - **VMT forward-growth** (`travel.rs`) — same shape.
  - Everything else (vmt/activity/allocation) is weighted joins.
- **ActivityCalculator** (`calculators/activitycalculator/`) — pure relational:
  `activity = baseTable · sourceTypeFuelFraction · regClassFraction` over 8 activity types.
- **DistanceCalculator** — distance-fraction join × `SHO.distance` (skip roadType 1).

### Phase 3 — Chained calculators (speciation, PM, GHG, toxics)

All read the accumulated worker-output `union` and relabel/transform. Pipelines:

| Calculator | ESI shape |
|---|---|
| SO2 | `derive`: `SO2 = meanBaseRate × Wsulfur × energy ÷ energyContent`; `Wsulfur` = `weighted_mean(sulfurLevel, marketShare)`. |
| SulfatePM | `derive` split NonECPM(118) → sulfate(115)+H2O(119)+residue(120); `filter` drops replaced 112/118 zeros. |
| NO / NO2 / HONO | `derive` speciation × `NONO2Ratio` (joined over modelYearGroup) + pollutant relabel. |
| HC speciation | `derive` CH4/NMHC/NMOG/VOC/TOG from THC × ratios; E10 altTHC/altNMHC path via `ifelse`. |
| TOG speciation | `aggregate` residual `NonHAPTOG = NMOG − Σ(species)`, `max(…,0)`. |
| Air toxics (+ distance) | 6 ratio paths, `join` over modelYear ranges (theta-join), `derive ×`. |
| Crankcase (non-PM) | `derive`: `output = input × crankcaseEmissionRatio` (POC proved this **exactly**, 2352/2352). |
| PM10, PM total, brake/tire | `derive ×` ratios / relabel-sum. |
| CO2AE | **2-stage**: pipeline A `derive AtmosphericCO2 = Σ energy·carbon·oxid·(44/12)`; pipeline B `derive CO2eq` from A's output. |

### Phase 4 — Evaporative (the hard class-c state)

- **TankTemperatureGenerator** — closed-form RVP/soak thermodynamics → `derive`.
- **EvaporativePermeation** — POC proved it (9/9): Arrhenius `A·exp(B·tankTemp)`, interval
  `filter`s (model-year range, ethanol bin), `coalesce` for null ETOH.
- **TankVaporVenting / MultidayTankVaporVenting** (the 4.7k- and 6.4k-line files) —
  ⚠ the cumulative/multi-day **soak recurrence** with canister-capacity-capped carry-over
  (`soakDayID` / `PeakHourOfColdSoak`). Maps to `scan` over the soak sequence; state =
  canister load + cumulative vapor vented; `min(load, capacity)` cap inside `step`.
- **Refueling, liquid-leaking** — closed-form `derive` (displacement `exp(…)`, spillage).

### Phase 5 — NONROAD (`crates/moves-nonroad/`)

The whole pipeline is `(SCC × geography × HP) → for year → for tech → for day → for pollutant`.
In ESI the loops are keys; the recurrences are `scan`.

- **Population & growth** (`population/agedist.rs:131`) — the central recurrence:
  `pop[y] = max(0, pop[y-1]·(1+growth))` **plus** the per-year **age forward-shift**
  `mdyrfrc[age] = max(0, tmpfrc[age-1]·(1−scrap[age]))` with the youngest slot as the
  remainder. ⚠ This is a **2-D (year × age) recurrence with a snapshot** — see §5 for how
  it maps to `scan` (year axis) carrying the bounded age vector as state columns.
  - `scrptime` cumulative-scrappage walk + synthetic-sales recurrence (`scrptime.rs:116,140`)
    → `scan`.
  - `modyr` dual cumulative accumulators (`modyr.rs:298,309`, feeding `detage`) → `scan`.
- **Deterioration** (`emissions/exhaust.rs:833`): `DF = 1 + A·min(age,cap)^B` → one `derive`.
- **Exhaust EFs + per-day assembly** (`exhaust.rs:1083`): `emstmp = EF·unitCF·DF·adj·adjtime`,
  with SOx/CO2/CRA reusing the THC product computed earlier in the same row — in ESI that's
  just **ordered `derive` steps** (THC column first, then SOx/CO2 derived from it), no scan.
- **Evaporative incl. `wadeeq`**: transcribe `wadeeq` (`output/strutil.rs:132` — ~10 chained
  4th-order polynomials, the `0.992` temp fix, all arithmetic) into one `derive` expression.
  The diurnal **swing accumulation** (`evaporative.rs:1108`, sum of 5 swing-fraction `wadeeq`
  evaluations × 0.78) is a bounded sum → `union` of 5 `derive`d rows + `aggregate(sum)`, or a
  small `scan`. ⚠ **Fidelity caveat:** `wadeeq` runs in `f32` in `moves.rs`; ESI evaluates in
  `f64` — expect tolerance-level, not bit-exact, agreement (§6).
- **Adjustments** (`emsadj`, `exhaust.rs:497`): temp `exp(coeff·(T−75))`, oxygenate, sulfur,
  altitude, RFG bins, permeation-temp — `derive` with `ifelse` branches.
- **Spatial allocation** (`allocation.rs:340`): `child = parent · Σ(val_child/val_parent)·coeff`
  → `join` + `derive` + `aggregate`.
- **Temporal allocation** (`daymthf.rs:72`, `common.rs:992`): month/day factors → `derive`.
- **Output aggregation** → `aggregate(sum)` to tons (`×CVTTON`).

### Phase 6 — Aggregation, output, fidelity

- **MOVESOutput** (`aggregation/output_aggregate.rs`): group-by-key `aggregate(sum)`;
  non-key dims → null (drop from `by`); sourceType ANY_VALUE (carry from representative);
  `weeksPerMonth = noOfDays/7` rescale as a `derive`; `filter` out synthetic pollutants
  ≥10000 and RunSpec-unselected `(pol,proc)`; drop producer zeros for replaced pollutants.
- **Fidelity** — see §6.

---

## 3. The Full-tier engine (the critical prerequisite)

Everything at MOVES scale depends on a lazy, pushdown-capable ESI engine; the reference
bindings are eager. The pragmatic path:

- **Host it in `moves.rs`.** Add a crate that implements the ESI engine's 9 operators over
  Polars `LazyFrame`, reusing `moves-data-default`'s manifest-driven partition pruning and
  `InputDataManager`'s `WhereClause → Expr` translation. `select`/`filter` push down into
  `scan_parquet`; `join`/`aggregate`/`derive` map to Polars `join`/`group_by().agg()`/
  `with_columns`; `scan`/`window` map to Polars cumulative/`over` expressions (and a
  hand-rolled fold for true recurrences).
- This crate *is* the deliverable that makes ESI viable for MOVES, and it doubles as the
  "Full tier" the ESI libraries spec describes but no reference binding yet implements.

---

## 4. Validation strategy

- **Reuse the existing harness.** `characterization/` already captures per-fixture execution
  inputs + canonical `MOVESOutput`, with a per-table/column `tolerance.toml`. Each ESI
  pipeline's output is diffed the same way.
- **Phase gates.** A phase is "done" when its pipelines reproduce the relevant captured
  intermediate/output within tolerance on the fixtures that exercise it
  (`characterization/fixtures/coverage-matrix.md` maps fixture → processes).
- **First targets = the 8 already-passing fixtures** (`docs/known-divergences.md`): 3 evap +
  nonroad-commercial (precision-only) + the vacuous crankcase/ext-idle ones. The **26
  quarantined fixtures share `moves.rs`'s own open bugs** (audit themes) — don't chase those
  in ESI until `moves.rs` fixes them; matching a known-wrong number is not the goal.
- **Milestone:** a full onroad scenario (e.g. `sample-runspec.xml`) reproduced end-to-end
  within tolerance — the thing the POC explicitly *couldn't* do, because the snapshots don't
  capture the rate→inventory step (`movesworkeroutput` is empty). With the full pipeline
  ported (Phase 1's activity multiply + Phase 2's activity), it becomes achievable.

---

## 5. Worked sketches of the genuinely hard parts

**Population growth (NONROAD `agedist`, year axis) — `scan`:**

```json
{ "id": "grow", "op": "scan", "input": "pop_seed_by_year",
  "partition_by": ["scc", "region", "hpCategory"],
  "order_by": ["year"],
  "init": { "totpop": "basePopulation" },
  "step": { "totpop": { "op": "max", "args": [0,
    { "op": "*", "args": ["prev_totpop",
      { "op": "+", "args": [1, "growthFactor"] }] }] } } }
```

**The age forward-shift (2-D year × age) — the genuine complexity.** `scan` carries *scalar*
state per partition row, so the age vector must be encoded as bounded state columns
(`MXAGYR=51`). Partition by cohort, order by year, and the `step` shifts each slot:

```json
"init": { "f0": "modfrc0", "f1": "modfrc1", "...": "...", "f40": "modfrc40" },
"step": {
  "f40": { "op": "max", "args": [0, { "op": "*", "args": ["prev_f39",
            { "op": "-", "args": [1, "scrap40"] }] }] },
  "f1":  { "op": "max", "args": [0, { "op": "*", "args": ["prev_f0",
            { "op": "-", "args": [1, "scrap1"] }] }] },
  "f0":  { "op": "-", "args": ["totpopfrc", "sum_of_shifted_f1_to_f40"] }
}
```

This is exact but verbose (51 state entries). **Decision point:** accept the verbosity
(generate it programmatically from the catalog bridge), or model age as a key dimension and
do the shift with a `window`/`lag`-style self-join *inside* each scan step — which `scan`
doesn't natively support, so the state-column encoding is the recommended route. Either way
it is expressible without an escape hatch; it is just the most intricate pipeline in the port.

**`wadeeq` — one `derive`, no escape hatch** (showing it really is just arithmetic):

```json
{ "id": "wade_vap_prs", "op": "derive", "input": "diurnal_in", "column": "vap_prs",
  "expr": { "op": "+", "args": [
    { "op": "*", "args": [1.0223, "rvp"] },
    { "op": "/", "args": [ { "op": "*", "args": [0.0119, 3, "rvp"] },
      { "op": "-", "args": [1, { "op": "*", "args": [0.0368, "rvp"] }] } ] } ] } }
```

…followed by `derive`s for `pct_evp` (4th-order in `vap_prs`), the partial pressures, density,
molecular weight, and the final product — a chain of ~10 `derive` steps. Verbose, but
portable and diffable, which is the point.

---

## 6. Fidelity strategy & known gaps

- **Tolerance, not bit-exact.** `moves.rs` matches canonical MOVES *within tolerance*
  (`tolerance.toml`); ESI should target the same bar. Bit-exactness is **not** achievable for
  NONROAD because `moves.rs` runs the NONROAD hot path in `f32` (Fortran `real*4`) and
  preserves Fortran operation order, while ESI evaluates in `f64`. Document this explicitly;
  if bit-exact NONROAD ever matters, it requires an f32 evaluation mode in the engine.
- **Deterministic reductions.** ESI's `aggregate` sums in a defined order (sort-by-magnitude)
  and `scan`/`window` use a stable order — keep the snapshot/order dependencies that
  `agedist.rs` flags (ascending age, youngest slot last) by setting `order_by` accordingly.
- **Generator normalization.** Reproducing a captured intermediate bit-for-bit means porting
  its generator's normalization (`sumSBD`, model-year-group keying), not guessing a formula —
  the SBWeighted POC is the cautionary tale (engine-exact vs Polars, ~25% vs the captured
  canonical until normalization is ported).

---

## 7. Scope boundary (mirrors moves.rs)

In scope (because `moves.rs` implements it): all onroad processes/calculators + 16 generators;
the NONROAD structural pipeline; the 4 control strategies' *semantics*; default-DB (Default
scale) execution.

Out of scope for now (because `moves.rs` defers/stubs it — `AUDIT-canonical-fidelity.md:321`,
`docs/porting-guide.md:126`): NONROAD production fixed-width loaders (`NR*.TMF/.EMF/.SPL/
.SCO/.ALO` — the reason NONROAD is numerically gated); CDB/PDB data-plane wiring; **Rates
mode** (`BaseRateOutput`); RunSpec fuel-year/region-derived filters; uncertainty/Monte-Carlo;
distributed execution; the Swing GUI. The known `bsfc=1.0` and `regClass`-collapse bugs are
`moves.rs` issues — track them, don't replicate them.

---

## 8. Prior art (already done — POC on `poc/esi-calculator-port`)

`esi-poc/` proved the shapes on real data, which de-risks Phases 1–4:

| POC | Proved | Result |
|---|---|---|
| `criteria_running.esi` | adjustment chain: quadratic temp adj, A/C heat-index quad, fuel blend, I/M blend+clamp | 8/8 vs Rust tests |
| `permeation.esi` | Arrhenius `exp`, interval `filter`s, `coalesce` | 9/9 vs Rust tests |
| `crankcase.esi` | real-Parquet loader + model-year theta-join + `× ratio` | **2352/2352 exact** on captured snapshot |
| `sbweighted.esi` | 3-table join + `weighted_mean` at scale (590k-row `EmissionRateByAge`) | engine-exact vs Polars (19,964 cells); canonical match gated on generator normalization |

The new operators (ESI PR #1) close the gaps the POC hit: `scan` for the recurrences,
`window` for ordered access, `aggregate.by` for derived-column grouping, `mod`/`round` for
key decode and fidelity.

---

## 9. Sequencing & open decisions

**Critical path:** Phase 0 (Full-tier engine + catalog bridge) → Phase 1 (rate core) →
Phase 2 (activity → enables end-to-end inventory) → Phase 3 (chained) → Phase 4 (evap) →
Phase 5 (NONROAD) → Phase 6 (aggregation/fidelity). Phases 3, 4, 5 parallelize once 1–2 land.

**Open decisions:**
1. **Where does the Full-tier engine live?** Recommended: a `moves-esi` crate in this repo
   wrapping the Polars reader. Alternative: upgrade the EarthSciInventory Rust binding.
2. **Programmatic `.esi` emission vs hand-authoring.** A catalog bridge that emits table
   schemas + index_sets + the verbose recurrence pipelines (e.g. the 51-slot age scan) from
   the existing registries is strongly preferred over hand-authoring 200+ tables.
3. **NONROAD f32 fidelity** — accept tolerance, or add an f32 engine mode.
4. **`mod`/`round` in ESM** (EarthSciSerialization#20) — minor; keeps the shared namespace
   aligned, not a blocker.

---

## References

- ESI: spec `esi-spec.md`, schema `esi-schema.json`, library spec `esi-libraries-spec.md`,
  new operators [EarthSciInventory#1](https://github.com/EarthSciML/EarthSciInventory/pull/1).
- moves.rs: `characterization/calculator-chains/calculator-dag.json`,
  `characterization/calculator-wiring-audit.md`, `AUDIT-canonical-fidelity.md`,
  `docs/known-divergences.md`, `crates/moves-framework/src/execution/engine.rs`,
  `crates/moves-nonroad/ARCHITECTURE.md`.
