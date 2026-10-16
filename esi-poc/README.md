# ESI proof-of-concept: re-implementing MOVES calculators

Can real moves.rs calculators — ones with genuine math — be re-expressed in the
[ESI format](https://github.com/EarthSciML/EarthSciInventory) and still reproduce
moves.rs's own results? This is that experiment: two calculators reproduced from
their Rust unit-test fixtures (chosen to exercise *different* capabilities), plus
one run end-to-end against a **real characterization snapshot's captured Parquet**.

## What's here

| Calculator | ESI file | Harness | Source of truth | Reproduced |
|---|---|---|---|---|
| `criteria_running_calculator.rs` | `criteria_running.esi` | `run_poc.py` | Rust `#[test]` fixtures | 8/8 |
| `evaporative_permeation_calculator.rs` | `permeation.esi` | `run_permeation.py` | Rust `#[test]` fixtures | 9/9 |
| `crankcase_emission.rs` (core) | `crankcase.esi` | `run_crankcase.py` | **real captured snapshot Parquet** | 2352/2352 exact |
| source-bin weighting (`BaseRateGenerator`) | `sbweighted.esi` | `run_sbweighted.py` | **real Parquet, 3-table join at scale** | engine exact vs Polars on 19,964 cells |

The first two harnesses re-run the calculator's Rust `#[test]` cases — same
fixture values, same per-test tweaks — through the pure-Python ESI reference
engine and check each reproduces the asserted `emission_quant` (or, for the
filter cases, the empty result). The third loads a real `CrankcaseEmissionRatio`
table straight from a committed characterization snapshot and verifies against
the captured values (see [Real-data end-to-end](#real-data-end-to-end)).

## Capabilities exercised (the point of picking two)

| ESI capability | CriteriaRunning | Permeation | Crankcase |
|---|:--:|:--:|:--:|
| `join` (equi) + cartesian cross-join | ✅ | ✅ | ✅ |
| weighted `aggregate` (sum) | ✅ | ✅ | ✅ |
| `derive` polynomial — quadratic `1+(T−75)(A+(T−75)B)` | ✅ | | |
| `min`/`max` clamp | ✅ | | |
| **`exp`** — Arrhenius `A·exp(B·tankTemp)` | | ✅ | |
| **`filter`** — interval predicates (model-year range, ethanol bin) | | ✅ | |
| **`coalesce`** — null ETOH volume → 0 | | ✅ | |
| **interval theta-join** — `min≤MY≤max` window join | | | ✅ |
| **real Parquet data-source loader** (snapshot) | | | ✅ |

CriteriaRunning is the multiplicative adjustment chain with an I/M blend;
Permeation adds genuine exponential physics and conditional/interval logic;
Crankcase adds a `join` predicate (the model-year window) and the data-source
loader reading real captured Parquet; SBWeighted adds `weighted_mean` and a
three-table join over the real 590k-row rate table at scale. Together they cover
the relational core plus polynomial, exponential, clamp, filter, null-handling,
interval-join, weighted-mean, and real-data loading.

## Real-data end-to-end

`run_crankcase.py` is the first POC that runs against **real captured data**
rather than transcribed fixtures. It loads the `CrankcaseEmissionRatio` table
(572 rows) straight from `characterization/snapshots/process-crankcase-running`
via an ESI `parquet` data-source loader, applies the crankcase ratio
(`crankcaseEmission = exhaustEmission × crankcaseRatio`) through the model-year
window join, and verifies **every** cell against a Polars ground-truth lookup:

```
loaded real CrankcaseEmissionRatio: 572 rows
2352/2352 cells match the captured ratios (ESI Parquet loader + interval theta-join + multiply)

model-year window selection on real data (polProcess 115, sourceType 21, regClass 20, fuelType 1):
  captured windows: [1950,1968]->0.33, [1969,2060]->0.0132
  MY 1960  ->  ESI crankcaseRatio 0.33
  MY 1975  ->  ESI crankcaseRatio 0.0132
```

`run_sbweighted.py` goes one step further — a **multi-step, multi-table chain at
scale**. It joins three real captured tables (the 590k-row `EmissionRateByAge`,
`SourceBinDistribution`, and `SourceBin`) and computes the source-bin-weighted
base rate with a `weighted_mean` aggregate. Two findings:

```
[1] ENGINE: ESI vs Polars (same formula, same real data): 19964 cells, 0 mismatches -> EXACT MATCH
[2] CANONICAL: 713/2852 cells within 2% of the captured SBWeightedEmissionRateByAge.meanBaseRate
```

The ESI **engine** executes the 3-table join + weighted-mean on real data exactly
(verified against an independent Polars recomputation). The *formula* only
loosely tracks the canonical intermediate, because the real `BaseRateGenerator`
adds `sumSBD` normalization and model-year-group keying — which is exactly the
point: reproducing a non-trivial captured intermediate *bit-for-bit* means
porting its generator, not guessing a formula.

**Scope/honesty.** A full scenario's *final* output (`MOVESOutput`) is the
aggregate of the entire calculator DAG (dozens of steps); the rate→inventory
multiply (rate × activity, with fuel/regClass splitting — the `ActivityCalculator`
back-half) is **not captured** in the snapshots (worker output is empty), so a
single hand-authored pipeline can't reproduce a whole scenario's emissions
end-to-end. The clean exact wins are where there's no hidden normalization
(crankcase's pure `× ratio`); multi-step chains run correctly on the real data
(SBWeighted's engine match) but exact canonical fidelity tracks the per-generator
porting. All of this uses the same `parquet` loader + pushdown path a production
`moves.rs`→ESI bridge would use (ESI libraries spec §4.7).

## Result

```
8/8        CriteriaRunning  tests reproduced through ESI (Rust fixtures)
9/9        Permeation       tests reproduced through ESI (Rust fixtures)
2352/2352  Crankcase        cells reproduced through ESI, EXACT (real snapshot Parquet)
19964/19964  SBWeighted     engine cells match Polars, EXACT (real Parquet, 3-table join + weighted_mean)
```

| Rust test (`criteria_running_calculator.rs`) | expected `emission_quant` | ESI |
|---|--:|:--:|
| `calculate_minimal_input_yields_one_row` | 2100.0 | ✅ |
| `calculate_applies_the_temperature_adjustment` | 1575.0 | ✅ |
| `calculate_applies_the_air_conditioning_adjustment` | 6300.0 | ✅ |
| `calculate_without_im_coverage_leaves_emission_unadjusted` | 3000.0 | ✅ |
| `calculate_clamps_negative_im_blend_to_zero` | 0.0 | ✅ |
| `calculate_weights_emission_rates_across_source_bins` | 4200.0 | ✅ |
| `calculate_sums_emission_rates_across_operating_modes` | 3900.0 | ✅ |
| `calculate_nox_humidity_branch_is_a_passthrough` | 2100.0 | ✅ |

## The math it exercises (not just lookups)

The pipeline reproduces the calculator's genuine closed-form adjustments and its
weighting/blend structure:

- **Fuel ratio blend** (market-share-weighted, GPA-blended):
  `fuelAdj = Σ_formulation marketShare · (ratio·(1−gpaFract) + ratioGPA·gpaFract)`
  — a `derive` + weighted `aggregate`.
- **Temperature adjustment** (quadratic): `tempAdj = 1 + (T−75)·(A + (T−75)·B)`.
- **A/C adjustment** (heat-index quadratic + clamp):
  `acOn = clamp(acA + h·(acB + h·acC), 0, 1)`,
  `acAdj = 1 + (fullAC − 1)·(acOn · penetration · functioningAC)`.
- **Source-bin and operating-mode weighting**: `aggregate(sum)` of
  `rate · binFraction · opModeFraction`.
- **SHO multiply and I/M blend with clamp**:
  `emission = max(quantIM·imFract + quant·(1−imFract), 0)`.

In ESI terms: `derive` (with the shared ESM scalar Expression AST) does the
per-row math; `join`/`aggregate` do the select-multiply-sum; the whole calculator
is one `aggregate(derive(join…))` pipeline — exactly the structure we expected
from the master equation `Emission = Σ Activity × BaseRate × ∏adj`.

## Scope / honesty

This reproduces the calculator's **computation**, fed the same fixture values the
Rust tests use. To keep the POC focused on the math:

- Single-valued output dimensions in the fixture (county, zone, link, year,
  month, day, hour, sourceType, modelYear) are held constant and omitted from
  keys.
- The surrogate-key resolution joins (sourceBin→fuelType, age→modelYear,
  link→zone→temperature, hourDay→hour, etc.) are pre-applied in the inputs that
  `run_poc.py` supplies, so the pipeline expresses the *adjustment chain and
  weighting*, not the dimension bookkeeping. A production port would add those
  joins (all expressible with the same `join`/`map_dim` ops) and bind the real
  default-DB tables via an ESI data-source loader (see the ESI libraries spec
  §4.7) — e.g. `moves.rs` reading its own Parquet export.
- "No I/M coverage" is modeled as `imAdjustFract = 0`, which is numerically
  identical to skipping the blend.

## Run it

```bash
# ESI re-implementations (need the EarthSciInventory repo beside moves.rs)
python3 esi-poc/run_poc.py
python3 esi-poc/run_permeation.py
python3 esi-poc/run_crankcase.py        # real snapshot Parquet; also needs `polars`
python3 esi-poc/run_sbweighted.py       # real Parquet, 3-table join + weighted_mean

# The Rust baselines
cargo test -p moves-calculators --lib criteria_running_calculator
cargo test -p moves-calculators --lib evaporative_permeation_calculator
cargo test -p moves-calculators --lib crankcase_emission
```

Both harnesses import the pure-Python ESI reference engine from
`../EarthSciInventory/implementations/python`. The `Scope / honesty` notes below
apply to both: the surrogate-key resolution joins are pre-applied in the
supplied inputs, single-valued output dimensions are held constant, and the math
chain + weighting + filters are what the pipelines reproduce.
