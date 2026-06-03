# ESI proof-of-concept: CriteriaRunningCalculator

Can a real moves.rs calculator — one with genuine math — be re-expressed in the
[ESI format](https://github.com/EarthSciML/EarthSciInventory) and still reproduce
moves.rs's own tests? This is that experiment.

## What's here

- **`criteria_running.esi`** — the computational core of
  `crates/moves-calculators/src/calculators/criteria_running_calculator.rs`,
  expressed as an ESI pipeline.
- **`run_poc.py`** — re-runs the calculator's eight Rust `#[test]` cases through
  the ESI engine and checks each reproduces the asserted `emission_quant`.

## Result

```
8/8 CriteriaRunning tests reproduced through ESI
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
python3 esi-poc/run_poc.py          # needs the EarthSciInventory repo beside moves.rs
cargo test -p moves-calculators --lib criteria_running::   # the Rust baseline
```

`run_poc.py` imports the pure-Python ESI reference engine from
`../EarthSciInventory/implementations/python`.
