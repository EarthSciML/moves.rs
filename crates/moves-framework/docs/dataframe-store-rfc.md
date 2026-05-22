# DataFrameStore RFC

**Status**: Proposed  
**Bead**: mo-o6xhs  
**Implements**: Blocker 2 (Task 50) — data-plane wiring for `moves-framework`

---

## 1. Background and Problem

`CalculatorContext` holds two placeholder structs today:

```rust
pub struct ExecutionTables { _private: () }  // slow tier — read-only default-DB tables
pub struct ScratchNamespace { _private: () } // scratch tier — per-chunk generator output
```

Phase 3 calculators implement `execute(&self, ctx: &CalculatorContext) -> Result<CalculatorOutput>` but cannot read input tables or write outputs because the data plane is absent. Every calculator's `execute` returns `Ok(CalculatorOutput::empty())`.

The DataFrameStore landing (Task 50) replaces both placeholders with concrete Polars-backed storage. This RFC locks the trait/struct surface, backend choice, and concurrency contract before any code lands.

---

## 2. Backend Decision: Polars `DataFrame`

**Decision: use Polars `DataFrame` (not raw Arrow `RecordBatch` or typed-row-vec).**

Rationale:

| Option | Pros | Cons | Decision |
|--------|------|------|----------|
| Polars `DataFrame` | `moves-data-default` already returns `LazyFrame`; `output_processor.rs` uses Parquet round-trip pinned to Polars writer; filter/join expressions reuse existing crate patterns | Polars dependency in `moves-framework` | **CHOSEN** |
| Arrow `RecordBatch` | Lighter, no Polars dependency | Loses existing Polars ergonomics; must re-implement filter/join; inconsistent with the rest of the codebase | Rejected |
| Typed row-vec | Zero new dependencies; already used by all `*Inputs` structs for unit tests | Per-calculator `Vec<ShoRow>` → cannot be shared across calculators without re-deserialization; no schema enforcement; no Parquet round-trip | Rejected for the store; kept as the unit-test data-plane contract |

The typed-row-vec pattern (`Vec<ShoRow>`, `DistanceInputs`, etc.) stays as the *unit-test interface* to calculator kernels. The store is the *execution-time interface*.

---

## 3. Proposed Trait and Struct Surface

### 3.1 `TableSchema`

```rust
/// Static schema declaration for one named table in the execution database.
///
/// Every table that appears in any `Calculator::input_tables()` or
/// `Generator::output_tables()` declaration must have a registered
/// `TableSchema`. Task T3 (`schema_registry`) enforces this at startup.
pub struct TableSchema {
    /// Canonical table name, e.g. `"sho"`, `"sourceBinDistribution"`.
    pub name: &'static str,
    /// Polars column schema: ordered (name, DataType) pairs.
    pub schema: polars::prelude::SchemaRef,
}
```

### 3.2 `DataFrameStore` trait

```rust
/// Read/write access to a collection of named Polars DataFrames.
///
/// Two concrete implementations exist:
/// - `InMemoryStore` — used for both the slow tier and the scratch tier.
///
/// The trait is object-safe; the registry holds `Box<dyn DataFrameStore>`.
pub trait DataFrameStore: Send + Sync {
    /// Return the DataFrame stored under `name`, or `None` if absent.
    fn get(&self, name: &str) -> Option<Arc<polars::prelude::DataFrame>>;

    /// Insert (or replace) a DataFrame under `name`.
    fn insert(&mut self, name: &str, df: polars::prelude::DataFrame);

    /// Return `true` if a DataFrame is stored under `name`.
    fn contains(&self, name: &str) -> bool;

    /// Return all stored table names, in insertion order.
    fn names(&self) -> Vec<&str>;
}
```

### 3.3 `InMemoryStore`

```rust
/// Concrete `DataFrameStore` backed by a `BTreeMap`.
///
/// Used for both the slow tier (loaded once by `InputDataManager`) and
/// the scratch tier (allocated fresh per chunk by the MasterLoop engine).
#[derive(Debug, Default)]
pub struct InMemoryStore {
    tables: BTreeMap<String, Arc<polars::prelude::DataFrame>>,
}
```

### 3.4 Updated `ExecutionTables` and `ScratchNamespace`

```rust
/// Slow tier — per-run filtered default-DB tables, read-only.
///
/// `Arc<InMemoryStore>` is shared across all chunks in a run;
/// no calculator or generator may call `insert` on it during a run.
/// Loading is driven by `InputDataManager` (Task 24 / T3).
pub struct ExecutionTables {
    store: Arc<InMemoryStore>,
}

/// Scratch tier — per-chunk generator output, owned and mutable.
///
/// Allocated fresh at chunk start by `MasterLoopEngine`.  Generators
/// write via `insert`; downstream calculators read via `get`.
/// Dropped when the chunk's iteration completes.
pub struct ScratchNamespace {
    store: InMemoryStore,
}
```

`ExecutionTables::empty()` continues to work: `Arc::new(InMemoryStore::default())`.

### 3.5 Updated `CalculatorContext`

```rust
pub struct CalculatorContext {
    tables: ExecutionTables,        // Arc-shared, read-only slow tier
    scratch: ScratchNamespace,      // owned, mutable scratch tier
    position: IterationPosition,
}
```

`Generator::execute` signature changes to `&mut CalculatorContext` to allow scratch writes. `Calculator::execute` stays `&CalculatorContext` (read-only).

### 3.6 `CalculatorOutput`

```rust
/// Value returned by `Calculator::execute`.
///
/// Wraps an optional emission DataFrame.  `empty()` is valid for Phase 2
/// shells still waiting on data-plane wiring; `with_dataframe(df)` is used
/// by wired calculators starting with the DistanceCalculator pilot (T5).
pub struct CalculatorOutput {
    df: Option<polars::prelude::DataFrame>,
}

impl CalculatorOutput {
    pub fn empty() -> Self { Self { df: None } }
    pub fn with_dataframe(df: polars::prelude::DataFrame) -> Self { Self { df: Some(df) } }
    pub fn into_dataframe(self) -> Option<polars::prelude::DataFrame> { self.df }
}
```

---

## 4. Typed Helpers (Task T3)

To shield calculator authors from raw Polars schema manipulation, Task T3 adds:

```rust
/// Conversion from a typed row-vec to a Polars DataFrame.
/// Implemented by derive-helper for each `*Row` struct.
pub trait IntoDataFrame {
    fn into_dataframe(self) -> Result<polars::prelude::DataFrame, Error>;
}

// Convenience on the store:
impl InMemoryStore {
    /// Insert a typed row-vec as a DataFrame, validating schema.
    pub fn insert_typed<R: IntoDataFrame>(&mut self, name: &str, rows: Vec<R>)
        -> Result<(), Error>;

    /// Read a named table back into a typed row-vec.
    pub fn iter_typed<R: TryFrom<polars::prelude::DataFrame>>(&self, name: &str)
        -> Result<Vec<R>, Error>;
}
```

These helpers let `DistanceCalculator::execute` read:

```rust
let sho: Vec<ShoRow> = ctx.tables().store.iter_typed("sho")?;
```

and let `SourceBinDistributionGenerator::execute` write:

```rust
ctx.scratch_mut().store.insert_typed("sourceBinDistribution", output.distribution)?;
```

---

## 5. Error Variants

```rust
pub enum Error {
    // ... existing variants ...

    /// A table required by a calculator was absent from the store.
    TableNotFound { name: String },

    /// A DataFrame in the store did not match the expected schema.
    SchemaMismatch { name: String, expected: String, got: String },

    /// A Polars operation failed.
    Polars(polars::error::PolarsError),
}
```

---

## 6. Concurrency Contract

| Tier | Sharing | Mutability | Who owns |
|------|---------|------------|----------|
| Slow (`ExecutionTables`) | `Arc<InMemoryStore>` — all chunks share one instance | Read-only after load | `InputDataManager` loads once; `MasterLoopEngine` hands `Arc` clone to each chunk |
| Scratch (`ScratchNamespace`) | No sharing — each chunk owns its own `InMemoryStore` | Mutable within a chunk; generators write before calculators read | `MasterLoopEngine` allocates at chunk start; drops at chunk end |

**Invariant tests (names, not yet implemented):**

```
chunk_scratch_is_isolated_across_chunks
chunk_scratch_visible_within_chunk_topo_order
generator_write_then_calculator_read_same_chunk_roundtrips
concurrent_chunks_do_not_observe_each_others_scratch
slow_tier_is_immutable_after_load
store_insert_then_get_round_trips
store_get_unknown_returns_none
store_insert_duplicate_replaces
store_can_be_held_inside_calculator_context
```

---

## 7. Worked Examples

### 7.1 `DistanceCalculator` reading SHO, Link, County

**Schema mapping for `DistanceInputs`:**

| `DistanceInputs` field | Store table name | Key columns |
|------------------------|-----------------|-------------|
| `sho` | `"sho"` | `linkID, sourceTypeID, modelYearID, roadTypeID, hourDayID, monthID, yearID, distance: f64` |
| `link` | `"link"` | `linkID, countyID, zoneID, roadTypeID, avgSpeedBinID` |
| `county` | `"county"` | `countyID, stateID` |
| `source_bin` | `"sourceBin"` | `sourceBinID, fuelTypeID, regClassID, engTechID` |
| `source_bin_distribution` | `"sourceBinDistribution"` | `sourceBinID, polProcessID, sourceTypeModelYearID, sourceBinActivityFraction: f64` |
| `source_type_model_year` | `"sourceTypeModelYear"` | `sourceTypeModelYearID, sourceTypeID, modelYearID` |
| `hour_day` | `"hourDay"` | `hourDayID, hourID, dayID` |

**Wired `execute` (sketch):**

```rust
fn execute(&self, ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
    let inputs = DistanceInputs {
        sho:                     ctx.tables().store.iter_typed("sho")?,
        link:                    ctx.tables().store.iter_typed("link")?,
        county:                  ctx.tables().store.iter_typed("county")?,
        source_bin:              ctx.tables().store.iter_typed("sourceBin")?,
        source_bin_distribution: ctx.tables().store.iter_typed("sourceBinDistribution")?,
        source_type_model_year:  ctx.tables().store.iter_typed("sourceTypeModelYear")?,
        hour_day:                ctx.tables().store.iter_typed("hourDay")?,
    };
    let rows = Self::calculate(&inputs, ctx.position().process_id.unwrap_or(ProcessId(1)))?;
    let df = rows.into_dataframe()?;
    Ok(CalculatorOutput::with_dataframe(df))
}
```

### 7.2 `SourceBinDistributionGenerator` writing `SourceBinDistribution`

**Schema mapping for output:**

| Output | Store table name | Key columns |
|--------|-----------------|-------------|
| `SourceBinDistributionOutput::distribution` | `"sourceBinDistribution"` | `sourceBinID: i64, polProcessID: i64, sourceTypeModelYearID: i64, sourceBinActivityFraction: f64` |
| `SourceBinDistributionOutput::new_source_bins` | `"sourceBin"` | `sourceBinID: i64, fuelTypeID: i64, engTechID: i64, regClassID: i64, engSizeID: i64, weightClassID: i64` |

**Wired `execute` (sketch):**

```rust
fn execute(&self, ctx: &mut CalculatorContext) -> Result<CalculatorOutput, Error> {
    let svp: Vec<SampleVehiclePopulationRow> =
        ctx.tables().store.iter_typed("sampleVehiclePopulation")?;
    let ppmyr: Vec<PollutantProcessModelYearRow> =
        ctx.tables().store.iter_typed("pollutantProcessModelYear")?;
    // ... build tables and call pollutant_process_distribution() ...
    let output = pollutant_process_distribution(&tables, pol_process_id);
    ctx.scratch_mut().store.insert_typed("sourceBinDistribution", output.distribution)?;
    ctx.scratch_mut().store.insert_typed("sourceBin", output.new_source_bins)?;
    Ok(CalculatorOutput::empty())
}
```

---

## 8. Row Struct → Schema Map (Acceptance Spot-Check)

The three calculator `*Inputs` structs and one generator reviewed against this RFC:

### `DistanceInputs` (distance_calculator.rs)
Fields: `source_bin`, `source_bin_distribution`, `source_type_model_year`, `sho`, `hour_day`, `link`, `county` — all mapped in §7.1.

### `So2Inputs` (so2_calculator.rs)
Fields: `fuel_supply`, `fuel_formulation`, `fuel_sub_type`, `fuel_type`, `year`, `sulfate_emission_rate`, `pollutant_process_assoc`, `run_spec_model_year`, `month_of_any_year`, `general_fuel_ratio`, `energy`.

| Field | Table name |
|-------|-----------|
| `fuel_supply` | `"fuelSupply"` |
| `fuel_formulation` | `"fuelFormulation"` |
| `fuel_sub_type` | `"fuelSubType"` |
| `fuel_type` | `"fuelType"` (select `fuelTypeID` column as `Vec<i32>`) |
| `year` | `"year"` |
| `sulfate_emission_rate` | `"sulfateEmissionRate"` |
| `pollutant_process_assoc` | `"pollutantProcessAssoc"` |
| `run_spec_model_year` | `"runSpecModelYear"` (select `modelYearID`) |
| `month_of_any_year` | `"monthOfAnyYear"` |
| `general_fuel_ratio` | `"generalFuelRatio"` |
| `energy` | `"movesWorkerOutput"` (filtered to `pollutantID = 91`) |

### `CriteriaRunningInputs` (criteria_running_calculator.rs)
21 fields covering: `age_category`, `county`, `criteria_ratio`, `emission_rate_by_age`, `fuel_formulation`, `fuel_subtype`, `fuel_supply`, `full_ac_adjustment`, `fuel_type`, `hour_day`, `im_coverage`, `im_factor`, `link`, `model_year`, `month_group_hour`, `month_of_any_year`, `op_mode_distribution`, `pollutant_process_assoc`, `pollutant_process_mapped_model_year`, `sho`, `source_bin`, `source_bin_distribution`, `source_type_age`, `source_type_model_year`, `temperature_adjustment`, `year`, `zone_month_hour`. Each maps to the snake_case → camelCase table name via `iter_typed`.

### `SourceBinDistributionGenerator` (source_bin_distribution_generator.rs)
Reads: `sampleVehiclePopulation`, `pollutantProcessModelYear`, `modelYearGroup`, `sourceTypePolProcess`, `sourceTypeModelYear`, `runSpecSourceFuelType`, `fuelUsageFraction`.  
Writes: `sourceBin`, `sourceBinDistribution`. Mapped in §7.2.

---

## 9. Implementation Order (Blocker 2 sub-tasks)

| Tag | Task | Gates |
|-----|------|-------|
| b2-t1 | Add `DataFrameStore` trait + `InMemoryStore` + replace placeholders (`mo-u40zv`) | All |
| b2-t2 | Typed helpers: `IntoDataFrame`, `iter_typed`, schema registry (`mo-92rst`) | b2-t1 |
| b2-t3 | Parquet round-trip determinism (`mo-99fn8`) | b2-t2 |
| b2-t4 | Slow-tier loading via `InputDataManager` (`mo-j7rtx`) | b2-t2 |
| b2-t5 | Pilot wire: `DistanceCalculator::execute` end-to-end (`mo-ymv41`) | b2-t2 |
| b2-t6 | Scratch-tier writes with per-chunk ownership (`mo-um3el`) | b2-t4 |
| b2-t7 | NONROAD data-plane integration (`mo-jshvm`) | b2-t6 |

---

## 10. Non-goals

- Schema validation at `insert` time (deferred to Task T3 schema registry).
- Parquet round-trip for the scratch tier (scratch is in-memory only; only slow-tier snapshots use Parquet).
- Cross-chunk scratch sharing (explicit non-goal; scratch is per-chunk owned).
- Broadcasting to all calculators beyond the `DistanceCalculator` pilot in b2-t5 (that is Blocker 3).
