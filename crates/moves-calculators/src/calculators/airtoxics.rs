//! Port of `calc/airtoxics/airtoxics.go` — the onroad `AirToxicsCalculator`.
//!
//! Migration plan: Phase 3, Task 50. The Nonroad sibling (Task 52,
//! `NRAirToxicsCalculator`) is the [`super::nrairtoxics`] module.
//!
//! # What this calculator does
//!
//! `AirToxicsCalculator` derives air-toxic pollutants from already-computed
//! emissions by scaling them with ratio lookup tables. It produces the
//! organic gaseous toxics — benzene, ethanol, MTBE, 1,3-butadiene, the
//! aldehydes (formaldehyde, acetaldehyde, acrolein, propionaldehyde), the
//! aromatics (ethyl benzene, hexane, styrene, toluene, xylene,
//! 2,2,4-trimethylpentane) — and the polycyclic-aromatic-hydrocarbon (PAH)
//! gas and particle species. The metallic toxics and dioxins/furans are
//! data-driven through the same generic ratio engine (the `ATRatioNonGas`
//! path) but, for the *onroad* calculator, are not in its own registration
//! set — see [`Calculator::registrations`].
//!
//! It is a **chained** calculator: it has no master-loop subscription of its
//! own and runs when the calculators it chains to produce their output (its
//! [`upstream`](Calculator::upstream) modules, `HCSpeciationCalculator` and
//! `SulfatePMCalculator`).
//!
//! # The six ratio-application paths
//!
//! The Go `calculate` runs six independent paths over every input
//! [`FuelBlock`]. Each path scales one input pollutant into a set of toxic
//! output pollutants and is gated by an [`ModuleFlags`] flag:
//!
//! ```text
//! path             input pollutant     ratio table           lookup key
//! ---------------- ------------------- ---------------------- --------------------------------
//! MinorHAPRatio    VOC (87)            minorHAPRatio          process, emission fuel subtype, model year
//! PAHGasRatio      VOC (87)            pahGasRatio            process, block fuel type, model year
//! PAHParticleRatio Organic Carbon(111) pahParticleRatio       process, block fuel type, model year
//! ATRatioGas1      any (chained)       ATRatio                emission fuel formulation, month, model year, output polProcess
//! ATRatioGas2      any (chained)       ATRatioGas2            output polProcess, source type, emission fuel subtype
//! ATRatioNonGas    any (chained)       ATRatioNonGas          output polProcess, source type, emission fuel subtype, model year
//! ```
//!
//! The first three paths scale by a ratio directly: `output = input * atRatio`.
//! The last three (`ATRatio*`) are *chained-to* paths — a `RunSpecChainedTo`
//! row maps the input block's `polProcessID` to the toxic
//! `(polProcessID, pollutantID, processID)` it produces, and the ratio table
//! supplies the multiplier.
//!
//! An emission carries both an emission quantity and an emission rate; every
//! scaling above multiplies *both*.
//!
//! # The nine lookup tables
//!
//! * `minorHAPRatio`, `pahGasRatio`, `pahParticleRatio` — each maps a key to a
//!   *list* of `RatioDetail` (output pollutant + ratio); a key can carry one
//!   detail per output pollutant;
//! * `ATRatioGas1ChainedTo`, `ATRatioGas2ChainedTo`, `ATRatioNonGasChainedTo`
//!   — `RunSpecChainedTo` extracts mapping an input `polProcessID` to a list of
//!   `ChainedToDetail`;
//! * `ATRatio`, `ATRatioGas2`, `ATRatioNonGas` — each maps a key to a single
//!   `f64` ratio.
//!
//! # `ModuleFlags` — the `ATC_Use*` modules
//!
//! The Java `AirToxicsCalculator` enables a subset of the SQL script's
//! `Use*` sections per runspec and passes them to the Go worker as
//! `ATC_<section>` external modules; the Go reads them through
//! `mwo.NeedsModule`. [`ModuleFlags`] models the six flags. A path runs only
//! when its flag is set *and* its ratio table is non-empty — the Go
//! `useX := len(table) > 0 && mwo.NeedsModule("ATC_UseX")`.
//!
//! # Relationship to `database/AirToxicsCalculator.sql`
//!
//! The pinned MOVES ships both the SQL script and the Go worker; the **Go is
//! the modern worker** and is what this port follows. The SQL `Processing`
//! section and the Go diverge in *shape*, not in result:
//!
//! * The SQL multiplies the `ATRatio*` paths by `AT*FuelSupply.marketShare`;
//!   the Go does not. This is not a numerical divergence: a Go `MWOEmission`
//!   is already a per-fuel-formulation slice, so the market-share split the
//!   SQL performs is already embodied in the input emission.
//! * The SQL `Processing` section reads `minorHAPRatio` (market-weighted, keyed
//!   on fuel *type* and month); the Go reads the separate `minorHAPRatioGo`
//!   extract (raw ratio, keyed on fuel *subtype*, no month). The two extracts
//!   of the same source table are designed to give equivalent results given
//!   the Go's per-formulation emission decomposition. This port mirrors the Go.
//!
//! The Go also tabulates `ATRatioGas2` from a default-DB `float` (32-bit)
//! column; the value is f32-precision once extracted, then carried as `f64`
//! exactly as the Go's text-file parse does.
//!
//! # Scope of this port
//!
//! The pinned Go file is the whole `airtoxics` package: the in-memory
//! lookup-table load (`StartSetup`) and the per-block pass (`calculate`).
//! Both are ported — [`AirToxics::build`] and [`AirToxics::air_toxics_block`].
//! The Go ran `calculate` as a pool of goroutines draining a channel of
//! `MWOBlock`s; that worker plumbing is not part of the calculation and is
//! dropped. This port keeps the **computation** and replaces the channel
//! boundary with plain values: a [`FuelBlock`] in, [`ToxicFuelBlock`]s out.
//!
//! The Go's per-output-block `NeedsGFRE` check is a documented no-op
//! ("No GFRE is used by the AirToxics calculator") and is not modeled.
//!
//! # Fidelity notes
//!
//! * **Append, not overwrite.** The Go's `addEmission` appends each scaled
//!   emission to the output block's `Emissions` slice. When two ratio rows
//!   name the same output pollutant, *both* scaled emissions are kept. This
//!   port pushes to [`ToxicFuelBlock::emissions`] and does not deduplicate.
//! * **Output process equals input process for the direct paths.** The Go
//!   `addEmission` copies the input block's key and overrides only
//!   `pollutantID` / `polProcessID`, so a `minorHAPRatio` / `pahGasRatio` /
//!   `pahParticleRatio` output keeps the *input* block's process. The
//!   `ATRatio*` paths instead take the process from the chained-to row.
//! * **Output order.** The Go grouped output blocks in a Go `map` keyed by
//!   `polProcessID`, whose iteration order is randomised.
//!   [`air_toxics_block`](AirToxics::air_toxics_block) returns the blocks in
//!   ascending `polProcessID` order so the output is deterministic; a
//!   fuel-block set is unordered, so this is a presentation choice only.
//!
//! # Data plane
//!
//! [`Calculator::execute`] receives a [`CalculatorContext`] whose
//! `ExecutionTables` / `ScratchNamespace` are Phase-2 placeholders until the
//! `DataFrameStore` data plane lands, so `execute` cannot yet
//! read the nine lookup tables nor the upstream emission blocks, nor write the
//! toxic blocks back. The numerically faithful algorithm is fully ported and
//! unit-tested on [`AirToxics`]; once the data plane exists, `execute` builds
//! an [`AirToxics`] from `ctx.tables()`, reads the input [`FuelBlock`]s,
//! applies [`air_toxics_block`](AirToxics::air_toxics_block), and stores the
//! resulting [`ToxicFuelBlock`]s.

use std::collections::{BTreeMap, HashMap};

use moves_data::{PollutantId, PollutantProcessAssociation, ProcessId};
use moves_framework::{
    Calculator, CalculatorContext, CalculatorOutput, CalculatorSubscription, DataFrameStoreTyped,
    Error, TableRow,
};
use polars::prelude::{DataFrame, DataType, NamedFrom, PolarsResult, Schema, Series};

/// VOC — volatile organic compounds, pollutant 87. The input the
/// `minorHAPRatio` and `pahGasRatio` paths scale from.
const VOC_POLLUTANT_ID: i32 = 87;

/// Organic Carbon, pollutant 111. The input the `pahParticleRatio` path scales
/// from.
const ORGANIC_CARBON_POLLUTANT_ID: i32 = 111;

/// One emission record — the Go `mwo.MWOEmission`, restricted to the fields
/// the air-toxics calculator reads and writes.
///
/// An emission carries a quantity and a rate; the scaling formulas multiply
/// *both*. `fuel_sub_type_id` and `fuel_formulation_id` identify the fuel the
/// emission belongs to: `fuel_sub_type_id` keys the `minorHAPRatio`,
/// `ATRatioGas2` and `ATRatioNonGas` lookups, `fuel_formulation_id` keys the
/// `ATRatio` lookup. Both are carried through unchanged onto every toxic
/// emission derived from this one.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Emission {
    /// `fuelSubTypeID` — the emission's fuel subtype.
    pub fuel_sub_type_id: i32,
    /// `fuelFormulationID` — the emission's fuel formulation.
    pub fuel_formulation_id: i32,
    /// `emissionQuant` — the emission quantity (mass).
    pub emission_quant: f64,
    /// `emissionRate` — the emission rate.
    pub emission_rate: f64,
}

impl Emission {
    /// A linearly scaled copy — the Go `mwo.NewEmissionScaled`.
    ///
    /// Both the quantity and the rate are multiplied by `factor`; the fuel
    /// subtype and formulation ids are copied unchanged.
    #[must_use]
    pub fn scaled(&self, factor: f64) -> Emission {
        Emission {
            fuel_sub_type_id: self.fuel_sub_type_id,
            fuel_formulation_id: self.fuel_formulation_id,
            emission_quant: factor * self.emission_quant,
            emission_rate: factor * self.emission_rate,
        }
    }
}

/// The key fields of an MWO `FuelBlock` that the air-toxics calculator reads —
/// a subset of the Go `mwo.MWOKey`.
///
/// The Go `calculate` reads only these seven fields of the input block's key;
/// the rest of `MWOKey` (geography, time-of-day, source-use type detail, …)
/// is opaque passthrough that the data-plane integration copies onto the
/// output blocks, so it is not modeled here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FuelBlockKey {
    /// `pollutantID` — gates the direct paths: `minorHAPRatio` / `pahGasRatio`
    /// consume VOC (87), `pahParticleRatio` consumes Organic Carbon (111).
    pub pollutant_id: i32,
    /// `processID` — keys the three direct ratio tables and is the output
    /// process of every emission they produce.
    pub process_id: i32,
    /// `polProcessID` — keys the three `ATRatio*ChainedTo` lookups.
    pub pol_process_id: i32,
    /// `modelYearID` — part of the `minorHAPRatio` / `pahGasRatio` /
    /// `pahParticleRatio` / `ATRatio` / `ATRatioNonGas` keys.
    pub model_year_id: i32,
    /// `fuelTypeID` — part of the `pahGasRatio` / `pahParticleRatio` keys.
    pub fuel_type_id: i32,
    /// `monthID` — part of the `ATRatio` key.
    pub month_id: i32,
    /// `sourceTypeID` — part of the `ATRatioGas2` / `ATRatioNonGas` keys.
    pub source_type_id: i32,
}

/// One input fuel block — the Go `mwo.FuelBlock`, restricted to the key fields
/// and emissions the calculator consumes.
#[derive(Debug, Clone, PartialEq)]
pub struct FuelBlock {
    /// The block's key fields.
    pub key: FuelBlockKey,
    /// The per-fuel-formulation emissions in the block.
    pub emissions: Vec<Emission>,
}

/// One output fuel block — a single toxic `(pollutant, process)`'s emissions.
///
/// The Go `calculate` produced these by copying the input block's key,
/// overriding the pollutant / process / polProcess fields, and attaching the
/// derived emissions. This port returns the three computed key fields plus the
/// emissions; copying the rest of the input key is data-plane plumbing the
/// caller handles.
#[derive(Debug, Clone, PartialEq)]
pub struct ToxicFuelBlock {
    /// `pollutantID` of the derived toxic.
    pub pollutant_id: i32,
    /// `processID` of the derived toxic — the input block's process for the
    /// direct paths, the chained-to row's process for the `ATRatio*` paths.
    pub process_id: i32,
    /// `polProcessID` — `pollutantID * 100 + processID` for the direct paths,
    /// the chained-to row's `outputPolProcessID` for the `ATRatio*` paths.
    pub pol_process_id: i32,
    /// The derived emissions — one per `(input emission × applicable ratio
    /// row)` that produced this `(pollutant, process)`, in the order the Go
    /// appended them.
    pub emissions: Vec<Emission>,
}

/// One `minorHAPRatio` table row — input to [`AirToxics::build`].
///
/// The Go `StartSetup` reads these columns from the `minorhapratiogo` extract
/// file (the SQL `cache select` into `##minorHAPRatioGo##` lists them in this
/// order): `processID, outputPollutantID, fuelSubTypeID, modelYearID, atRatio`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MinorHapRatioRow {
    /// `processID`.
    pub process_id: i32,
    /// `outputPollutantID` — the toxic this row produces.
    pub output_pollutant_id: i32,
    /// `fuelSubTypeID`.
    pub fuel_sub_type_id: i32,
    /// `modelYearID`.
    pub model_year_id: i32,
    /// `atRatio` — the toxic-to-VOC ratio.
    pub at_ratio: f64,
}

/// One `pahGasRatio` or `pahParticleRatio` table row — input to
/// [`AirToxics::build`].
///
/// Both tables share this column layout (the Go reads them with the same parse
/// lambda; the SQL `cache select`s list the same columns): `processID,
/// outputPollutantID, fuelTypeID, modelYearID, atRatio`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PahRatioRow {
    /// `processID`.
    pub process_id: i32,
    /// `outputPollutantID` — the PAH species this row produces.
    pub output_pollutant_id: i32,
    /// `fuelTypeID`.
    pub fuel_type_id: i32,
    /// `modelYearID`.
    pub model_year_id: i32,
    /// `atRatio` — the toxic-to-input ratio.
    pub at_ratio: f64,
}

/// One `RunSpecChainedTo` row for an `ATRatio*` path — input to
/// [`AirToxics::build`].
///
/// The Go reads these six columns from an `atratio*chainedto` extract file
/// (`cache SELECT * FROM RunSpecChainedTo`). [`AirToxics::build`] keys the
/// table on `input_pol_process_id` and stores only the three `output_*`
/// columns as a `ChainedToDetail`; `input_pollutant_id` and
/// `input_process_id` are carried for column fidelity but not consumed.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ChainedToRow {
    /// `outputPolProcessID` — the toxic `polProcessID` this row produces.
    pub output_pol_process_id: i32,
    /// `outputPollutantID` — the toxic pollutant.
    pub output_pollutant_id: i32,
    /// `outputProcessID` — the toxic process.
    pub output_process_id: i32,
    /// `inputPolProcessID` — the source emission's `polProcessID`; this is the
    /// lookup key.
    pub input_pol_process_id: i32,
    /// `inputPollutantID` — the source pollutant. Not consumed by the Go's
    /// `calculate`.
    pub input_pollutant_id: i32,
    /// `inputProcessID` — the source process. Not consumed by the Go's
    /// `calculate`.
    pub input_process_id: i32,
}

/// One `ATRatio` table row — input to [`AirToxics::build`].
///
/// The Go reads these nine columns from the `atratio` extract file; the SQL
/// `cache select` lists them in this order: `fuelTypeID, fuelFormulationID,
/// polProcessID, minModelYearID, maxModelYearID, ageID, monthID, atRatio,
/// modelYearID`.
///
/// The `ATRatio` lookup key the Go's `calculate` builds uses only
/// `fuel_formulation_id`, `month_id`, `model_year_id` and `pol_process_id`;
/// `fuel_type_id`, `min_model_year_id`, `max_model_year_id` and `age_id` were
/// the SQL extract's `WHERE`-clause inputs and are carried for column fidelity
/// but not consumed.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AtRatioRow {
    /// `fuelTypeID`. Not consumed by `calculate`.
    pub fuel_type_id: i32,
    /// `fuelFormulationID`.
    pub fuel_formulation_id: i32,
    /// `polProcessID` — the *output* toxic `polProcessID`; matched against a
    /// chained-to row's `outputPolProcessID`.
    pub pol_process_id: i32,
    /// `minModelYearID`. Not consumed by `calculate`.
    pub min_model_year_id: i32,
    /// `maxModelYearID`. Not consumed by `calculate`.
    pub max_model_year_id: i32,
    /// `ageID`. Not consumed by `calculate`.
    pub age_id: i32,
    /// `monthID`.
    pub month_id: i32,
    /// `atRatio` — the toxic-to-input ratio.
    pub at_ratio: f64,
    /// `modelYearID`.
    pub model_year_id: i32,
}

/// One `ATRatioGas2` table row — input to [`AirToxics::build`].
///
/// The Go reads the first four of the default-DB table's five columns from the
/// `atratiogas2` extract file: `polProcessID, sourceTypeID, fuelSubTypeID,
/// atRatio`. The table's fifth column, `ATRatioCV`, is not consumed.
///
/// `ATRatioGas2.ATRatio` is a default-DB `float` (32-bit) column; the value is
/// f32-precision once extracted and is carried here as `f64` exactly as the
/// Go's text-file parse does.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AtRatioGas2Row {
    /// `polProcessID` — the *output* toxic `polProcessID`.
    pub pol_process_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `fuelSubTypeID`.
    pub fuel_sub_type_id: i32,
    /// `atRatio` — the toxic-to-input ratio.
    pub at_ratio: f64,
}

/// One `ATRatioNonGas` table row — input to [`AirToxics::build`].
///
/// The Go reads these five columns from the `atrationongas` extract file; the
/// SQL `cache select` lists them in this order: `polProcessID, sourceTypeID,
/// fuelSubTypeID, modelYearID, ATRatio`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AtRatioNonGasRow {
    /// `polProcessID` — the *output* toxic `polProcessID`.
    pub pol_process_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `fuelSubTypeID`.
    pub fuel_sub_type_id: i32,
    /// `modelYearID`.
    pub model_year_id: i32,
    /// `atRatio` — the toxic-to-input ratio.
    pub at_ratio: f64,
}

/// The nine table extracts [`AirToxics::build`] indexes — the inputs the Go
/// `StartSetup` reads from its nine extract files.
///
/// Grouping the extracts in one struct (rather than nine `build` parameters)
/// keeps the constructor within Clippy's argument-count limit and lets a
/// caller — or a test — populate only the tables it needs via
/// `AirToxicsExtracts { at_ratio: …, ..Default::default() }`.
#[derive(Debug, Clone, Default)]
pub struct AirToxicsExtracts {
    /// `minorHAPRatio` rows (the `minorHAPRatioGo` extract).
    pub minor_hap_ratio: Vec<MinorHapRatioRow>,
    /// `pahGasRatio` rows.
    pub pah_gas_ratio: Vec<PahRatioRow>,
    /// `pahParticleRatio` rows.
    pub pah_particle_ratio: Vec<PahRatioRow>,
    /// `ATRatioGas1ChainedTo` rows.
    pub at_ratio_gas1_chained_to: Vec<ChainedToRow>,
    /// `ATRatioGas2ChainedTo` rows.
    pub at_ratio_gas2_chained_to: Vec<ChainedToRow>,
    /// `ATRatioNonGasChainedTo` rows.
    pub at_ratio_non_gas_chained_to: Vec<ChainedToRow>,
    /// `ATRatio` rows.
    pub at_ratio: Vec<AtRatioRow>,
    /// `ATRatioGas2` rows.
    pub at_ratio_gas2: Vec<AtRatioGas2Row>,
    /// `ATRatioNonGas` rows.
    pub at_ratio_non_gas: Vec<AtRatioNonGasRow>,
}

/// The six `ATC_Use*` module flags — the Go `mwo.NeedsModule` results.
///
/// The Java `AirToxicsCalculator` enables a subset of the SQL script's `Use*`
/// sections per runspec and passes them to the Go worker as `ATC_<section>`
/// external modules. A path runs only when its flag here is set *and* its
/// ratio table is non-empty (the Go `useX := len(table) > 0 &&
/// mwo.NeedsModule("ATC_UseX")`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ModuleFlags {
    /// `ATC_UseMinorHAPRatio`.
    pub minor_hap_ratio: bool,
    /// `ATC_UsePAHGasRatio`.
    pub pah_gas_ratio: bool,
    /// `ATC_UsePAHParticleRatio`.
    pub pah_particle_ratio: bool,
    /// `ATC_UseATRatioGas1`.
    pub at_ratio_gas1: bool,
    /// `ATC_UseATRatioGas2`.
    pub at_ratio_gas2: bool,
    /// `ATC_UseATRatioNonGas`.
    pub at_ratio_non_gas: bool,
}

/// One ratio-table detail — the Go `minorHAPRatioDetail` / `PAHGasRatioDetail`
/// / `PAHParticleRatioDetail` (the three Go types are structurally identical).
///
/// A `minorHAPRatio` / `pahGasRatio` / `pahParticleRatio` key maps to a list
/// of these: one detail per output pollutant the key produces.
#[derive(Debug, Clone, Copy, PartialEq)]
struct RatioDetail {
    /// `outputPollutantID` — the output toxic pollutant.
    output_pollutant_id: i32,
    /// `atRatio` — the multiplier applied to the input emission.
    at_ratio: f64,
}

/// One chained-to detail — the Go `chainedToDetail`.
///
/// An `ATRatio*ChainedTo` key (the input `polProcessID`) maps to a list of
/// these: one per toxic `(polProcessID, pollutantID, processID)` the input
/// produces.
#[derive(Debug, Clone, Copy, PartialEq)]
struct ChainedToDetail {
    /// `outputPolProcessID`.
    output_pol_process_id: i32,
    /// `outputPollutantID`.
    output_pollutant_id: i32,
    /// `outputProcessID`.
    output_process_id: i32,
}

/// Key of the `minorHAPRatio` lookup — the Go `minorHAPRatioKey`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct MinorHapRatioKey {
    /// `processID` — the input block's process.
    process_id: i32,
    /// `fuelSubTypeID` — the *emission's* fuel subtype.
    fuel_sub_type_id: i32,
    /// `modelYearID` — the input block's model year.
    model_year_id: i32,
}

/// Key of the `pahGasRatio` and `pahParticleRatio` lookups — the Go
/// `PAHGasRatioKey` / `PAHParticleRatioKey` (structurally identical).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct PahRatioKey {
    /// `processID` — the input block's process.
    process_id: i32,
    /// `fuelTypeID` — the *block's* fuel type.
    fuel_type_id: i32,
    /// `modelYearID` — the input block's model year.
    model_year_id: i32,
}

/// Key of the `ATRatio` lookup — the Go `ATRatioKey`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct AtRatioKey {
    /// `fuelFormulationID` — the *emission's* fuel formulation.
    fuel_formulation_id: i32,
    /// `monthID` — the input block's month.
    month_id: i32,
    /// `modelYearID` — the input block's model year.
    model_year_id: i32,
    /// `outputPolProcessID` — the chained-to row's output `polProcessID`.
    output_pol_process_id: i32,
}

/// Key of the `ATRatioGas2` lookup — the Go `ATRatioGas2Key`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct AtRatioGas2Key {
    /// `outputPolProcessID` — the chained-to row's output `polProcessID`.
    output_pol_process_id: i32,
    /// `sourceTypeID` — the input block's source type.
    source_type_id: i32,
    /// `fuelSubTypeID` — the *emission's* fuel subtype.
    fuel_sub_type_id: i32,
}

/// Key of the `ATRatioNonGas` lookup — the Go `ATRatioNonGasKey`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct AtRatioNonGasKey {
    /// `outputPolProcessID` — the chained-to row's output `polProcessID`.
    output_pol_process_id: i32,
    /// `sourceTypeID` — the input block's source type.
    source_type_id: i32,
    /// `fuelSubTypeID` — the *emission's* fuel subtype.
    fuel_sub_type_id: i32,
    /// `modelYearID` — the input block's model year.
    model_year_id: i32,
}

/// The onroad air-toxics lookup tables and the per-block calculation — the
/// in-memory state and `calculate` body of the Go `airtoxics` package.
#[derive(Debug, Clone, Default)]
pub struct AirToxics {
    /// `minorHAPRatio` — minor-HAP ratios, keyed by process / emission fuel
    /// subtype / model year.
    minor_hap_ratio: HashMap<MinorHapRatioKey, Vec<RatioDetail>>,
    /// `pahGasRatio` — gaseous-PAH ratios, keyed by process / block fuel type
    /// / model year.
    pah_gas_ratio: HashMap<PahRatioKey, Vec<RatioDetail>>,
    /// `pahParticleRatio` — particulate-PAH ratios, keyed by process / block
    /// fuel type / model year.
    pah_particle_ratio: HashMap<PahRatioKey, Vec<RatioDetail>>,
    /// `ATRatioGas1ChainedTo` — input `polProcessID` → toxic chained-to rows.
    at_ratio_gas1_chained_to: HashMap<i32, Vec<ChainedToDetail>>,
    /// `ATRatioGas2ChainedTo` — input `polProcessID` → toxic chained-to rows.
    at_ratio_gas2_chained_to: HashMap<i32, Vec<ChainedToDetail>>,
    /// `ATRatioNonGasChainedTo` — input `polProcessID` → toxic chained-to rows.
    at_ratio_non_gas_chained_to: HashMap<i32, Vec<ChainedToDetail>>,
    /// `ATRatio` — the `ATRatioGas1`-path ratios.
    at_ratio: HashMap<AtRatioKey, f64>,
    /// `ATRatioGas2` — the `ATRatioGas2`-path ratios.
    at_ratio_gas2: HashMap<AtRatioGas2Key, f64>,
    /// `ATRatioNonGas` — the `ATRatioNonGas`-path ratios.
    at_ratio_non_gas: HashMap<AtRatioNonGasKey, f64>,
}

impl AirToxics {
    /// Build the lookup tables from the nine table extracts — the in-memory
    /// `StartSetup` of the Go `airtoxics` package.
    ///
    /// The `minorHAPRatio` / `pahGasRatio` / `pahParticleRatio` tables map a
    /// key to a list of `RatioDetail`, and the three `ATRatio*ChainedTo`
    /// tables map an input `polProcessID` to a list of `ChainedToDetail`;
    /// rows sharing a key are appended in extract (file) order. The `ATRatio`
    /// / `ATRatioGas2` / `ATRatioNonGas` tables map a key to a single `f64`;
    /// on a duplicate key the last row wins, matching the Go map assignment.
    #[must_use]
    pub fn build(extracts: AirToxicsExtracts) -> AirToxics {
        let mut minor_hap_ratio: HashMap<MinorHapRatioKey, Vec<RatioDetail>> = HashMap::new();
        for row in extracts.minor_hap_ratio {
            minor_hap_ratio
                .entry(MinorHapRatioKey {
                    process_id: row.process_id,
                    fuel_sub_type_id: row.fuel_sub_type_id,
                    model_year_id: row.model_year_id,
                })
                .or_default()
                .push(RatioDetail {
                    output_pollutant_id: row.output_pollutant_id,
                    at_ratio: row.at_ratio,
                });
        }

        let pah_gas_ratio = index_pah_ratio(extracts.pah_gas_ratio);
        let pah_particle_ratio = index_pah_ratio(extracts.pah_particle_ratio);

        let at_ratio_gas1_chained_to = index_chained_to(extracts.at_ratio_gas1_chained_to);
        let at_ratio_gas2_chained_to = index_chained_to(extracts.at_ratio_gas2_chained_to);
        let at_ratio_non_gas_chained_to = index_chained_to(extracts.at_ratio_non_gas_chained_to);

        let mut at_ratio: HashMap<AtRatioKey, f64> = HashMap::new();
        for row in extracts.at_ratio {
            at_ratio.insert(
                AtRatioKey {
                    fuel_formulation_id: row.fuel_formulation_id,
                    month_id: row.month_id,
                    model_year_id: row.model_year_id,
                    output_pol_process_id: row.pol_process_id,
                },
                row.at_ratio,
            );
        }

        let mut at_ratio_gas2: HashMap<AtRatioGas2Key, f64> = HashMap::new();
        for row in extracts.at_ratio_gas2 {
            at_ratio_gas2.insert(
                AtRatioGas2Key {
                    output_pol_process_id: row.pol_process_id,
                    source_type_id: row.source_type_id,
                    fuel_sub_type_id: row.fuel_sub_type_id,
                },
                row.at_ratio,
            );
        }

        let mut at_ratio_non_gas: HashMap<AtRatioNonGasKey, f64> = HashMap::new();
        for row in extracts.at_ratio_non_gas {
            at_ratio_non_gas.insert(
                AtRatioNonGasKey {
                    output_pol_process_id: row.pol_process_id,
                    source_type_id: row.source_type_id,
                    fuel_sub_type_id: row.fuel_sub_type_id,
                    model_year_id: row.model_year_id,
                },
                row.at_ratio,
            );
        }

        AirToxics {
            minor_hap_ratio,
            pah_gas_ratio,
            pah_particle_ratio,
            at_ratio_gas1_chained_to,
            at_ratio_gas2_chained_to,
            at_ratio_non_gas_chained_to,
            at_ratio,
            at_ratio_gas2,
            at_ratio_non_gas,
        }
    }

    /// Derive the air-toxics output blocks from one input fuel block — the Go
    /// `calculate`'s per-`FuelBlock` body.
    ///
    /// The Go worker drains a channel of `MWOBlock`s, each holding several
    /// `FuelBlock`s; this method handles one fuel block and the data-plane
    /// integration iterates the rest.
    ///
    /// Each of the six paths runs only when its [`ModuleFlags`] flag is set
    /// *and* its ratio table is non-empty (the Go `useX := len(table) > 0 &&
    /// mwo.NeedsModule(…)`). The direct paths additionally require the input
    /// block's pollutant to be VOC (87) or Organic Carbon (111); the
    /// `ATRatio*` paths apply to any input whose `polProcessID` has a
    /// chained-to row.
    ///
    /// Output blocks are returned in ascending `polProcessID` order (see the
    /// module-level fidelity note on output order); a block whose
    /// `polProcessID` is produced by more than one path carries every path's
    /// emissions, in path order.
    #[must_use]
    pub fn air_toxics_block(&self, block: &FuelBlock, modules: ModuleFlags) -> Vec<ToxicFuelBlock> {
        // Output blocks keyed by polProcessID — a BTreeMap both reproduces the
        // Go's get-or-create-by-polProcessID grouping and keeps the result in
        // ascending polProcessID order.
        let mut blocks: BTreeMap<i32, ToxicFuelBlock> = BTreeMap::new();

        if modules.minor_hap_ratio && !self.minor_hap_ratio.is_empty() {
            self.apply_minor_hap_ratio(block, &mut blocks);
        }
        if modules.pah_gas_ratio && !self.pah_gas_ratio.is_empty() {
            self.apply_pah_gas_ratio(block, &mut blocks);
        }
        if modules.pah_particle_ratio && !self.pah_particle_ratio.is_empty() {
            self.apply_pah_particle_ratio(block, &mut blocks);
        }
        if modules.at_ratio_gas1 && !self.at_ratio.is_empty() {
            self.apply_at_ratio_gas1(block, &mut blocks);
        }
        if modules.at_ratio_gas2 && !self.at_ratio_gas2.is_empty() {
            self.apply_at_ratio_gas2(block, &mut blocks);
        }
        if modules.at_ratio_non_gas && !self.at_ratio_non_gas.is_empty() {
            self.apply_at_ratio_non_gas(block, &mut blocks);
        }

        blocks.into_values().collect()
    }

    /// The `minorHAPRatio` path — the Go `if fb.Key.PollutantID == 87 &&
    /// useMinorHAPRatio` branch.
    ///
    /// Applies only to a VOC (87) block. The lookup key is rebuilt per
    /// emission because it carries the *emission's* `fuelSubTypeID`; the output
    /// process is the input block's process.
    fn apply_minor_hap_ratio(&self, block: &FuelBlock, blocks: &mut BTreeMap<i32, ToxicFuelBlock>) {
        if block.key.pollutant_id != VOC_POLLUTANT_ID {
            return;
        }
        for emission in &block.emissions {
            let key = MinorHapRatioKey {
                process_id: block.key.process_id,
                fuel_sub_type_id: emission.fuel_sub_type_id,
                model_year_id: block.key.model_year_id,
            };
            if let Some(details) = self.minor_hap_ratio.get(&key) {
                for detail in details {
                    add_emission(
                        blocks,
                        &block.key,
                        detail.output_pollutant_id,
                        detail.at_ratio,
                        emission,
                    );
                }
            }
        }
    }

    /// The `pahGasRatio` path — the Go `if fb.Key.PollutantID == 87 &&
    /// usePAHGasRatio` branch.
    ///
    /// Applies only to a VOC (87) block. The lookup key carries the *block's*
    /// `fuelTypeID`, so it is built once; every detail scales every emission.
    fn apply_pah_gas_ratio(&self, block: &FuelBlock, blocks: &mut BTreeMap<i32, ToxicFuelBlock>) {
        if block.key.pollutant_id != VOC_POLLUTANT_ID {
            return;
        }
        let key = PahRatioKey {
            process_id: block.key.process_id,
            fuel_type_id: block.key.fuel_type_id,
            model_year_id: block.key.model_year_id,
        };
        if let Some(details) = self.pah_gas_ratio.get(&key) {
            for detail in details {
                for emission in &block.emissions {
                    add_emission(
                        blocks,
                        &block.key,
                        detail.output_pollutant_id,
                        detail.at_ratio,
                        emission,
                    );
                }
            }
        }
    }

    /// The `pahParticleRatio` path — the Go `if fb.Key.PollutantID == 111 &&
    /// usePAHParticleRatio` branch.
    ///
    /// Applies only to an Organic Carbon (111) block. Otherwise identical in
    /// shape to the `pahGasRatio` path.
    fn apply_pah_particle_ratio(
        &self,
        block: &FuelBlock,
        blocks: &mut BTreeMap<i32, ToxicFuelBlock>,
    ) {
        if block.key.pollutant_id != ORGANIC_CARBON_POLLUTANT_ID {
            return;
        }
        let key = PahRatioKey {
            process_id: block.key.process_id,
            fuel_type_id: block.key.fuel_type_id,
            model_year_id: block.key.model_year_id,
        };
        if let Some(details) = self.pah_particle_ratio.get(&key) {
            for detail in details {
                for emission in &block.emissions {
                    add_emission(
                        blocks,
                        &block.key,
                        detail.output_pollutant_id,
                        detail.at_ratio,
                        emission,
                    );
                }
            }
        }
    }

    /// The `ATRatioGas1` path — the Go `if useATRatioGas1` branch.
    ///
    /// For each chained-to row of the input block's `polProcessID`, each
    /// emission is scaled by the `ATRatio` keyed on the emission's fuel
    /// formulation, the block's month and model year, and the chained-to row's
    /// output `polProcessID`. An emission with no matching `ATRatio` row is
    /// skipped.
    fn apply_at_ratio_gas1(&self, block: &FuelBlock, blocks: &mut BTreeMap<i32, ToxicFuelBlock>) {
        let Some(chained) = self.at_ratio_gas1_chained_to.get(&block.key.pol_process_id) else {
            return;
        };
        for chained_to in chained {
            for emission in &block.emissions {
                let key = AtRatioKey {
                    fuel_formulation_id: emission.fuel_formulation_id,
                    month_id: block.key.month_id,
                    model_year_id: block.key.model_year_id,
                    output_pol_process_id: chained_to.output_pol_process_id,
                };
                if let Some(&ratio) = self.at_ratio.get(&key) {
                    add_chained_emission(blocks, chained_to, emission, ratio);
                }
            }
        }
    }

    /// The `ATRatioGas2` path — the Go `if useATRatioGas2` branch.
    ///
    /// For each chained-to row of the input block's `polProcessID`, each
    /// emission is scaled by the `ATRatioGas2` keyed on the chained-to row's
    /// output `polProcessID`, the block's source type and the emission's fuel
    /// subtype.
    fn apply_at_ratio_gas2(&self, block: &FuelBlock, blocks: &mut BTreeMap<i32, ToxicFuelBlock>) {
        let Some(chained) = self.at_ratio_gas2_chained_to.get(&block.key.pol_process_id) else {
            return;
        };
        for chained_to in chained {
            for emission in &block.emissions {
                let key = AtRatioGas2Key {
                    output_pol_process_id: chained_to.output_pol_process_id,
                    source_type_id: block.key.source_type_id,
                    fuel_sub_type_id: emission.fuel_sub_type_id,
                };
                if let Some(&ratio) = self.at_ratio_gas2.get(&key) {
                    add_chained_emission(blocks, chained_to, emission, ratio);
                }
            }
        }
    }

    /// The `ATRatioNonGas` path — the Go `if useATRatioNonGas` branch.
    ///
    /// For each chained-to row of the input block's `polProcessID`, each
    /// emission is scaled by the `ATRatioNonGas` keyed on the chained-to row's
    /// output `polProcessID`, the block's source type, the emission's fuel
    /// subtype and the block's model year.
    fn apply_at_ratio_non_gas(
        &self,
        block: &FuelBlock,
        blocks: &mut BTreeMap<i32, ToxicFuelBlock>,
    ) {
        let Some(chained) = self
            .at_ratio_non_gas_chained_to
            .get(&block.key.pol_process_id)
        else {
            return;
        };
        for chained_to in chained {
            for emission in &block.emissions {
                let key = AtRatioNonGasKey {
                    output_pol_process_id: chained_to.output_pol_process_id,
                    source_type_id: block.key.source_type_id,
                    fuel_sub_type_id: emission.fuel_sub_type_id,
                    model_year_id: block.key.model_year_id,
                };
                if let Some(&ratio) = self.at_ratio_non_gas.get(&key) {
                    add_chained_emission(blocks, chained_to, emission, ratio);
                }
            }
        }
    }
}

/// Index a `pahGasRatio` / `pahParticleRatio` extract — the two tables share
/// the [`PahRatioRow`] shape and the Go reads them with the same lambda.
///
/// Rows sharing a key are kept in extract (file) order.
fn index_pah_ratio(rows: Vec<PahRatioRow>) -> HashMap<PahRatioKey, Vec<RatioDetail>> {
    let mut map: HashMap<PahRatioKey, Vec<RatioDetail>> = HashMap::new();
    for row in rows {
        map.entry(PahRatioKey {
            process_id: row.process_id,
            fuel_type_id: row.fuel_type_id,
            model_year_id: row.model_year_id,
        })
        .or_default()
        .push(RatioDetail {
            output_pollutant_id: row.output_pollutant_id,
            at_ratio: row.at_ratio,
        });
    }
    map
}

/// Index an `ATRatio*ChainedTo` extract — the Go `readChainedToFile`.
///
/// The table is keyed by the source emission's `inputPolProcessID`; rows
/// sharing a key are kept in extract (file) order.
fn index_chained_to(rows: Vec<ChainedToRow>) -> HashMap<i32, Vec<ChainedToDetail>> {
    let mut map: HashMap<i32, Vec<ChainedToDetail>> = HashMap::new();
    for row in rows {
        map.entry(row.input_pol_process_id)
            .or_default()
            .push(ChainedToDetail {
                output_pol_process_id: row.output_pol_process_id,
                output_pollutant_id: row.output_pollutant_id,
                output_process_id: row.output_process_id,
            });
    }
    map
}

/// Add one scaled emission to a direct-path (`minorHAPRatio` / `pahGasRatio` /
/// `pahParticleRatio`) output block — the Go `addEmission`.
///
/// The Go `addEmission` takes an `outputProcessID` parameter but every caller
/// passes `fb.Key.ProcessID`, and the new fuel block copies the input block's
/// key (overriding only the pollutant), so the output process always equals
/// the input block's process. This port computes `outputPolProcessID =
/// outputPollutantID * 100 + block_key.process_id` directly. A block created
/// by an earlier path keeps its key — the Go's `if nfb == nil` guard.
fn add_emission(
    blocks: &mut BTreeMap<i32, ToxicFuelBlock>,
    block_key: &FuelBlockKey,
    output_pollutant_id: i32,
    ratio: f64,
    emission: &Emission,
) {
    let output_pol_process_id = output_pollutant_id * 100 + block_key.process_id;
    blocks
        .entry(output_pol_process_id)
        .or_insert_with(|| ToxicFuelBlock {
            pollutant_id: output_pollutant_id,
            process_id: block_key.process_id,
            pol_process_id: output_pol_process_id,
            emissions: Vec::new(),
        })
        .emissions
        .push(emission.scaled(ratio));
}

/// Add one scaled emission to an `ATRatio*`-path output block.
///
/// The chained-to row supplies the output `(polProcessID, pollutantID,
/// processID)`. A block created by an earlier path or chained-to row keeps its
/// key — the Go's `if nfb == nil` guard.
fn add_chained_emission(
    blocks: &mut BTreeMap<i32, ToxicFuelBlock>,
    chained_to: &ChainedToDetail,
    emission: &Emission,
    ratio: f64,
) {
    blocks
        .entry(chained_to.output_pol_process_id)
        .or_insert_with(|| ToxicFuelBlock {
            pollutant_id: chained_to.output_pollutant_id,
            process_id: chained_to.output_process_id,
            pol_process_id: chained_to.output_pol_process_id,
            emissions: Vec::new(),
        })
        .emissions
        .push(emission.scaled(ratio));
}

// ===========================================================================
// TableRow implementations — typed DataFrame ↔ row round-trips.
// ===========================================================================

fn row_err(
    table: &'static str,
    row: usize,
    column: &'static str,
    msg: String,
) -> moves_framework::Error {
    moves_framework::Error::RowExtraction {
        table: table.into(),
        row,
        column: column.into(),
        message: msg,
    }
}

impl TableRow for MinorHapRatioRow {
    fn table_name() -> &'static str {
        "minorHAPRatio"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("processID".into(), DataType::Int32),
            ("outputPollutantID".into(), DataType::Int32),
            ("fuelSubTypeID".into(), DataType::Int32),
            ("modelYearID".into(), DataType::Int32),
            ("atRatio".into(), DataType::Float64),
        ])
    }

    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "processID".into(),
                    rows.iter().map(|r| r.process_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "outputPollutantID".into(),
                    rows.iter()
                        .map(|r| r.output_pollutant_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "fuelSubTypeID".into(),
                    rows.iter()
                        .map(|r| r.fuel_sub_type_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "modelYearID".into(),
                    rows.iter().map(|r| r.model_year_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "atRatio".into(),
                    rows.iter().map(|r| r.at_ratio).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "minorHAPRatio";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .f64()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let process = get_i32("processID")?;
        let output_pollutant = get_i32("outputPollutantID")?;
        let fuel_sub = get_i32("fuelSubTypeID")?;
        let model_year = get_i32("modelYearID")?;
        let ratio = get_f64("atRatio")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(MinorHapRatioRow {
                    process_id: process.get(i).ok_or_else(|| null("processID"))?,
                    output_pollutant_id: output_pollutant
                        .get(i)
                        .ok_or_else(|| null("outputPollutantID"))?,
                    fuel_sub_type_id: fuel_sub.get(i).ok_or_else(|| null("fuelSubTypeID"))?,
                    model_year_id: model_year.get(i).ok_or_else(|| null("modelYearID"))?,
                    at_ratio: ratio.get(i).ok_or_else(|| null("atRatio"))?,
                })
            })
            .collect()
    }
}

impl TableRow for PahRatioRow {
    fn table_name() -> &'static str {
        "pahGasRatio"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("processID".into(), DataType::Int32),
            ("outputPollutantID".into(), DataType::Int32),
            ("fuelTypeID".into(), DataType::Int32),
            ("modelYearID".into(), DataType::Int32),
            ("atRatio".into(), DataType::Float64),
        ])
    }

    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "processID".into(),
                    rows.iter().map(|r| r.process_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "outputPollutantID".into(),
                    rows.iter()
                        .map(|r| r.output_pollutant_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "fuelTypeID".into(),
                    rows.iter().map(|r| r.fuel_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "modelYearID".into(),
                    rows.iter().map(|r| r.model_year_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "atRatio".into(),
                    rows.iter().map(|r| r.at_ratio).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "pahGasRatio";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .f64()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let process = get_i32("processID")?;
        let output_pollutant = get_i32("outputPollutantID")?;
        let fuel_type = get_i32("fuelTypeID")?;
        let model_year = get_i32("modelYearID")?;
        let ratio = get_f64("atRatio")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(PahRatioRow {
                    process_id: process.get(i).ok_or_else(|| null("processID"))?,
                    output_pollutant_id: output_pollutant
                        .get(i)
                        .ok_or_else(|| null("outputPollutantID"))?,
                    fuel_type_id: fuel_type.get(i).ok_or_else(|| null("fuelTypeID"))?,
                    model_year_id: model_year.get(i).ok_or_else(|| null("modelYearID"))?,
                    at_ratio: ratio.get(i).ok_or_else(|| null("atRatio"))?,
                })
            })
            .collect()
    }
}

impl TableRow for ChainedToRow {
    fn table_name() -> &'static str {
        "RunSpecChainedTo"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("outputPolProcessID".into(), DataType::Int32),
            ("outputPollutantID".into(), DataType::Int32),
            ("outputProcessID".into(), DataType::Int32),
            ("inputPolProcessID".into(), DataType::Int32),
            ("inputPollutantID".into(), DataType::Int32),
            ("inputProcessID".into(), DataType::Int32),
        ])
    }

    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "outputPolProcessID".into(),
                    rows.iter()
                        .map(|r| r.output_pol_process_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "outputPollutantID".into(),
                    rows.iter()
                        .map(|r| r.output_pollutant_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "outputProcessID".into(),
                    rows.iter()
                        .map(|r| r.output_process_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "inputPolProcessID".into(),
                    rows.iter()
                        .map(|r| r.input_pol_process_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "inputPollutantID".into(),
                    rows.iter()
                        .map(|r| r.input_pollutant_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "inputProcessID".into(),
                    rows.iter()
                        .map(|r| r.input_process_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "RunSpecChainedTo";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let out_pp = get_i32("outputPolProcessID")?;
        let out_pol = get_i32("outputPollutantID")?;
        let out_proc = get_i32("outputProcessID")?;
        let in_pp = get_i32("inputPolProcessID")?;
        let in_pol = get_i32("inputPollutantID")?;
        let in_proc = get_i32("inputProcessID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(ChainedToRow {
                    output_pol_process_id: out_pp
                        .get(i)
                        .ok_or_else(|| null("outputPolProcessID"))?,
                    output_pollutant_id: out_pol.get(i).ok_or_else(|| null("outputPollutantID"))?,
                    output_process_id: out_proc.get(i).ok_or_else(|| null("outputProcessID"))?,
                    input_pol_process_id: in_pp.get(i).ok_or_else(|| null("inputPolProcessID"))?,
                    input_pollutant_id: in_pol.get(i).ok_or_else(|| null("inputPollutantID"))?,
                    input_process_id: in_proc.get(i).ok_or_else(|| null("inputProcessID"))?,
                })
            })
            .collect()
    }
}

impl TableRow for AtRatioRow {
    fn table_name() -> &'static str {
        "ATRatio"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("fuelTypeID".into(), DataType::Int32),
            ("fuelFormulationID".into(), DataType::Int32),
            ("polProcessID".into(), DataType::Int32),
            ("minModelYearID".into(), DataType::Int32),
            ("maxModelYearID".into(), DataType::Int32),
            ("ageID".into(), DataType::Int32),
            ("monthID".into(), DataType::Int32),
            ("atRatio".into(), DataType::Float64),
            ("modelYearID".into(), DataType::Int32),
        ])
    }

    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "fuelTypeID".into(),
                    rows.iter().map(|r| r.fuel_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "fuelFormulationID".into(),
                    rows.iter()
                        .map(|r| r.fuel_formulation_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "polProcessID".into(),
                    rows.iter().map(|r| r.pol_process_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "minModelYearID".into(),
                    rows.iter()
                        .map(|r| r.min_model_year_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "maxModelYearID".into(),
                    rows.iter()
                        .map(|r| r.max_model_year_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "ageID".into(),
                    rows.iter().map(|r| r.age_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "monthID".into(),
                    rows.iter().map(|r| r.month_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "atRatio".into(),
                    rows.iter().map(|r| r.at_ratio).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "modelYearID".into(),
                    rows.iter().map(|r| r.model_year_id).collect::<Vec<i32>>(),
                )
                .into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "ATRatio";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .f64()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let fuel_type = get_i32("fuelTypeID")?;
        let fuel_form = get_i32("fuelFormulationID")?;
        let pol_proc = get_i32("polProcessID")?;
        let min_my = get_i32("minModelYearID")?;
        let max_my = get_i32("maxModelYearID")?;
        let age = get_i32("ageID")?;
        let month = get_i32("monthID")?;
        let ratio = get_f64("atRatio")?;
        let model_year = get_i32("modelYearID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(AtRatioRow {
                    fuel_type_id: fuel_type.get(i).ok_or_else(|| null("fuelTypeID"))?,
                    fuel_formulation_id: fuel_form
                        .get(i)
                        .ok_or_else(|| null("fuelFormulationID"))?,
                    pol_process_id: pol_proc.get(i).ok_or_else(|| null("polProcessID"))?,
                    min_model_year_id: min_my.get(i).ok_or_else(|| null("minModelYearID"))?,
                    max_model_year_id: max_my.get(i).ok_or_else(|| null("maxModelYearID"))?,
                    age_id: age.get(i).ok_or_else(|| null("ageID"))?,
                    month_id: month.get(i).ok_or_else(|| null("monthID"))?,
                    at_ratio: ratio.get(i).ok_or_else(|| null("atRatio"))?,
                    model_year_id: model_year.get(i).ok_or_else(|| null("modelYearID"))?,
                })
            })
            .collect()
    }
}

impl TableRow for AtRatioGas2Row {
    fn table_name() -> &'static str {
        "ATRatioGas2"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("polProcessID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("fuelSubTypeID".into(), DataType::Int32),
            ("atRatio".into(), DataType::Float64),
        ])
    }

    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "polProcessID".into(),
                    rows.iter().map(|r| r.pol_process_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "fuelSubTypeID".into(),
                    rows.iter()
                        .map(|r| r.fuel_sub_type_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "atRatio".into(),
                    rows.iter().map(|r| r.at_ratio).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "ATRatioGas2";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .f64()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let pol_proc = get_i32("polProcessID")?;
        let src_type = get_i32("sourceTypeID")?;
        let fuel_sub = get_i32("fuelSubTypeID")?;
        let ratio = get_f64("atRatio")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(AtRatioGas2Row {
                    pol_process_id: pol_proc.get(i).ok_or_else(|| null("polProcessID"))?,
                    source_type_id: src_type.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    fuel_sub_type_id: fuel_sub.get(i).ok_or_else(|| null("fuelSubTypeID"))?,
                    at_ratio: ratio.get(i).ok_or_else(|| null("atRatio"))?,
                })
            })
            .collect()
    }
}

impl TableRow for AtRatioNonGasRow {
    fn table_name() -> &'static str {
        "ATRatioNonGas"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("polProcessID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("fuelSubTypeID".into(), DataType::Int32),
            ("modelYearID".into(), DataType::Int32),
            ("ATRatio".into(), DataType::Float64),
        ])
    }

    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "polProcessID".into(),
                    rows.iter().map(|r| r.pol_process_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "fuelSubTypeID".into(),
                    rows.iter()
                        .map(|r| r.fuel_sub_type_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "modelYearID".into(),
                    rows.iter().map(|r| r.model_year_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "ATRatio".into(),
                    rows.iter().map(|r| r.at_ratio).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "ATRatioNonGas";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .f64()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let pol_proc = get_i32("polProcessID")?;
        let src_type = get_i32("sourceTypeID")?;
        let fuel_sub = get_i32("fuelSubTypeID")?;
        let model_year = get_i32("modelYearID")?;
        let ratio = get_f64("ATRatio")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(AtRatioNonGasRow {
                    pol_process_id: pol_proc.get(i).ok_or_else(|| null("polProcessID"))?,
                    source_type_id: src_type.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    fuel_sub_type_id: fuel_sub.get(i).ok_or_else(|| null("fuelSubTypeID"))?,
                    model_year_id: model_year.get(i).ok_or_else(|| null("modelYearID"))?,
                    at_ratio: ratio.get(i).ok_or_else(|| null("ATRatio"))?,
                })
            })
            .collect()
    }
}

/// One `MOVESWorkerOutput` row as read/written by `AirToxicsCalculator`.
struct AirToxicsMwoRow {
    year_id: i32,
    month_id: i32,
    day_id: i32,
    hour_id: i32,
    state_id: i32,
    county_id: i32,
    zone_id: i32,
    link_id: i32,
    pollutant_id: i32,
    process_id: i32,
    source_type_id: i32,
    reg_class_id: i32,
    fuel_type_id: i32,
    model_year_id: i32,
    road_type_id: i32,
    fuel_sub_type_id: i32,
    fuel_formulation_id: i32,
    emission_quant: f64,
    emission_rate: f64,
}

impl TableRow for AirToxicsMwoRow {
    fn table_name() -> &'static str {
        "MOVESWorkerOutput"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("yearID".into(), DataType::Int32),
            ("monthID".into(), DataType::Int32),
            ("dayID".into(), DataType::Int32),
            ("hourID".into(), DataType::Int32),
            ("stateID".into(), DataType::Int32),
            ("countyID".into(), DataType::Int32),
            ("zoneID".into(), DataType::Int32),
            ("linkID".into(), DataType::Int32),
            ("pollutantID".into(), DataType::Int32),
            ("processID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("regClassID".into(), DataType::Int32),
            ("fuelTypeID".into(), DataType::Int32),
            ("modelYearID".into(), DataType::Int32),
            ("roadTypeID".into(), DataType::Int32),
            ("fuelSubTypeID".into(), DataType::Int32),
            ("fuelFormulationID".into(), DataType::Int32),
            ("emissionQuant".into(), DataType::Float64),
            ("emissionRate".into(), DataType::Float64),
        ])
    }

    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "yearID".into(),
                    rows.iter().map(|r| r.year_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "monthID".into(),
                    rows.iter().map(|r| r.month_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "dayID".into(),
                    rows.iter().map(|r| r.day_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "hourID".into(),
                    rows.iter().map(|r| r.hour_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "stateID".into(),
                    rows.iter().map(|r| r.state_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "countyID".into(),
                    rows.iter().map(|r| r.county_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "zoneID".into(),
                    rows.iter().map(|r| r.zone_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "linkID".into(),
                    rows.iter().map(|r| r.link_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "pollutantID".into(),
                    rows.iter().map(|r| r.pollutant_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "processID".into(),
                    rows.iter().map(|r| r.process_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "regClassID".into(),
                    rows.iter().map(|r| r.reg_class_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "fuelTypeID".into(),
                    rows.iter().map(|r| r.fuel_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "modelYearID".into(),
                    rows.iter().map(|r| r.model_year_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "roadTypeID".into(),
                    rows.iter().map(|r| r.road_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "fuelSubTypeID".into(),
                    rows.iter()
                        .map(|r| r.fuel_sub_type_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "fuelFormulationID".into(),
                    rows.iter()
                        .map(|r| r.fuel_formulation_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "emissionQuant".into(),
                    rows.iter().map(|r| r.emission_quant).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "emissionRate".into(),
                    rows.iter().map(|r| r.emission_rate).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "MOVESWorkerOutput";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .f64()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let year = get_i32("yearID")?;
        let month = get_i32("monthID")?;
        let day = get_i32("dayID")?;
        let hour = get_i32("hourID")?;
        let state = get_i32("stateID")?;
        let county = get_i32("countyID")?;
        let zone = get_i32("zoneID")?;
        let link = get_i32("linkID")?;
        let pollutant = get_i32("pollutantID")?;
        let process = get_i32("processID")?;
        let src_type = get_i32("sourceTypeID")?;
        let reg_class = get_i32("regClassID")?;
        let fuel_type = get_i32("fuelTypeID")?;
        let model_year = get_i32("modelYearID")?;
        let road_type = get_i32("roadTypeID")?;
        let fuel_sub_type = get_i32("fuelSubTypeID")?;
        let fuel_formulation = get_i32("fuelFormulationID")?;
        let emission_quant = get_f64("emissionQuant")?;
        let emission_rate = get_f64("emissionRate")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(AirToxicsMwoRow {
                    year_id: year.get(i).ok_or_else(|| null("yearID"))?,
                    month_id: month.get(i).ok_or_else(|| null("monthID"))?,
                    day_id: day.get(i).ok_or_else(|| null("dayID"))?,
                    hour_id: hour.get(i).ok_or_else(|| null("hourID"))?,
                    state_id: state.get(i).ok_or_else(|| null("stateID"))?,
                    county_id: county.get(i).ok_or_else(|| null("countyID"))?,
                    zone_id: zone.get(i).ok_or_else(|| null("zoneID"))?,
                    link_id: link.get(i).ok_or_else(|| null("linkID"))?,
                    pollutant_id: pollutant.get(i).ok_or_else(|| null("pollutantID"))?,
                    process_id: process.get(i).ok_or_else(|| null("processID"))?,
                    source_type_id: src_type.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    reg_class_id: reg_class.get(i).ok_or_else(|| null("regClassID"))?,
                    fuel_type_id: fuel_type.get(i).ok_or_else(|| null("fuelTypeID"))?,
                    model_year_id: model_year.get(i).ok_or_else(|| null("modelYearID"))?,
                    road_type_id: road_type.get(i).ok_or_else(|| null("roadTypeID"))?,
                    fuel_sub_type_id: fuel_sub_type.get(i).ok_or_else(|| null("fuelSubTypeID"))?,
                    fuel_formulation_id: fuel_formulation
                        .get(i)
                        .ok_or_else(|| null("fuelFormulationID"))?,
                    emission_quant: emission_quant.get(i).ok_or_else(|| null("emissionQuant"))?,
                    emission_rate: emission_rate.get(i).ok_or_else(|| null("emissionRate"))?,
                })
            })
            .collect()
    }
}

/// `(pollutant, process)` registration helper — keeps [`REGISTRATION_GROUPS`]
/// readable.
const fn reg(pollutant: u16, process: u16) -> PollutantProcessAssociation {
    PollutantProcessAssociation {
        pollutant_id: PollutantId(pollutant),
        process_id: ProcessId(process),
    }
}

/// The 46 toxic pollutants `AirToxicsCalculator` registers for the running
/// (1) and start (2) exhaust processes — the organic toxics 20–46 plus the
/// particulate (68–84) and gaseous (168–185) PAH species.
const EXHAUST_TOXICS: &[u16] = &[
    20, 21, 22, 23, 24, 25, 26, 27, 40, 41, 42, 43, 44, 45, 46, 68, 69, 70, 71, 72, 73, 74, 75, 76,
    77, 78, 81, 82, 83, 84, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 181, 182, 183,
    184, 185,
];

/// The 9 toxic pollutants `AirToxicsCalculator` registers for each evaporative
/// and refueling process (11, 12, 13, 18, 19) — the fuel-borne organic toxics
/// and gaseous naphthalene.
const EVAP_REFUELING_TOXICS: &[u16] = &[20, 21, 22, 40, 41, 42, 45, 46, 185];

/// The 29 toxic pollutants `AirToxicsCalculator` registers for the
/// extended-idle (90) and auxiliary-power (91) exhaust processes — the
/// [`EXHAUST_TOXICS`] set without ethanol (21), MTBE (22) and the particulate
/// PAH species (68–84).
const IDLE_EXHAUST_TOXICS: &[u16] = &[
    20, 23, 24, 25, 26, 27, 40, 41, 42, 43, 44, 45, 46, 168, 169, 170, 171, 172, 173, 174, 175,
    176, 177, 178, 181, 182, 183, 184, 185,
];

/// The `(pollutant, process)` pairs `AirToxicsCalculator` registers, grouped
/// by process.
///
/// The canonical source for these pairs is the `Registration` directives for
/// `AirToxicsCalculator` in `CalculatorInfo.txt` at the MOVES source pin — not
/// the Java constructor's `register(...)` loop, which can over- or under-count
/// against the runtime registry. The flattened pair count is
/// [`REGISTRATION_COUNT`] and reconciles with both the 195 `Registration`
/// rows and `registrations_count: 195` for `AirToxicsCalculator` in
/// `characterization/calculator-chains/calculator-dag.json`.
///
/// The metallic toxics and dioxins/furans the migration plan mentions for
/// Task 50 are produced by the same generic ratio engine but are *not* in the
/// onroad `AirToxicsCalculator`'s own registration set — the registered
/// pollutants are the organic toxics 20–46 and the PAH species 68–84 / 168–185.
const REGISTRATION_GROUPS: &[(u16, &[u16])] = &[
    // Running Exhaust (1) and Start Exhaust (2).
    (1, EXHAUST_TOXICS),
    (2, EXHAUST_TOXICS),
    // Evap Permeation (11), Evap Fuel Vapor Venting (12), Evap Fuel Leaks (13),
    // Refueling Displacement Vapor Loss (18), Refueling Spillage Loss (19).
    (11, EVAP_REFUELING_TOXICS),
    (12, EVAP_REFUELING_TOXICS),
    (13, EVAP_REFUELING_TOXICS),
    (18, EVAP_REFUELING_TOXICS),
    (19, EVAP_REFUELING_TOXICS),
    // Extended Idle Exhaust (90) and Auxiliary Power Exhaust (91).
    (90, IDLE_EXHAUST_TOXICS),
    (91, IDLE_EXHAUST_TOXICS),
];

/// The number of `(pollutant, process)` pairs across [`REGISTRATION_GROUPS`]
/// — the length of [`REGISTRATIONS`]. Expected to be 195.
const REGISTRATION_COUNT: usize = {
    let mut count = 0;
    let mut i = 0;
    while i < REGISTRATION_GROUPS.len() {
        count += REGISTRATION_GROUPS[i].1.len();
        i += 1;
    }
    count
};

/// The flattened `(pollutant, process)` pairs `AirToxicsCalculator` registers
/// — [`REGISTRATION_GROUPS`] expanded so [`Calculator::registrations`] can hand
/// back one contiguous slice.
static REGISTRATIONS: [PollutantProcessAssociation; REGISTRATION_COUNT] = {
    let mut regs = [reg(0, 0); REGISTRATION_COUNT];
    let mut idx = 0;
    let mut group = 0;
    while group < REGISTRATION_GROUPS.len() {
        let (process, pollutants) = REGISTRATION_GROUPS[group];
        let mut p = 0;
        while p < pollutants.len() {
            regs[idx] = reg(pollutants[p], process);
            idx += 1;
            p += 1;
        }
        group += 1;
    }
    regs
};

/// `AirToxicsCalculator` declares no master-loop subscription of its own;
/// see the [`Calculator::subscriptions`] impl.
static NO_SUBSCRIPTIONS: &[CalculatorSubscription] = &[];

/// Upstream modules — `AirToxicsCalculator` chains to `HCSpeciationCalculator`
/// (which produces the speciated VOC it scales) and `SulfatePMCalculator`
/// (which produces the particulate it chains from). Matches the two `Chain`
/// directives for `AirToxicsCalculator` in `CalculatorInfo.txt` at the MOVES
/// source pin, and `depends_on` in `calculator-dag.json`.
static UPSTREAM: &[&str] = &["HCSpeciationCalculator", "SulfatePMCalculator"];

/// Default-DB tables the calculator's SQL extracts. The three
/// `ATRatio*ChainedTo` extracts all come from `RunSpecChainedTo`.
static INPUT_TABLES: &[&str] = &[
    "ATRatio",
    "ATRatioGas2",
    "ATRatioNonGas",
    "RunSpecChainedTo",
    "minorHAPRatio",
    "pahGasRatio",
    "pahParticleRatio",
];

/// `AirToxicsCalculator` as a chain-DAG [`Calculator`].
///
/// The numerically faithful work lives on [`AirToxics`]; this zero-sized type
/// carries the calculator's chain metadata — [`name`](Calculator::name),
/// [`registrations`](Calculator::registrations),
/// [`upstream`](Calculator::upstream) — so the registry can wire it into the
/// calculator chain.
#[derive(Debug, Clone, Copy, Default)]
pub struct AirToxicsCalculator;

impl AirToxicsCalculator {
    /// Chain-DAG name — matches the Java class / Go package and the
    /// `calculator-dag.json` entry.
    pub const NAME: &'static str = "AirToxicsCalculator";

    /// Construct the calculator.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl Calculator for AirToxicsCalculator {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    /// `AirToxicsCalculator` carries no master-loop subscription of its own:
    /// `calculator-dag.json` records `subscribes_directly: false` and the Java
    /// `subscribeToMe` calls `chainCalculator` instead of `targetLoop.subscribe`.
    /// It is a chained calculator — it runs when the calculators it chains to
    /// (its [`upstream`](Calculator::upstream) modules) run, deriving the
    /// toxics from their output.
    fn subscriptions(&self) -> &[CalculatorSubscription] {
        NO_SUBSCRIPTIONS
    }

    /// The 195 `(pollutant, process)` pairs from the `Registration` directives
    /// for `AirToxicsCalculator` in `CalculatorInfo.txt` — see
    /// `REGISTRATION_GROUPS`. The onroad calculator registers the organic
    /// toxics (20–46) and the gaseous / particulate PAH species (68–84,
    /// 168–185); the metals and dioxins/furans are not in its own set.
    fn registrations(&self) -> &[PollutantProcessAssociation] {
        &REGISTRATIONS
    }

    fn upstream(&self) -> &[&'static str] {
        UPSTREAM
    }

    fn input_tables(&self) -> &[&'static str] {
        INPUT_TABLES
    }

    /// Run the calculator for the current master-loop iteration.
    ///
    /// **Data plane pending.** [`CalculatorContext`] exposes only
    /// placeholder `ExecutionTables` / `ScratchNamespace` today, so this body
    /// cannot read the nine air-toxics tables nor the upstream emission fuel
    /// blocks, nor write the toxic blocks back. The faithful algorithm is
    /// ported and tested on [`AirToxics`]; once the `DataFrameStore` lands,
    /// `execute` builds an [`AirToxics`] from `ctx.tables()`, reads the input
    /// [`FuelBlock`]s, applies
    /// [`air_toxics_block`](AirToxics::air_toxics_block), and stores the
    /// resulting [`ToxicFuelBlock`]s.
    fn execute(&self, ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
        let tables = ctx.tables();
        let chained_to: Vec<ChainedToRow> = tables.iter_typed("RunSpecChainedTo")?;
        let extracts = AirToxicsExtracts {
            minor_hap_ratio: tables.iter_typed::<MinorHapRatioRow>("minorHAPRatio")?,
            pah_gas_ratio: tables.iter_typed::<PahRatioRow>("pahGasRatio")?,
            pah_particle_ratio: tables.iter_typed::<PahRatioRow>("pahParticleRatio")?,
            at_ratio_gas1_chained_to: chained_to.clone(),
            at_ratio_gas2_chained_to: chained_to.clone(),
            at_ratio_non_gas_chained_to: chained_to,
            at_ratio: tables.iter_typed::<AtRatioRow>("ATRatio")?,
            at_ratio_gas2: tables.iter_typed::<AtRatioGas2Row>("ATRatioGas2")?,
            at_ratio_non_gas: tables.iter_typed::<AtRatioNonGasRow>("ATRatioNonGas")?,
        };
        let air_toxics = AirToxics::build(extracts);
        let modules = ModuleFlags {
            minor_hap_ratio: true,
            pah_gas_ratio: true,
            pah_particle_ratio: true,
            at_ratio_gas1: true,
            at_ratio_gas2: true,
            at_ratio_non_gas: true,
        };
        let input_rows: Vec<AirToxicsMwoRow> = tables.iter_typed("MOVESWorkerOutput")?;
        let mut output_rows: Vec<AirToxicsMwoRow> = Vec::new();
        for row in &input_rows {
            let block = FuelBlock {
                key: FuelBlockKey {
                    pollutant_id: row.pollutant_id,
                    process_id: row.process_id,
                    pol_process_id: row.pollutant_id * 100 + row.process_id,
                    model_year_id: row.model_year_id,
                    fuel_type_id: row.fuel_type_id,
                    month_id: row.month_id,
                    source_type_id: row.source_type_id,
                },
                emissions: vec![Emission {
                    fuel_sub_type_id: row.fuel_sub_type_id,
                    fuel_formulation_id: row.fuel_formulation_id,
                    emission_quant: row.emission_quant,
                    emission_rate: row.emission_rate,
                }],
            };
            for tblock in air_toxics.air_toxics_block(&block, modules) {
                for emission in &tblock.emissions {
                    output_rows.push(AirToxicsMwoRow {
                        pollutant_id: tblock.pollutant_id,
                        process_id: tblock.process_id,
                        year_id: row.year_id,
                        month_id: row.month_id,
                        day_id: row.day_id,
                        hour_id: row.hour_id,
                        state_id: row.state_id,
                        county_id: row.county_id,
                        zone_id: row.zone_id,
                        link_id: row.link_id,
                        source_type_id: row.source_type_id,
                        reg_class_id: row.reg_class_id,
                        fuel_type_id: row.fuel_type_id,
                        model_year_id: row.model_year_id,
                        road_type_id: row.road_type_id,
                        fuel_sub_type_id: emission.fuel_sub_type_id,
                        fuel_formulation_id: emission.fuel_formulation_id,
                        emission_quant: emission.emission_quant,
                        emission_rate: emission.emission_rate,
                    });
                }
            }
        }
        crate::wiring::emit_rows(output_rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A `minorHAPRatio` row helper.
    fn minor_hap_row(
        process_id: i32,
        output_pollutant_id: i32,
        fuel_sub_type_id: i32,
        model_year_id: i32,
        at_ratio: f64,
    ) -> MinorHapRatioRow {
        MinorHapRatioRow {
            process_id,
            output_pollutant_id,
            fuel_sub_type_id,
            model_year_id,
            at_ratio,
        }
    }

    /// A `pahGasRatio` / `pahParticleRatio` row helper.
    fn pah_row(
        process_id: i32,
        output_pollutant_id: i32,
        fuel_type_id: i32,
        model_year_id: i32,
        at_ratio: f64,
    ) -> PahRatioRow {
        PahRatioRow {
            process_id,
            output_pollutant_id,
            fuel_type_id,
            model_year_id,
            at_ratio,
        }
    }

    /// A `RunSpecChainedTo` row helper. `output_pol_process_id` is set to
    /// `output_pollutant_id * 100 + output_process_id`, as the real table is.
    fn chained_row(
        output_pollutant_id: i32,
        output_process_id: i32,
        input_pol_process_id: i32,
    ) -> ChainedToRow {
        ChainedToRow {
            output_pol_process_id: output_pollutant_id * 100 + output_process_id,
            output_pollutant_id,
            output_process_id,
            input_pol_process_id,
            input_pollutant_id: input_pol_process_id / 100,
            input_process_id: input_pol_process_id % 100,
        }
    }

    /// An `ATRatio` row helper. Only `fuel_formulation_id`, `month_id`,
    /// `model_year_id`, `pol_process_id` and `at_ratio` are consumed; the
    /// extract's other columns are filled with placeholders.
    fn at_ratio_row(
        fuel_formulation_id: i32,
        month_id: i32,
        model_year_id: i32,
        pol_process_id: i32,
        at_ratio: f64,
    ) -> AtRatioRow {
        AtRatioRow {
            fuel_type_id: 1,
            fuel_formulation_id,
            pol_process_id,
            min_model_year_id: 0,
            max_model_year_id: 0,
            age_id: 0,
            month_id,
            at_ratio,
            model_year_id,
        }
    }

    /// An [`Emission`] helper.
    fn emission(
        quant: f64,
        rate: f64,
        fuel_sub_type_id: i32,
        fuel_formulation_id: i32,
    ) -> Emission {
        Emission {
            fuel_sub_type_id,
            fuel_formulation_id,
            emission_quant: quant,
            emission_rate: rate,
        }
    }

    /// `polProcessID` for a `(pollutant, process)` pair.
    fn ppid(pollutant: i32, process: i32) -> i32 {
        pollutant * 100 + process
    }

    /// A VOC (87) fuel-block key: process 1, fuel type 1, model year 2020,
    /// month 6, source type 21. `pol_process_id` is `87 * 100 + 1`.
    fn voc_key() -> FuelBlockKey {
        FuelBlockKey {
            pollutant_id: VOC_POLLUTANT_ID,
            process_id: 1,
            pol_process_id: VOC_POLLUTANT_ID * 100 + 1,
            model_year_id: 2020,
            fuel_type_id: 1,
            month_id: 6,
            source_type_id: 21,
        }
    }

    /// [`ModuleFlags`] with every flag set — used when a test wants the path
    /// it is exercising to be the only one with a populated table.
    fn all_modules() -> ModuleFlags {
        ModuleFlags {
            minor_hap_ratio: true,
            pah_gas_ratio: true,
            pah_particle_ratio: true,
            at_ratio_gas1: true,
            at_ratio_gas2: true,
            at_ratio_non_gas: true,
        }
    }

    #[test]
    fn emission_scaled_multiplies_both_quant_and_rate() {
        let e = emission(8.0, 4.0, 20, 100);
        assert_eq!(e.scaled(0.5), emission(4.0, 2.0, 20, 100));
    }

    #[test]
    fn emission_scaled_keeps_fuel_ids() {
        let scaled = emission(8.0, 4.0, 20, 100).scaled(0.25);
        assert_eq!(scaled.fuel_sub_type_id, 20);
        assert_eq!(scaled.fuel_formulation_id, 100);
    }

    #[test]
    fn build_indexes_all_nine_tables() {
        let toxics = AirToxics::build(AirToxicsExtracts {
            minor_hap_ratio: vec![minor_hap_row(1, 20, 10, 2020, 0.5)],
            pah_gas_ratio: vec![pah_row(1, 168, 1, 2020, 0.25)],
            pah_particle_ratio: vec![pah_row(1, 68, 1, 2020, 0.1)],
            at_ratio_gas1_chained_to: vec![chained_row(40, 1, 8701)],
            at_ratio_gas2_chained_to: vec![chained_row(41, 1, 8701)],
            at_ratio_non_gas_chained_to: vec![chained_row(42, 1, 8701)],
            at_ratio: vec![at_ratio_row(100, 6, 2020, 4001, 2.0)],
            at_ratio_gas2: vec![AtRatioGas2Row {
                pol_process_id: 4101,
                source_type_id: 21,
                fuel_sub_type_id: 10,
                at_ratio: 3.0,
            }],
            at_ratio_non_gas: vec![AtRatioNonGasRow {
                pol_process_id: 4201,
                source_type_id: 21,
                fuel_sub_type_id: 10,
                model_year_id: 2020,
                at_ratio: 4.0,
            }],
        });
        assert_eq!(toxics.minor_hap_ratio.len(), 1);
        assert_eq!(toxics.pah_gas_ratio.len(), 1);
        assert_eq!(toxics.pah_particle_ratio.len(), 1);
        assert_eq!(toxics.at_ratio_gas1_chained_to.len(), 1);
        assert_eq!(toxics.at_ratio_gas2_chained_to.len(), 1);
        assert_eq!(toxics.at_ratio_non_gas_chained_to.len(), 1);
        assert_eq!(toxics.at_ratio.len(), 1);
        assert_eq!(toxics.at_ratio_gas2.len(), 1);
        assert_eq!(toxics.at_ratio_non_gas.len(), 1);
    }

    #[test]
    fn build_keeps_file_order_on_a_shared_ratio_key() {
        // Two minorHAPRatio rows share a key — both details are kept, in order.
        let toxics = AirToxics::build(AirToxicsExtracts {
            minor_hap_ratio: vec![
                minor_hap_row(1, 20, 10, 2020, 0.5),
                minor_hap_row(1, 24, 10, 2020, 0.7),
            ],
            ..Default::default()
        });
        let details = toxics
            .minor_hap_ratio
            .get(&MinorHapRatioKey {
                process_id: 1,
                fuel_sub_type_id: 10,
                model_year_id: 2020,
            })
            .expect("keyed details");
        assert_eq!(details.len(), 2);
        assert_eq!(details[0].output_pollutant_id, 20);
        assert_eq!(details[1].output_pollutant_id, 24);
    }

    #[test]
    fn build_last_write_wins_on_a_duplicate_at_ratio_key() {
        // Two ATRatio rows share a key — the Go map assignment keeps the last.
        let toxics = AirToxics::build(AirToxicsExtracts {
            at_ratio: vec![
                at_ratio_row(100, 6, 2020, 4001, 2.0),
                at_ratio_row(100, 6, 2020, 4001, 9.0),
            ],
            ..Default::default()
        });
        assert_eq!(
            toxics.at_ratio.get(&AtRatioKey {
                fuel_formulation_id: 100,
                month_id: 6,
                model_year_id: 2020,
                output_pol_process_id: 4001,
            }),
            Some(&9.0),
        );
    }

    #[test]
    fn build_chained_to_keyed_by_input_pol_process_id() {
        let toxics = AirToxics::build(AirToxicsExtracts {
            at_ratio_gas1_chained_to: vec![chained_row(40, 1, 8701), chained_row(41, 1, 8701)],
            ..Default::default()
        });
        let details = toxics
            .at_ratio_gas1_chained_to
            .get(&8701)
            .expect("keyed by input polProcessID");
        assert_eq!(details.len(), 2);
        assert_eq!(details[0].output_pollutant_id, 40);
        assert_eq!(details[1].output_pollutant_id, 41);
    }

    #[test]
    fn minor_hap_ratio_scales_voc() {
        let toxics = AirToxics::build(AirToxicsExtracts {
            minor_hap_ratio: vec![minor_hap_row(1, 20, 10, 2020, 0.5)],
            ..Default::default()
        });
        let block = FuelBlock {
            key: voc_key(),
            emissions: vec![emission(8.0, 4.0, 10, 100)],
        };
        let out = toxics.air_toxics_block(&block, all_modules());
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].pollutant_id, 20);
        assert_eq!(out[0].process_id, 1);
        assert_eq!(out[0].pol_process_id, ppid(20, 1));
        assert_eq!(out[0].emissions, vec![emission(4.0, 2.0, 10, 100)]);
    }

    #[test]
    fn minor_hap_ratio_keys_on_emission_fuel_sub_type() {
        // The row keys on fuel subtype 10; only the subtype-10 emission hits.
        let toxics = AirToxics::build(AirToxicsExtracts {
            minor_hap_ratio: vec![minor_hap_row(1, 20, 10, 2020, 0.5)],
            ..Default::default()
        });
        let block = FuelBlock {
            key: voc_key(),
            emissions: vec![emission(8.0, 4.0, 10, 100), emission(2.0, 1.0, 99, 100)],
        };
        let out = toxics.air_toxics_block(&block, all_modules());
        assert_eq!(out.len(), 1);
        // Only the subtype-10 emission produced a toxic.
        assert_eq!(out[0].emissions, vec![emission(4.0, 2.0, 10, 100)]);
    }

    #[test]
    fn minor_hap_ratio_skips_non_voc_input() {
        let toxics = AirToxics::build(AirToxicsExtracts {
            minor_hap_ratio: vec![minor_hap_row(1, 20, 10, 2020, 0.5)],
            ..Default::default()
        });
        // Organic Carbon (111) block — minorHAPRatio applies only to VOC.
        let block = FuelBlock {
            key: FuelBlockKey {
                pollutant_id: ORGANIC_CARBON_POLLUTANT_ID,
                ..voc_key()
            },
            emissions: vec![emission(8.0, 4.0, 10, 100)],
        };
        assert!(toxics.air_toxics_block(&block, all_modules()).is_empty());
    }

    #[test]
    fn pah_gas_ratio_scales_all_voc_emissions() {
        // pahGasRatio keys on the block's fuel type, so every emission is hit.
        let toxics = AirToxics::build(AirToxicsExtracts {
            pah_gas_ratio: vec![pah_row(1, 168, 1, 2020, 0.5)],
            ..Default::default()
        });
        let block = FuelBlock {
            key: voc_key(),
            emissions: vec![emission(8.0, 4.0, 10, 100), emission(2.0, 1.0, 99, 200)],
        };
        let out = toxics.air_toxics_block(&block, all_modules());
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].pollutant_id, 168);
        // Both emissions scaled, in input order.
        assert_eq!(
            out[0].emissions,
            vec![emission(4.0, 2.0, 10, 100), emission(1.0, 0.5, 99, 200)],
        );
    }

    #[test]
    fn pah_gas_ratio_keys_on_block_fuel_type() {
        // The row is for fuel type 2; a fuel-type-1 block does not match.
        let toxics = AirToxics::build(AirToxicsExtracts {
            pah_gas_ratio: vec![pah_row(1, 168, 2, 2020, 0.5)],
            ..Default::default()
        });
        let block = FuelBlock {
            key: voc_key(),
            emissions: vec![emission(8.0, 4.0, 10, 100)],
        };
        assert!(toxics.air_toxics_block(&block, all_modules()).is_empty());
    }

    #[test]
    fn pah_gas_ratio_skips_non_voc_input() {
        let toxics = AirToxics::build(AirToxicsExtracts {
            pah_gas_ratio: vec![pah_row(1, 168, 1, 2020, 0.5)],
            ..Default::default()
        });
        let block = FuelBlock {
            key: FuelBlockKey {
                pollutant_id: ORGANIC_CARBON_POLLUTANT_ID,
                ..voc_key()
            },
            emissions: vec![emission(8.0, 4.0, 10, 100)],
        };
        assert!(toxics.air_toxics_block(&block, all_modules()).is_empty());
    }

    #[test]
    fn pah_particle_ratio_scales_organic_carbon() {
        let toxics = AirToxics::build(AirToxicsExtracts {
            pah_particle_ratio: vec![pah_row(1, 68, 1, 2020, 0.25)],
            ..Default::default()
        });
        let block = FuelBlock {
            key: FuelBlockKey {
                pollutant_id: ORGANIC_CARBON_POLLUTANT_ID,
                ..voc_key()
            },
            emissions: vec![emission(8.0, 4.0, 10, 100)],
        };
        let out = toxics.air_toxics_block(&block, all_modules());
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].pollutant_id, 68);
        assert_eq!(out[0].emissions, vec![emission(2.0, 1.0, 10, 100)]);
    }

    #[test]
    fn pah_particle_ratio_skips_non_organic_carbon_input() {
        let toxics = AirToxics::build(AirToxicsExtracts {
            pah_particle_ratio: vec![pah_row(1, 68, 1, 2020, 0.25)],
            ..Default::default()
        });
        // VOC (87) block — pahParticleRatio applies only to Organic Carbon.
        let block = FuelBlock {
            key: voc_key(),
            emissions: vec![emission(8.0, 4.0, 10, 100)],
        };
        assert!(toxics.air_toxics_block(&block, all_modules()).is_empty());
    }

    #[test]
    fn direct_path_output_process_equals_input_process() {
        // A VOC block on process 2 produces a toxic on process 2.
        let toxics = AirToxics::build(AirToxicsExtracts {
            minor_hap_ratio: vec![minor_hap_row(2, 20, 10, 2020, 0.5)],
            ..Default::default()
        });
        let block = FuelBlock {
            key: FuelBlockKey {
                process_id: 2,
                pol_process_id: ppid(VOC_POLLUTANT_ID, 2),
                ..voc_key()
            },
            emissions: vec![emission(8.0, 4.0, 10, 100)],
        };
        let out = toxics.air_toxics_block(&block, all_modules());
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].process_id, 2);
        assert_eq!(out[0].pol_process_id, ppid(20, 2));
    }

    #[test]
    fn at_ratio_gas1_scales_via_chained_to() {
        // VOC running (8701) chains to toxic 40 on process 1.
        let toxics = AirToxics::build(AirToxicsExtracts {
            at_ratio_gas1_chained_to: vec![chained_row(40, 1, ppid(VOC_POLLUTANT_ID, 1))],
            at_ratio: vec![at_ratio_row(100, 6, 2020, ppid(40, 1), 2.0)],
            ..Default::default()
        });
        let block = FuelBlock {
            key: voc_key(),
            emissions: vec![emission(8.0, 4.0, 10, 100)],
        };
        let out = toxics.air_toxics_block(&block, all_modules());
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].pollutant_id, 40);
        assert_eq!(out[0].process_id, 1);
        assert_eq!(out[0].pol_process_id, ppid(40, 1));
        assert_eq!(out[0].emissions, vec![emission(16.0, 8.0, 10, 100)]);
    }

    #[test]
    fn at_ratio_gas1_keys_on_emission_fuel_formulation() {
        // The ATRatio row is for fuel formulation 100; only that emission hits.
        let toxics = AirToxics::build(AirToxicsExtracts {
            at_ratio_gas1_chained_to: vec![chained_row(40, 1, ppid(VOC_POLLUTANT_ID, 1))],
            at_ratio: vec![at_ratio_row(100, 6, 2020, ppid(40, 1), 2.0)],
            ..Default::default()
        });
        let block = FuelBlock {
            key: voc_key(),
            emissions: vec![emission(8.0, 4.0, 10, 100), emission(2.0, 1.0, 10, 200)],
        };
        let out = toxics.air_toxics_block(&block, all_modules());
        assert_eq!(out.len(), 1);
        // Only the formulation-100 emission produced a toxic.
        assert_eq!(out[0].emissions, vec![emission(16.0, 8.0, 10, 100)]);
    }

    #[test]
    fn at_ratio_gas1_output_uses_chained_to_process() {
        // The chained-to row sends a process-1 input to a process-2 toxic.
        let toxics = AirToxics::build(AirToxicsExtracts {
            at_ratio_gas1_chained_to: vec![chained_row(40, 2, ppid(VOC_POLLUTANT_ID, 1))],
            at_ratio: vec![at_ratio_row(100, 6, 2020, ppid(40, 2), 2.0)],
            ..Default::default()
        });
        let block = FuelBlock {
            key: voc_key(),
            emissions: vec![emission(8.0, 4.0, 10, 100)],
        };
        let out = toxics.air_toxics_block(&block, all_modules());
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].process_id, 2);
        assert_eq!(out[0].pol_process_id, ppid(40, 2));
    }

    #[test]
    fn at_ratio_gas1_skips_emission_without_a_ratio() {
        // The chained-to row exists but no ATRatio matches — nothing produced.
        let toxics = AirToxics::build(AirToxicsExtracts {
            at_ratio_gas1_chained_to: vec![chained_row(40, 1, ppid(VOC_POLLUTANT_ID, 1))],
            at_ratio: vec![at_ratio_row(100, 6, 2020, ppid(99, 1), 2.0)],
            ..Default::default()
        });
        let block = FuelBlock {
            key: voc_key(),
            emissions: vec![emission(8.0, 4.0, 10, 100)],
        };
        assert!(toxics.air_toxics_block(&block, all_modules()).is_empty());
    }

    #[test]
    fn at_ratio_gas2_keys_on_source_type_and_fuel_sub_type() {
        let toxics = AirToxics::build(AirToxicsExtracts {
            at_ratio_gas2_chained_to: vec![chained_row(41, 1, ppid(VOC_POLLUTANT_ID, 1))],
            at_ratio_gas2: vec![AtRatioGas2Row {
                pol_process_id: ppid(41, 1),
                source_type_id: 21,
                fuel_sub_type_id: 10,
                at_ratio: 0.5,
            }],
            ..Default::default()
        });
        let block = FuelBlock {
            key: voc_key(),
            // Only the subtype-10 emission matches the gas2 row.
            emissions: vec![emission(8.0, 4.0, 10, 100), emission(2.0, 1.0, 99, 100)],
        };
        let out = toxics.air_toxics_block(&block, all_modules());
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].pollutant_id, 41);
        assert_eq!(out[0].emissions, vec![emission(4.0, 2.0, 10, 100)]);
    }

    #[test]
    fn at_ratio_gas2_skips_when_source_type_differs() {
        let toxics = AirToxics::build(AirToxicsExtracts {
            at_ratio_gas2_chained_to: vec![chained_row(41, 1, ppid(VOC_POLLUTANT_ID, 1))],
            at_ratio_gas2: vec![AtRatioGas2Row {
                pol_process_id: ppid(41, 1),
                source_type_id: 99,
                fuel_sub_type_id: 10,
                at_ratio: 0.5,
            }],
            ..Default::default()
        });
        let block = FuelBlock {
            key: voc_key(),
            emissions: vec![emission(8.0, 4.0, 10, 100)],
        };
        assert!(toxics.air_toxics_block(&block, all_modules()).is_empty());
    }

    #[test]
    fn at_ratio_non_gas_keys_on_model_year() {
        let toxics = AirToxics::build(AirToxicsExtracts {
            at_ratio_non_gas_chained_to: vec![chained_row(42, 1, ppid(VOC_POLLUTANT_ID, 1))],
            at_ratio_non_gas: vec![AtRatioNonGasRow {
                pol_process_id: ppid(42, 1),
                source_type_id: 21,
                fuel_sub_type_id: 10,
                model_year_id: 2020,
                at_ratio: 0.25,
            }],
            ..Default::default()
        });
        // Matching model year 2020.
        let block = FuelBlock {
            key: voc_key(),
            emissions: vec![emission(8.0, 4.0, 10, 100)],
        };
        let out = toxics.air_toxics_block(&block, all_modules());
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].emissions, vec![emission(2.0, 1.0, 10, 100)]);

        // Non-matching model year 2010 — nothing produced.
        let block = FuelBlock {
            key: FuelBlockKey {
                model_year_id: 2010,
                ..voc_key()
            },
            emissions: vec![emission(8.0, 4.0, 10, 100)],
        };
        assert!(toxics.air_toxics_block(&block, all_modules()).is_empty());
    }

    #[test]
    fn module_flag_off_disables_a_path() {
        let toxics = AirToxics::build(AirToxicsExtracts {
            minor_hap_ratio: vec![minor_hap_row(1, 20, 10, 2020, 0.5)],
            ..Default::default()
        });
        let block = FuelBlock {
            key: voc_key(),
            emissions: vec![emission(8.0, 4.0, 10, 100)],
        };
        // minor_hap_ratio flag cleared — the table is non-empty but unused.
        let modules = ModuleFlags {
            minor_hap_ratio: false,
            ..all_modules()
        };
        assert!(toxics.air_toxics_block(&block, modules).is_empty());
    }

    #[test]
    fn empty_table_disables_a_path() {
        // The flag is set but the table is empty — the Go's len(table) > 0 gate.
        let toxics = AirToxics::build(AirToxicsExtracts::default());
        let block = FuelBlock {
            key: voc_key(),
            emissions: vec![emission(8.0, 4.0, 10, 100)],
        };
        assert!(toxics.air_toxics_block(&block, all_modules()).is_empty());
    }

    #[test]
    fn paths_accumulate_into_a_shared_output_block() {
        // Both the minorHAPRatio path and the ATRatioGas1 path produce
        // pollutant 40 on process 1 — they share polProcessID 4001 and the
        // output block carries both paths' emissions, in path order.
        let toxics = AirToxics::build(AirToxicsExtracts {
            minor_hap_ratio: vec![minor_hap_row(1, 40, 10, 2020, 0.5)],
            at_ratio_gas1_chained_to: vec![chained_row(40, 1, ppid(VOC_POLLUTANT_ID, 1))],
            at_ratio: vec![at_ratio_row(100, 6, 2020, ppid(40, 1), 3.0)],
            ..Default::default()
        });
        let block = FuelBlock {
            key: voc_key(),
            emissions: vec![emission(8.0, 4.0, 10, 100)],
        };
        let out = toxics.air_toxics_block(&block, all_modules());
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].pol_process_id, ppid(40, 1));
        // minorHAPRatio (×0.5) runs before ATRatioGas1 (×3.0).
        assert_eq!(
            out[0].emissions,
            vec![emission(4.0, 2.0, 10, 100), emission(24.0, 12.0, 10, 100)],
        );
    }

    #[test]
    fn output_blocks_sorted_by_pol_process_id() {
        // Three minorHAPRatio details produce pollutants 46, 20, 40 — the
        // output blocks come back in ascending polProcessID order.
        let toxics = AirToxics::build(AirToxicsExtracts {
            minor_hap_ratio: vec![
                minor_hap_row(1, 46, 10, 2020, 0.5),
                minor_hap_row(1, 20, 10, 2020, 0.5),
                minor_hap_row(1, 40, 10, 2020, 0.5),
            ],
            ..Default::default()
        });
        let block = FuelBlock {
            key: voc_key(),
            emissions: vec![emission(8.0, 4.0, 10, 100)],
        };
        let out = toxics.air_toxics_block(&block, all_modules());
        let pol_process_ids: Vec<i32> = out.iter().map(|b| b.pol_process_id).collect();
        assert_eq!(pol_process_ids, vec![ppid(20, 1), ppid(40, 1), ppid(46, 1)]);
    }

    #[test]
    fn chained_to_with_multiple_outputs() {
        // One input polProcessID chains to two toxics.
        let toxics = AirToxics::build(AirToxicsExtracts {
            at_ratio_gas1_chained_to: vec![
                chained_row(40, 1, ppid(VOC_POLLUTANT_ID, 1)),
                chained_row(41, 1, ppid(VOC_POLLUTANT_ID, 1)),
            ],
            at_ratio: vec![
                at_ratio_row(100, 6, 2020, ppid(40, 1), 2.0),
                at_ratio_row(100, 6, 2020, ppid(41, 1), 0.5),
            ],
            ..Default::default()
        });
        let block = FuelBlock {
            key: voc_key(),
            emissions: vec![emission(8.0, 4.0, 10, 100)],
        };
        let out = toxics.air_toxics_block(&block, all_modules());
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].pollutant_id, 40);
        assert_eq!(out[0].emissions, vec![emission(16.0, 8.0, 10, 100)]);
        assert_eq!(out[1].pollutant_id, 41);
        assert_eq!(out[1].emissions, vec![emission(4.0, 2.0, 10, 100)]);
    }

    #[test]
    fn empty_input_block_produces_nothing() {
        let toxics = AirToxics::build(AirToxicsExtracts {
            minor_hap_ratio: vec![minor_hap_row(1, 20, 10, 2020, 0.5)],
            ..Default::default()
        });
        let block = FuelBlock {
            key: voc_key(),
            emissions: vec![],
        };
        assert!(toxics.air_toxics_block(&block, all_modules()).is_empty());
    }

    #[test]
    fn calculator_metadata() {
        let calc = AirToxicsCalculator::new();
        assert_eq!(calc.name(), "AirToxicsCalculator");
        // Chained calculator — no direct master-loop subscription.
        assert!(calc.subscriptions().is_empty());
        assert_eq!(
            calc.upstream(),
            &["HCSpeciationCalculator", "SulfatePMCalculator"],
        );
        for table in [
            "ATRatio",
            "ATRatioGas2",
            "ATRatioNonGas",
            "RunSpecChainedTo",
            "minorHAPRatio",
            "pahGasRatio",
            "pahParticleRatio",
        ] {
            assert!(calc.input_tables().contains(&table), "missing {table}");
        }
    }

    #[test]
    fn calculator_registers_195_pollutant_process_pairs() {
        assert_eq!(REGISTRATION_COUNT, 195);
        let calc = AirToxicsCalculator::new();
        let regs = calc.registrations();
        assert_eq!(regs.len(), 195);

        // Spot-check a registration from each of the irregular process groups:
        // benzene (20) in running (1) and start (2) exhaust, naphthalene gas
        // (185) in evap permeation (11), benzene in extended-idle exhaust (90).
        assert!(regs.contains(&reg(20, 1)));
        assert!(regs.contains(&reg(20, 2)));
        assert!(regs.contains(&reg(185, 11)));
        assert!(regs.contains(&reg(20, 90)));

        // The particulate PAH species (68–84) are exhaust-only — running and
        // start, never the evaporative or idle processes.
        assert!(regs.contains(&reg(68, 1)));
        assert!(!regs.contains(&reg(68, 11)));
        assert!(!regs.contains(&reg(68, 90)));

        // Ethanol (21) and MTBE (22) are not registered for idle exhaust (90).
        assert!(!regs.contains(&reg(21, 90)));
        assert!(!regs.contains(&reg(22, 90)));

        // No registration is duplicated.
        let mut seen = std::collections::HashSet::new();
        for r in regs {
            assert!(
                seen.insert((r.pollutant_id, r.process_id)),
                "duplicate {r:?}"
            );
        }
    }

    #[test]
    fn execute_wires_through_data_plane() {
        use moves_framework::DataFrameStore;
        let calc = AirToxicsCalculator::new();
        let mut store = moves_framework::InMemoryStore::new();
        // minorHAPRatio: VOC (87), process 1, sub-type 10, MY 2020 → pollutant 20, ratio 0.5.
        store.insert(
            "minorHAPRatio",
            MinorHapRatioRow::into_dataframe(vec![minor_hap_row(1, 20, 10, 2020, 0.5)]).unwrap(),
        );
        // Empty tables for unexercised paths.
        store.insert("pahGasRatio", PahRatioRow::into_dataframe(vec![]).unwrap());
        store.insert(
            "pahParticleRatio",
            PahRatioRow::into_dataframe(vec![]).unwrap(),
        );
        store.insert(
            "RunSpecChainedTo",
            ChainedToRow::into_dataframe(vec![]).unwrap(),
        );
        store.insert("ATRatio", AtRatioRow::into_dataframe(vec![]).unwrap());
        store.insert(
            "ATRatioGas2",
            AtRatioGas2Row::into_dataframe(vec![]).unwrap(),
        );
        store.insert(
            "ATRatioNonGas",
            AtRatioNonGasRow::into_dataframe(vec![]).unwrap(),
        );
        // Input: one VOC (87) row matching the minorHAPRatio entry above.
        store.insert(
            "MOVESWorkerOutput",
            AirToxicsMwoRow::into_dataframe(vec![AirToxicsMwoRow {
                year_id: 2020,
                month_id: 6,
                day_id: 5,
                hour_id: 8,
                state_id: 26,
                county_id: 26161,
                zone_id: 261_610,
                link_id: 5001,
                pollutant_id: 87,
                process_id: 1,
                source_type_id: 21,
                reg_class_id: 0,
                fuel_type_id: 1,
                model_year_id: 2020,
                road_type_id: 5,
                fuel_sub_type_id: 10,
                fuel_formulation_id: 100,
                emission_quant: 100.0,
                emission_rate: 0.0,
            }])
            .unwrap(),
        );
        let ctx = CalculatorContext::with_tables(store);
        let out = calc.execute(&ctx).expect("execute ok");
        let df = out.dataframe().expect("output should contain a DataFrame");
        // minorHAPRatio fires: VOC 87 * 0.5 → pollutant 20.
        assert_eq!(
            df.height(),
            1,
            "expected 1 output row from minorHAPRatio path"
        );
        let eq = df
            .column("emissionQuant")
            .unwrap()
            .f64()
            .unwrap()
            .get(0)
            .unwrap();
        assert!((eq - 50.0).abs() < 1e-9, "emissionQuant {eq} != 50.0");
        let pol = df
            .column("pollutantID")
            .unwrap()
            .i32()
            .unwrap()
            .get(0)
            .unwrap();
        assert_eq!(pol, 20);
    }

    #[test]
    fn calculator_is_object_safe() {
        // The registry stores calculators as Box<dyn Calculator>.
        let calc: Box<dyn Calculator> = Box::new(AirToxicsCalculator::new());
        assert_eq!(calc.name(), "AirToxicsCalculator");
        assert_eq!(calc.registrations().len(), 195);
    }
}
