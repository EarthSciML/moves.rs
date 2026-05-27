//! Port of `calc/nrairtoxics/nrairtoxics.go` — the `NRAirToxicsCalculator`,
//! the Nonroad air-toxics calculator.
//!
//! Migration plan: Phase 3, Task 52 (the Nonroad equivalent of Task 50,
//! `AirToxicsCalculator`).
//!
//! # What this calculator does
//!
//! `NRAirToxicsCalculator` derives the nonroad air-toxics pollutants from
//! three upstream nonroad emission tallies:
//!
//! * **VOC** (pollutant 87) — the gaseous toxics: benzene, ethanol, MTBE,
//!   1,3-butadiene, the aldehydes, the aromatics, and the *gaseous* PAH
//!   (polycyclic aromatic hydrocarbon) species;
//! * **PM2.5** (pollutant 110) — the *particulate* PAH species;
//! * **Fuel consumption** (pollutant 99, running exhaust only) — the metallic
//!   toxics (mercury, arsenic, chromium, manganese, nickel) and the
//!   dioxin/furan congeners.
//!
//! Each output toxic is a fixed multiple of its input pollutant. The
//! multipliers come from five lookup tables; a sixth table drives a separate
//! NonHAPTOG pass (see below).
//!
//! # The five ratio tables and the algorithm
//!
//! For one input [`Emission`] the calculator looks up the ratio rows that key
//! to the emission's process / engine-technology / fuel and, for every output
//! pollutant the run needs, scales the input emission:
//!
//! ```text
//! VOC (87)  -> nrATRatio           : output = VOC   * atRatio
//!           -> nrPAHGasRatio       : output = VOC   * atRatio
//! PM2.5(110)-> nrPAHParticleRatio  : output = PM2.5 * atRatio
//! fuel (99) -> nrDioxinEmissionRate: output = gallons(fuel) * meanBaseRate
//!           -> nrMetalEmissionRate : output = gallons(fuel) * meanBaseRate
//! ```
//!
//! The dioxin and metal rates are expressed *per gallon of fuel* while the
//! fuel-consumption input arrives in *grams*, so those two paths multiply by a
//! grams→gallons conversion (see [`gallons_factor`]). The ratio tables apply
//! their multiplier directly.
//!
//! An emission carries both an emission quantity and an emission rate; every
//! scaling above multiplies *both*.
//!
//! # The NonHAPTOG pass
//!
//! A second, independent pass computes NonHAPTOG (pollutant 88) — total
//! organic gases minus the hazardous air pollutants. The Go runs it as a
//! separate goroutine pool (`StartCalculatingNonHAPTOG`); this port keeps it
//! as a separate method, [`NrAirToxics::non_hap_tog_block`]. NonHAPTOG is
//! accumulated from partial contributions:
//!
//! ```text
//! NonHAPTOG(88)(partial) = + NMOG(80)
//! NonHAPTOG(88)(partial) = - <integrated species>
//! ```
//!
//! i.e. an NMOG block contributes `+NMOG` and each `nrIntegratedSpecies`
//! pollutant contributes its own negated emission. The full NonHAPTOG total is
//! the sum of those partials, formed downstream when the per-pollutant blocks
//! are aggregated.
//!
//! # The six lookup tables
//!
//! * `nrATRatio` — gaseous-toxic ratios, keyed by
//!   `(processID, engTechID, fuelSubTypeID, nrHPCategory)` ([`AtRatioRow`]);
//! * `nrPAHGasRatio`, `nrPAHParticleRatio`, `nrDioxinEmissionRate`,
//!   `nrMetalEmissionRate` — all keyed by
//!   `(processID, fuelTypeID, engTechID, nrHPCategory)` ([`ProcFuelEngHpRow`]);
//! * `nrIntegratedSpecies` — the set of pollutant ids subtracted from NMOG to
//!   form NonHAPTOG.
//!
//! Each ratio table maps a key to a *list* of ratio details (the Go
//! `map[Key][]*Detail`): a key can carry one detail per output pollutant.
//! `nrHPCategory` is a single-character horse-power-category code; the Go
//! reads it as a byte, so it is a [`u8`] here.
//!
//! # Relationship to Task 50 (`AirToxicsCalculator`)
//!
//! The onroad `AirToxicsCalculator` covers the same toxic families but keys
//! its lookups on model-year ranges and onroad source types. None of that
//! applies to nonroad equipment — the nonroad keys carry no model year, and
//! the horse-power category replaces the source type. This port mirrors the
//! (simpler) Nonroad Go file exactly.
//!
//! # Scope of this port
//!
//! The pinned Go file is the whole `nrairtoxics` package: the in-memory
//! lookup-table load (`StartSetup`) and the two per-block passes (`calculate`
//! and `calculateNonHAPTOG`). All three are ported in full —
//! [`NrAirToxics::build`], [`NrAirToxics::air_toxics_block`] and
//! [`NrAirToxics::non_hap_tog_block`].
//!
//! The Go ran each pass as a pool of goroutines draining a channel of
//! `MWOBlock`s; that worker plumbing is not part of the calculation and is
//! dropped. This port keeps the **computation** — the lookups, the scaling
//! formulas, the per-pollutant grouping into new fuel blocks — and replaces
//! the channel boundary with plain values: [`FuelBlock`]s in,
//! [`ToxicFuelBlock`]s out.
//!
//! # Fidelity notes
//!
//! * **Per-emission pollutant overwrite.** The Go keys its per-emission
//!   `emissions` map by `pollutantID`. The VOC path fills it from `nrATRatio`
//!   then `nrPAHGasRatio`; the fuel path fills it from `nrDioxinEmissionRate`
//!   then `nrMetalEmissionRate`. If two tables tabulate the same output
//!   pollutant the *later* table wins. This port preserves that: the produced
//!   emissions are kept in a map and a later insert overwrites an earlier one.
//! * **Unknown fuel formulation skips the whole emission.** The Go's
//!   `ff == nil` check precedes every pollutant branch, so an emission whose
//!   fuel formulation is unknown produces nothing — even on the PM2.5 and
//!   fuel-consumption paths, which never read the formulation's fuel subtype.
//!   [`air_toxics_for_emission`](NrAirToxics::air_toxics_for_emission) returns
//!   `None` in that case.
//! * **Two fuel-id sources.** `nrATRatio` keys on the *fuel formulation's*
//!   `fuelSubTypeID`; the other four tables key on the *block's* `fuelTypeID`.
//!   The Go reads them from those two distinct places and this port preserves
//!   the distinction.
//! * **Grams→gallons conversion order.** The Go computes the conversion as
//!   `(1.0/453.592)/density` — two sequential divisions — not the algebraically
//!   equal `1.0/(453.592*density)`. [`gallons_factor`] matches the Go's order
//!   exactly so the f64 rounding is bit-identical.
//! * **Output order.** The Go grouped output emissions into new fuel blocks
//!   keyed in a Go `map`, whose iteration order is randomised.
//!   [`air_toxics_block`](NrAirToxics::air_toxics_block) returns the blocks in
//!   ascending pollutant-id order so the output is deterministic; a fuel-block
//!   set is unordered, so this is a presentation choice only.
//!
//! # Data plane (Task 50)
//!
//! [`Calculator::execute`] receives a [`CalculatorContext`] whose
//! `ExecutionTables` / `ScratchNamespace` are Phase-2 placeholders until the
//! `DataFrameStore` lands (migration-plan Task 50), so `execute` cannot yet
//! read the six lookup tables nor the upstream VOC / PM2.5 / fuel-consumption
//! emission blocks, nor write the toxic blocks back. The numerically faithful
//! algorithm is fully ported and unit-tested on [`NrAirToxics`]; once the data
//! plane exists, `execute` builds an [`NrAirToxics`] from `ctx.tables()`, reads
//! the input [`FuelBlock`]s, applies
//! [`air_toxics_block`](NrAirToxics::air_toxics_block) and
//! [`non_hap_tog_block`](NrAirToxics::non_hap_tog_block), and stores the
//! resulting [`ToxicFuelBlock`]s.

use std::collections::{BTreeMap, HashMap, HashSet};

use moves_data::{PollutantId, PollutantProcessAssociation, ProcessId};
use moves_framework::{
    Calculator, CalculatorContext, CalculatorOutput, CalculatorSubscription, DataFrameStoreTyped,
    Error, TableRow,
};
use polars::prelude::{DataFrame, DataType, NamedFrom, PolarsResult, Schema, Series};

/// VOC — volatile organic compounds, pollutant 87. The input the gaseous
/// toxics (`nrATRatio`) and gaseous PAH (`nrPAHGasRatio`) are scaled from.
const VOC_POLLUTANT_ID: i32 = 87;
/// PM2.5 — particulate matter ≤2.5 µm, pollutant 110. The input the
/// particulate PAH (`nrPAHParticleRatio`) are scaled from.
const PM25_POLLUTANT_ID: i32 = 110;
/// Fuel consumption, pollutant 99. The input the dioxins and metals are scaled
/// from — and only for running exhaust (see [`RUNNING_EXHAUST_PROCESS_ID`]).
const FUEL_CONSUMPTION_POLLUTANT_ID: i32 = 99;
/// Running Exhaust, process 1. The dioxin/metal path runs only for fuel
/// consumption emitted by this process.
const RUNNING_EXHAUST_PROCESS_ID: i32 = 1;
/// NMOG — non-methane organic gases, pollutant 80. The positive term of the
/// NonHAPTOG sum.
const NMOG_POLLUTANT_ID: i32 = 80;
/// NonHAPTOG — total organic gases minus the hazardous air pollutants,
/// pollutant 88. The output of the NonHAPTOG pass.
const NON_HAP_TOG_POLLUTANT_ID: i32 = 88;

/// Grams per pound — `1 lb = 453.592 g`. Fuel consumption arrives in grams;
/// the dioxin and metal emission rates are tabulated per gallon of fuel, so
/// the conversion divides out the grams and the fuel density (see
/// [`gallons_factor`]).
const GRAMS_PER_POUND: f64 = 453.592;

/// Grams→gallons conversion factor for a fuel type — the Go `gallonsFactor`.
///
/// Fuel consumption (pollutant 99) is reported in grams, but the dioxin and
/// metal emission rates are per gallon of fuel. The factor converts:
/// `gallons = grams / (453.592 g/lb × density lb/gal)`.
///
/// The Go computes this as two sequential divisions —
/// `gallonsFactor := 1.0/453.592` then `gallonsFactor /= density` — so this
/// port writes `(1.0 / GRAMS_PER_POUND) / density` to keep the same f64
/// rounding rather than the algebraically equal `1.0 / (453.592 * density)`.
///
/// The per-fuel densities (pounds per gallon) are the Go literals: gasoline
/// `6.17`, diesel `7.1`, CNG `0.0061`, LPG `4.507`. Fuel type ids 23 and 24
/// are nonroad diesel variants and share the diesel density. Any other fuel
/// type gets the identity factor `1.0` — the Go `default: gallonsFactor = 1`,
/// applying no conversion.
#[must_use]
pub fn gallons_factor(fuel_type_id: i32) -> f64 {
    let grams_to_pounds = 1.0 / GRAMS_PER_POUND;
    match fuel_type_id {
        // Gasoline.
        1 => grams_to_pounds / 6.17,
        // Diesel and its nonroad variants (23, 24).
        2 | 23 | 24 => grams_to_pounds / 7.1,
        // CNG.
        3 => grams_to_pounds / 0.0061,
        // LPG.
        4 => grams_to_pounds / 4.507,
        // Any other fuel type: no conversion.
        _ => 1.0,
    }
}

/// One emission record — the Go `mwo.MWOEmission`, restricted to the fields
/// the air-toxics calculator reads and writes.
///
/// An emission carries a quantity and a rate; the scaling formulas multiply
/// *both*. `fuel_sub_type_id` and `fuel_formulation_id` identify the fuel the
/// emission belongs to and are carried through unchanged onto every toxic
/// emission derived from it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Emission {
    /// `fuelSubTypeID` — the emission's fuel subtype.
    pub fuel_sub_type_id: i32,
    /// `fuelFormulationID` — the emission's fuel formulation. Keys the
    /// nonroad worker's `FuelFormulations` table to recover the *formulation's*
    /// fuel subtype, which keys the `nrATRatio` lookup.
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

/// Key of the `nrATRatio` lookup — the Go `NRATRatioKey`.
///
/// `nrATRatio` is the only one of the five ratio tables keyed by *fuel
/// subtype*; the other four key by *fuel type* (see [`ProcFuelEngHpKey`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct AtRatioKey {
    /// `processID`.
    process_id: i32,
    /// `engTechID`.
    eng_tech_id: i32,
    /// `fuelSubTypeID` — the *fuel formulation's* subtype.
    fuel_sub_type_id: i32,
    /// `nrHPCategory` — the single-character horse-power-category code.
    nr_hp_category: u8,
}

/// Key of the `nrPAHGasRatio`, `nrPAHParticleRatio`, `nrDioxinEmissionRate`
/// and `nrMetalEmissionRate` lookups — the Go `NRProcFuelEngHPKey`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct ProcFuelEngHpKey {
    /// `processID`.
    process_id: i32,
    /// `fuelTypeID` — the *block's* fuel type.
    fuel_type_id: i32,
    /// `engTechID`.
    eng_tech_id: i32,
    /// `nrHPCategory` — the single-character horse-power-category code.
    nr_hp_category: u8,
}

/// One ratio-table detail — the Go `NRATRatioDetail` / `NRProcFuelEngHPDetail`
/// (the two Go types are structurally identical).
///
/// A ratio table maps a key to a list of these: one detail per output
/// pollutant the key produces.
#[derive(Debug, Clone, Copy, PartialEq)]
struct RatioDetail {
    /// `pollutantID` — the output toxic pollutant.
    pollutant_id: i32,
    /// `polProcessID` — `pollutantID * 100 + processID`, the id checked
    /// against the run's needed set.
    pol_process_id: i32,
    /// The multiplier — `atRatio` for the PAH ratio tables, `meanBaseRate`
    /// for the dioxin and metal emission-rate tables. The Go stores all of
    /// them in a single `atRatio` field.
    ratio: f64,
}

/// One `nrATRatio` table row — input to [`NrAirToxics::build`].
///
/// The Go `StartSetup` reads these columns from the `nratratio` extract file
/// (and the SQL `cache select` lists them in this order): `pollutantID,
/// processID, engTechID, fuelSubtypeID, nrHPCategory, atRatio`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AtRatioRow {
    /// `pollutantID` — the output toxic.
    pub pollutant_id: i32,
    /// `processID`.
    pub process_id: i32,
    /// `engTechID`.
    pub eng_tech_id: i32,
    /// `fuelSubtypeID`.
    pub fuel_sub_type_id: i32,
    /// `nrHPCategory` — the horse-power-category code byte.
    pub nr_hp_category: u8,
    /// `atRatio` — the toxic-to-VOC ratio.
    pub at_ratio: f64,
}

/// One `nrPAHGasRatio` / `nrPAHParticleRatio` / `nrDioxinEmissionRate` /
/// `nrMetalEmissionRate` table row — input to [`NrAirToxics::build`].
///
/// All four tables share this column layout (the Go reads them with the same
/// parse lambda and the SQL `cache select`s list the same columns):
/// `pollutantID, processID, fuelTypeID, engTechID, nrHPCategory, ratio` —
/// where the last column is `atratio` for the PAH tables and `meanBaseRate`
/// for the dioxin and metal tables.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ProcFuelEngHpRow {
    /// `pollutantID` — the output toxic.
    pub pollutant_id: i32,
    /// `processID`.
    pub process_id: i32,
    /// `fuelTypeID`.
    pub fuel_type_id: i32,
    /// `engTechID`.
    pub eng_tech_id: i32,
    /// `nrHPCategory` — the horse-power-category code byte.
    pub nr_hp_category: u8,
    /// `atratio` (PAH tables) or `meanBaseRate` (dioxin / metal tables).
    pub ratio: f64,
}

/// The key fields of an MWO `FuelBlock` that the NR air-toxics calculator
/// reads — a subset of the Go `mwo.MWOKey`.
///
/// The Go `calculate` reads only these five fields of the input block's key;
/// the rest of `MWOKey` (geography, time, source type, …) is opaque
/// passthrough that the data-plane integration copies onto the output blocks,
/// so it is not modeled here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FuelBlockKey {
    /// `pollutantID` — the calculator processes only VOC (87), PM2.5 (110)
    /// and fuel-consumption (99) blocks.
    pub pollutant_id: i32,
    /// `processID`.
    pub process_id: i32,
    /// `engTechID`.
    pub eng_tech_id: i32,
    /// `fuelTypeID` — keys the four `ProcFuelEngHp` ratio tables and selects
    /// the [`gallons_factor`].
    pub fuel_type_id: i32,
    /// `hpID` — keys the `nrHPCategory` horse-power-category lookup.
    pub hp_id: i32,
}

/// One input fuel block — the Go `mwo.FuelBlock`, restricted to the key
/// fields and emissions the calculator consumes.
#[derive(Debug, Clone, PartialEq)]
pub struct FuelBlock {
    /// The block's key fields.
    pub key: FuelBlockKey,
    /// The per-fuel-formulation emissions in the block.
    pub emissions: Vec<Emission>,
}

/// One output fuel block — a single toxic pollutant's emissions.
///
/// The Go `calculate` / `calculateNonHAPTOG` produced these by copying the
/// input block's key, overwriting `pollutantID` and `polProcessID`, and
/// attaching the derived emissions. This port returns the two computed key
/// fields plus the emissions; copying the rest of the input key is data-plane
/// plumbing the caller handles.
#[derive(Debug, Clone, PartialEq)]
pub struct ToxicFuelBlock {
    /// `pollutantID` of the derived toxic.
    pub pollutant_id: i32,
    /// `polProcessID` — `pollutantID * 100 + processID`.
    pub pol_process_id: i32,
    /// The derived emissions, one per input emission that produced this
    /// pollutant, in input-emission order.
    pub emissions: Vec<Emission>,
}

/// The ambient nonroad-worker lookup tables the calculator consults but does
/// not own — the Go `mwo` package globals.
///
/// The six air-toxics tables are loaded by the calculator itself
/// ([`NrAirToxics`]); the tables here are populated elsewhere in the nonroad
/// worker setup and shared across calculators:
///
/// * `FuelFormulations` — the calculator reads only each formulation's
///   `fuelSubTypeID`, so just that projection is stored;
/// * `NRHPCategory` — the `(hpID, engTechID)` → horse-power-category map;
/// * `NeededPolProcessIDs` — the set of `polProcessID`s the run requires,
///   which gates which output pollutants are computed.
#[derive(Debug, Clone, Default)]
pub struct NonroadWorkerTables {
    /// `mwo.FuelFormulations` projected to `fuelFormulationID → fuelSubTypeID`.
    fuel_sub_type_by_formulation: HashMap<i32, i32>,
    /// `mwo.NRHPCategory` — `(hpID, engTechID)` → horse-power-category byte.
    hp_categories: HashMap<(i32, i32), u8>,
    /// `mwo.NeededPolProcessIDs` — the set of needed `polProcessID`s.
    needed_pol_process_ids: HashSet<i32>,
}

impl NonroadWorkerTables {
    /// Assemble the worker tables from their three inputs.
    ///
    /// * `fuel_formulations` — `(fuelFormulationID, fuelSubTypeID)` pairs;
    /// * `hp_categories` — `((hpID, engTechID), nrHPCategory)` pairs;
    /// * `needed_pol_process_ids` — the needed `polProcessID`s.
    #[must_use]
    pub fn new(
        fuel_formulations: impl IntoIterator<Item = (i32, i32)>,
        hp_categories: impl IntoIterator<Item = ((i32, i32), u8)>,
        needed_pol_process_ids: impl IntoIterator<Item = i32>,
    ) -> Self {
        Self {
            fuel_sub_type_by_formulation: fuel_formulations.into_iter().collect(),
            hp_categories: hp_categories.into_iter().collect(),
            needed_pol_process_ids: needed_pol_process_ids.into_iter().collect(),
        }
    }

    /// The fuel subtype of a fuel formulation, or `None` when the formulation
    /// is unknown — the Go `mwo.FuelFormulations[id]` returning nil, which
    /// makes `calculate` skip the emission.
    fn fuel_sub_type_id(&self, fuel_formulation_id: i32) -> Option<i32> {
        self.fuel_sub_type_by_formulation
            .get(&fuel_formulation_id)
            .copied()
    }

    /// The horse-power category for an `(hpID, engTechID)` pair.
    ///
    /// A missing entry yields `0` — the zero value a Go map returns for an
    /// absent key, which `calculate` then carries straight into the lookup
    /// keys.
    fn hp_category(&self, hp_id: i32, eng_tech_id: i32) -> u8 {
        self.hp_categories
            .get(&(hp_id, eng_tech_id))
            .copied()
            .unwrap_or(0)
    }

    /// Whether a `polProcessID` is in the run's needed set — the Go
    /// `mwo.NeededPolProcessIDs[ppid]`.
    fn is_pol_process_needed(&self, pol_process_id: i32) -> bool {
        self.needed_pol_process_ids.contains(&pol_process_id)
    }
}

/// Apply one ratio-table detail slice to one input emission.
///
/// For every detail whose output `polProcessID` is in the run's needed set,
/// the input emission is scaled by `detail.ratio * unit_factor` and recorded
/// in `produced` under the detail's output pollutant.
///
/// `unit_factor` is `1.0` for the direct-ratio tables (`nrATRatio`,
/// `nrPAHGasRatio`, `nrPAHParticleRatio`) and the [`gallons_factor`] for the
/// per-gallon emission-rate tables (`nrDioxinEmissionRate`,
/// `nrMetalEmissionRate`). A later detail with the same output pollutant
/// overwrites an earlier one — the Go keys its `emissions` map by
/// `pollutantID` (see the module-level fidelity note).
fn apply_ratio_details(
    details: &[RatioDetail],
    emission: &Emission,
    unit_factor: f64,
    tables: &NonroadWorkerTables,
    produced: &mut BTreeMap<i32, Emission>,
) {
    for detail in details {
        if tables.is_pol_process_needed(detail.pol_process_id) {
            produced.insert(
                detail.pollutant_id,
                emission.scaled(detail.ratio * unit_factor),
            );
        }
    }
}

/// The Nonroad air-toxics lookup tables and the two calculation passes — the
/// in-memory state, `calculate` and `calculateNonHAPTOG` bodies of the Go
/// `nrairtoxics` package.
#[derive(Debug, Clone, Default)]
pub struct NrAirToxics {
    /// `nrATRatio` — gaseous-toxic ratios, keyed by fuel subtype.
    at_ratio: HashMap<AtRatioKey, Vec<RatioDetail>>,
    /// `nrATRatioProcesses` — the set of process ids present in `nrATRatio`.
    at_ratio_processes: HashSet<i32>,
    /// `nrPAHGasRatio` — gaseous-PAH ratios, keyed by fuel type.
    pah_gas_ratio: HashMap<ProcFuelEngHpKey, Vec<RatioDetail>>,
    /// `nrPAHGasRatioProcesses` — process ids present in `nrPAHGasRatio`.
    pah_gas_ratio_processes: HashSet<i32>,
    /// `nrPAHParticleRatio` — particulate-PAH ratios, keyed by fuel type.
    pah_particle_ratio: HashMap<ProcFuelEngHpKey, Vec<RatioDetail>>,
    /// `nrPAHParticleRatioProcesses` — process ids in `nrPAHParticleRatio`.
    pah_particle_ratio_processes: HashSet<i32>,
    /// `nrDioxinEmissionRate` — dioxin/furan per-gallon rates, keyed by fuel
    /// type.
    dioxin_emission_rate: HashMap<ProcFuelEngHpKey, Vec<RatioDetail>>,
    /// `nrDioxinEmissionRateProcesses` — process ids in
    /// `nrDioxinEmissionRate`.
    dioxin_emission_rate_processes: HashSet<i32>,
    /// `nrMetalEmissionRate` — metallic-toxic per-gallon rates, keyed by fuel
    /// type.
    metal_emission_rate: HashMap<ProcFuelEngHpKey, Vec<RatioDetail>>,
    /// `nrMetalEmissionRateProcesses` — process ids in `nrMetalEmissionRate`.
    metal_emission_rate_processes: HashSet<i32>,
    /// `nrIntegratedSpecies` — pollutant ids subtracted from NMOG to form
    /// NonHAPTOG.
    integrated_species: HashSet<i32>,
}

impl NrAirToxics {
    /// Build the lookup tables from the six air-toxics table extracts — the
    /// in-memory half of the Go `StartSetup`.
    ///
    /// Each ratio table maps a key to a list of details, one per output
    /// pollutant; when several rows share a key the Go appends them in file
    /// order and this port preserves that order. The per-table process sets
    /// (`nrATRatioProcesses` etc.) are derived here from the rows, as the Go
    /// derives them inside `StartSetup`.
    ///
    /// The four `ProcFuelEngHp`-keyed tables share the [`ProcFuelEngHpRow`]
    /// shape; `integrated_species` is the `nrIntegratedSpecies` pollutant id
    /// list.
    #[must_use]
    pub fn build(
        at_ratio_rows: impl IntoIterator<Item = AtRatioRow>,
        pah_gas_ratio_rows: impl IntoIterator<Item = ProcFuelEngHpRow>,
        pah_particle_ratio_rows: impl IntoIterator<Item = ProcFuelEngHpRow>,
        dioxin_emission_rate_rows: impl IntoIterator<Item = ProcFuelEngHpRow>,
        metal_emission_rate_rows: impl IntoIterator<Item = ProcFuelEngHpRow>,
        integrated_species: impl IntoIterator<Item = i32>,
    ) -> Self {
        let mut at_ratio: HashMap<AtRatioKey, Vec<RatioDetail>> = HashMap::new();
        let mut at_ratio_processes = HashSet::new();
        for row in at_ratio_rows {
            at_ratio
                .entry(AtRatioKey {
                    process_id: row.process_id,
                    eng_tech_id: row.eng_tech_id,
                    fuel_sub_type_id: row.fuel_sub_type_id,
                    nr_hp_category: row.nr_hp_category,
                })
                .or_default()
                .push(RatioDetail {
                    pollutant_id: row.pollutant_id,
                    pol_process_id: row.pollutant_id * 100 + row.process_id,
                    ratio: row.at_ratio,
                });
            at_ratio_processes.insert(row.process_id);
        }

        let (pah_gas_ratio, pah_gas_ratio_processes) = index_proc_fuel_eng_hp(pah_gas_ratio_rows);
        let (pah_particle_ratio, pah_particle_ratio_processes) =
            index_proc_fuel_eng_hp(pah_particle_ratio_rows);
        let (dioxin_emission_rate, dioxin_emission_rate_processes) =
            index_proc_fuel_eng_hp(dioxin_emission_rate_rows);
        let (metal_emission_rate, metal_emission_rate_processes) =
            index_proc_fuel_eng_hp(metal_emission_rate_rows);

        Self {
            at_ratio,
            at_ratio_processes,
            pah_gas_ratio,
            pah_gas_ratio_processes,
            pah_particle_ratio,
            pah_particle_ratio_processes,
            dioxin_emission_rate,
            dioxin_emission_rate_processes,
            metal_emission_rate,
            metal_emission_rate_processes,
            integrated_species: integrated_species.into_iter().collect(),
        }
    }

    /// Derive the air-toxics emissions from one input [`Emission`] — the inner
    /// per-emission body of the Go `calculate`.
    ///
    /// `block_key` is the key of the fuel block the emission belongs to; the
    /// calculator reads `pollutant_id`, `process_id`, `eng_tech_id`,
    /// `fuel_type_id` and `hp_id` from it. Which ratio tables are consulted
    /// depends on `block_key.pollutant_id`:
    ///
    /// * VOC (87) — `nrATRatio` then `nrPAHGasRatio`;
    /// * PM2.5 (110) — `nrPAHParticleRatio`;
    /// * fuel consumption (99), running exhaust only — `nrDioxinEmissionRate`
    ///   then `nrMetalEmissionRate`, each scaled by the [`gallons_factor`].
    ///
    /// Any other pollutant produces no output. The block-level pollutant /
    /// process filter is applied by [`air_toxics_block`]; calling this
    /// directly is meaningful only for a VOC, PM2.5 or fuel-consumption block.
    ///
    /// Returns `None` when the emission's fuel formulation is unknown — the Go
    /// `ff == nil`, which makes `calculate` skip the emission entirely, even
    /// on the PM2.5 and fuel-consumption paths that never read the fuel
    /// subtype (see the module-level fidelity note). A returned `Vec` holds
    /// the produced `(pollutant_id, emission)` pairs in ascending
    /// pollutant-id order, and is empty when the run needs none of the
    /// pollutants the matching ratio rows produce.
    ///
    /// [`air_toxics_block`]: Self::air_toxics_block
    #[must_use]
    pub fn air_toxics_for_emission(
        &self,
        block_key: &FuelBlockKey,
        emission: &Emission,
        tables: &NonroadWorkerTables,
    ) -> Option<Vec<(i32, Emission)>> {
        // Go: ff := mwo.FuelFormulations[e.FuelFormulationID]; if ff == nil { continue }.
        // This precedes every pollutant branch, so an unknown formulation
        // skips the whole emission regardless of which branch would run.
        let formulation_fuel_sub_type_id = tables.fuel_sub_type_id(emission.fuel_formulation_id)?;

        // Go: hpCategory := mwo.NRHPCategory[NRHPCategoryKey{HPID, EngTechID}].
        let nr_hp_category = tables.hp_category(block_key.hp_id, block_key.eng_tech_id);
        let process_id = block_key.process_id;

        // The produced emissions, keyed by output pollutant. A BTreeMap keeps
        // the result in ascending pollutant-id order and reproduces the Go's
        // last-write-wins on a repeated pollutant.
        let mut produced: BTreeMap<i32, Emission> = BTreeMap::new();

        match block_key.pollutant_id {
            VOC_POLLUTANT_ID => {
                // nrATRatio — keyed on the *formulation's* fuel subtype.
                if let Some(details) = self.at_ratio.get(&AtRatioKey {
                    process_id,
                    eng_tech_id: block_key.eng_tech_id,
                    fuel_sub_type_id: formulation_fuel_sub_type_id,
                    nr_hp_category,
                }) {
                    apply_ratio_details(details, emission, 1.0, tables, &mut produced);
                }
                // nrPAHGasRatio — keyed on the *block's* fuel type. Filled
                // after nrATRatio, so it wins any shared output pollutant.
                if let Some(details) = self.pah_gas_ratio.get(&ProcFuelEngHpKey {
                    process_id,
                    fuel_type_id: block_key.fuel_type_id,
                    eng_tech_id: block_key.eng_tech_id,
                    nr_hp_category,
                }) {
                    apply_ratio_details(details, emission, 1.0, tables, &mut produced);
                }
            }
            PM25_POLLUTANT_ID => {
                // nrPAHParticleRatio — keyed on the block's fuel type.
                if let Some(details) = self.pah_particle_ratio.get(&ProcFuelEngHpKey {
                    process_id,
                    fuel_type_id: block_key.fuel_type_id,
                    eng_tech_id: block_key.eng_tech_id,
                    nr_hp_category,
                }) {
                    apply_ratio_details(details, emission, 1.0, tables, &mut produced);
                }
            }
            FUEL_CONSUMPTION_POLLUTANT_ID if process_id == RUNNING_EXHAUST_PROCESS_ID => {
                // Fuel consumption is in grams but the dioxin/metal rates are
                // per gallon, so both paths scale by the grams→gallons factor.
                let gallons = gallons_factor(block_key.fuel_type_id);
                let key = ProcFuelEngHpKey {
                    process_id,
                    fuel_type_id: block_key.fuel_type_id,
                    eng_tech_id: block_key.eng_tech_id,
                    nr_hp_category,
                };
                // nrDioxinEmissionRate, then nrMetalEmissionRate — metal wins
                // any shared output pollutant.
                if let Some(details) = self.dioxin_emission_rate.get(&key) {
                    apply_ratio_details(details, emission, gallons, tables, &mut produced);
                }
                if let Some(details) = self.metal_emission_rate.get(&key) {
                    apply_ratio_details(details, emission, gallons, tables, &mut produced);
                }
            }
            // Any other pollutant: no air-toxics output.
            _ => {}
        }

        Some(produced.into_iter().collect())
    }

    /// Derive the air-toxics output blocks from one input fuel block — the
    /// Go `calculate`'s per-`FuelBlock` body.
    ///
    /// A block whose pollutant is not a usable input, or whose
    /// `(pollutant, process)` has no ratio rows, yields no output (the Go's
    /// block-level `continue` filter):
    ///
    /// * a block whose pollutant is not VOC (87), PM2.5 (110) or
    ///   fuel-consumption (99) is skipped;
    /// * a fuel-consumption block whose process is not running exhaust is
    ///   skipped;
    /// * a VOC block whose process appears in neither `nrATRatio` nor
    ///   `nrPAHGasRatio` is skipped;
    /// * a PM2.5 block whose process is absent from `nrPAHParticleRatio` is
    ///   skipped;
    /// * a fuel-consumption block whose process is absent from both
    ///   `nrDioxinEmissionRate` and `nrMetalEmissionRate` is skipped.
    ///
    /// Otherwise each emission is run through
    /// [`air_toxics_for_emission`](Self::air_toxics_for_emission) and the
    /// resulting toxic emissions are grouped into [`ToxicFuelBlock`]s by
    /// pollutant — the emissions within a block keep input-emission order, and
    /// the blocks are returned in ascending pollutant-id order (see the
    /// module-level fidelity note on output order).
    #[must_use]
    pub fn air_toxics_block(
        &self,
        block: &FuelBlock,
        tables: &NonroadWorkerTables,
    ) -> Vec<ToxicFuelBlock> {
        let pollutant_id = block.key.pollutant_id;
        let process_id = block.key.process_id;

        // Go block-level filter: only VOC, PM2.5 or running-exhaust fuel
        // consumption are usable inputs, and only when the process has rows
        // in a relevant ratio table.
        let usable = match pollutant_id {
            VOC_POLLUTANT_ID => {
                self.at_ratio_processes.contains(&process_id)
                    || self.pah_gas_ratio_processes.contains(&process_id)
            }
            PM25_POLLUTANT_ID => self.pah_particle_ratio_processes.contains(&process_id),
            FUEL_CONSUMPTION_POLLUTANT_ID if process_id == RUNNING_EXHAUST_PROCESS_ID => {
                self.metal_emission_rate_processes.contains(&process_id)
                    || self.dioxin_emission_rate_processes.contains(&process_id)
            }
            _ => false,
        };
        if !usable {
            return Vec::new();
        }

        // Group the toxic emissions by output pollutant. A BTreeMap keeps the
        // output blocks in ascending pollutant-id order; each Vec keeps
        // input-emission order.
        let mut by_pollutant: BTreeMap<i32, Vec<Emission>> = BTreeMap::new();
        for emission in &block.emissions {
            let Some(produced) = self.air_toxics_for_emission(&block.key, emission, tables) else {
                continue;
            };
            for (toxic_id, toxic_emission) in produced {
                by_pollutant
                    .entry(toxic_id)
                    .or_default()
                    .push(toxic_emission);
            }
        }

        by_pollutant
            .into_iter()
            .map(|(toxic_id, emissions)| ToxicFuelBlock {
                pollutant_id: toxic_id,
                pol_process_id: toxic_id * 100 + process_id,
                emissions,
            })
            .collect()
    }

    /// Derive the NonHAPTOG partial-contribution block from one input fuel
    /// block — the Go `calculateNonHAPTOG`'s per-`FuelBlock` body.
    ///
    /// NonHAPTOG (pollutant 88) is `NMOG − Σ(integrated species)`. This pass
    /// produces one *partial* contribution per input block:
    ///
    /// * an NMOG (80) block contributes every emission scaled by `+1`;
    /// * an `nrIntegratedSpecies` block contributes every emission scaled by
    ///   `-1`.
    ///
    /// The full NonHAPTOG total is the sum of those partials, formed
    /// downstream when the per-pollutant blocks are aggregated.
    ///
    /// Returns `None` when the block produces no contribution — the Go's
    /// block-level `continue` filter:
    ///
    /// * NonHAPTOG (`88 * 100 + processID`) is not in the run's needed set; or
    /// * the block's pollutant is neither NMOG nor an integrated species; or
    /// * the block has no emissions.
    ///
    /// Otherwise the returned [`ToxicFuelBlock`] carries pollutant 88 and one
    /// scaled emission per input emission, in input-emission order.
    #[must_use]
    pub fn non_hap_tog_block(
        &self,
        block: &FuelBlock,
        tables: &NonroadWorkerTables,
    ) -> Option<ToxicFuelBlock> {
        let process_id = block.key.process_id;

        // Go: ppid := 88*100 + processID; if !NeededPolProcessIDs[ppid] { continue }.
        let pol_process_id = NON_HAP_TOG_POLLUTANT_ID * 100 + process_id;
        if !tables.is_pol_process_needed(pol_process_id) {
            return None;
        }

        // Go: skip any pollutant that is neither NMOG (80) nor an integrated
        // species.
        let is_nmog = block.key.pollutant_id == NMOG_POLLUTANT_ID;
        if !is_nmog && !self.integrated_species.contains(&block.key.pollutant_id) {
            return None;
        }

        // NMOG contributes +NMOG; an integrated species contributes -itself.
        let factor = if is_nmog { 1.0 } else { -1.0 };
        let emissions: Vec<Emission> = block.emissions.iter().map(|e| e.scaled(factor)).collect();
        if emissions.is_empty() {
            return None;
        }

        Some(ToxicFuelBlock {
            pollutant_id: NON_HAP_TOG_POLLUTANT_ID,
            pol_process_id,
            emissions,
        })
    }
}

/// Index a `ProcFuelEngHp`-keyed ratio table — shared by `nrPAHGasRatio`,
/// `nrPAHParticleRatio`, `nrDioxinEmissionRate` and `nrMetalEmissionRate`,
/// which have the same key and column layout.
///
/// Returns the key→details map and the set of process ids present, the Go's
/// `NR…Ratio` map and `NR…RatioProcesses` set. Rows sharing a key are kept in
/// iteration (file) order.
fn index_proc_fuel_eng_hp(
    rows: impl IntoIterator<Item = ProcFuelEngHpRow>,
) -> (HashMap<ProcFuelEngHpKey, Vec<RatioDetail>>, HashSet<i32>) {
    let mut map: HashMap<ProcFuelEngHpKey, Vec<RatioDetail>> = HashMap::new();
    let mut processes = HashSet::new();
    for row in rows {
        map.entry(ProcFuelEngHpKey {
            process_id: row.process_id,
            fuel_type_id: row.fuel_type_id,
            eng_tech_id: row.eng_tech_id,
            nr_hp_category: row.nr_hp_category,
        })
        .or_default()
        .push(RatioDetail {
            pollutant_id: row.pollutant_id,
            pol_process_id: row.pollutant_id * 100 + row.process_id,
            ratio: row.ratio,
        });
        processes.insert(row.process_id);
    }
    (map, processes)
}

/// `(pollutant, process)` registration helper — keeps [`REGISTRATION_GROUPS`]
/// readable.
const fn reg(pollutant: u16, process: u16) -> PollutantProcessAssociation {
    PollutantProcessAssociation {
        pollutant_id: PollutantId(pollutant),
        process_id: ProcessId(process),
    }
}

/// The air-toxics pollutants `NRAirToxicsCalculator` registers, grouped by the
/// nonroad process that emits them.
///
/// The 12 processes are running and crankcase exhaust, the two refueling
/// losses, the evaporative permeation processes (including the three
/// recreational-marine hose-permeation processes), and the three fuel-vapor
/// venting processes.
///
/// The canonical source for these pairs is the `Registration` directives for
/// `NRAirToxicsCalculator` in `CalculatorInfo.txt` at the MOVES source pin —
/// not the Java constructor's `register(...)` loop, which can over- or
/// under-count against the runtime registry. The flattened pair count is
/// [`REGISTRATION_COUNT`] and reconciles with both the 205 `Registration`
/// rows and `registrations_count: 205` for `NRAirToxicsCalculator` in
/// `characterization/calculator-chains/calculator-dag.json`.
const REGISTRATION_GROUPS: &[(u16, &[u16])] = &[
    // Running Exhaust (1) — the full set: aromatics, aldehydes, metals,
    // dioxins/furans, gaseous and particulate PAH, and NonHAPTOG.
    (
        1,
        &[
            20, 21, 22, 23, 24, 25, 26, 27, 40, 41, 42, 43, 44, 45, 46, 60, 61, 62, 63, 65, 66, 67,
            68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 81, 82, 83, 84, 88, 130, 131, 132, 133,
            134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 168, 169, 170, 171,
            172, 173, 174, 175, 176, 177, 178, 181, 182, 183, 184, 185,
        ],
    ),
    // Crankcase Running Exhaust (15) — no metals or dioxins/furans.
    (
        15,
        &[
            20, 21, 22, 23, 24, 25, 26, 27, 40, 41, 42, 43, 44, 45, 46, 68, 69, 70, 71, 72, 73, 74,
            75, 76, 77, 78, 81, 82, 83, 84, 88, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177,
            178, 181, 182, 183, 184, 185,
        ],
    ),
    // Refueling Displacement Vapor Loss (18).
    (18, &[20, 21, 22, 40, 41, 42, 45, 46, 88]),
    // Refueling Spillage Loss (19).
    (19, &[20, 21, 22, 40, 41, 42, 45, 46, 88]),
    // Evap Tank Permeation (20).
    (20, &[20, 21, 22, 40, 41, 42, 45, 46, 88]),
    // Evap Hose Permeation (21).
    (21, &[20, 21, 22, 40, 41, 42, 45, 46, 88]),
    // Evap RecMar Neck Hose Permeation (22) — no NonHAPTOG.
    (22, &[20, 21, 22, 40, 41, 42, 45, 46]),
    // Evap RecMar Supply/Return Hose Permeation (23) — no NonHAPTOG.
    (23, &[20, 21, 22, 40, 41, 42, 45, 46]),
    // Evap RecMar Vent Hose Permeation (24) — no NonHAPTOG.
    (24, &[20, 21, 22, 40, 41, 42, 45, 46]),
    // Diurnal Fuel Vapor Venting (30).
    (30, &[20, 21, 22, 40, 41, 42, 45, 46, 88]),
    // HotSoak Fuel Vapor Venting (31).
    (31, &[20, 21, 22, 40, 41, 42, 45, 46, 88]),
    // RunningLoss Fuel Vapor Venting (32).
    (32, &[20, 21, 22, 40, 41, 42, 45, 46, 88]),
];

/// The number of `(pollutant, process)` pairs across [`REGISTRATION_GROUPS`]
/// — the length of [`REGISTRATIONS`]. Expected to be 205.
const REGISTRATION_COUNT: usize = {
    let mut count = 0;
    let mut i = 0;
    while i < REGISTRATION_GROUPS.len() {
        count += REGISTRATION_GROUPS[i].1.len();
        i += 1;
    }
    count
};

/// The flattened `(pollutant, process)` pairs `NRAirToxicsCalculator`
/// registers — [`REGISTRATION_GROUPS`] expanded so [`Calculator::registrations`]
/// can hand back one contiguous slice.
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

/// `NRAirToxicsCalculator` declares no master-loop subscription of its own;
/// see the [`Calculator::subscriptions`] impl.
static NO_SUBSCRIPTIONS: &[CalculatorSubscription] = &[];

/// Upstream modules — `NRAirToxicsCalculator` chains to
/// `NRHCSpeciationCalculator` (which produces the NMOG it feeds into the
/// NonHAPTOG pass) and `NonroadEmissionCalculator` (which produces the VOC,
/// PM2.5 and fuel-consumption tallies it scales). Matches the two `Chain`
/// directives for `NRAirToxicsCalculator` in `CalculatorInfo.txt` at the
/// MOVES source pin, and `depends_on` in `calculator-dag.json`.
static UPSTREAM: &[&str] = &["NRHCSpeciationCalculator", "NonroadEmissionCalculator"];

/// Default-DB tables the calculator's SQL extracts — the six air-toxics
/// tables `database/NRAirToxicsCalculator.sql` creates and caches. The
/// calculation also consults the shared nonroad worker tables
/// `FuelFormulation` and `nrHPCategory`, which other calculators load.
static INPUT_TABLES: &[&str] = &[
    "nrATRatio",
    "nrDioxinEmissionRate",
    "nrIntegratedSpecies",
    "nrMetalEmissionRate",
    "nrPAHGasRatio",
    "nrPAHParticleRatio",
];

// ===========================================================================
// TableRow implementations — typed DataFrame ↔ row round-trips.
// ===========================================================================

fn row_err(table: &'static str, row: usize, column: &'static str, msg: String) -> Error {
    Error::RowExtraction {
        table: table.into(),
        row,
        column: column.into(),
        message: msg,
    }
}

impl TableRow for AtRatioRow {
    fn table_name() -> &'static str {
        "nrATRatio"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("pollutantID".into(), DataType::Int32),
            ("processID".into(), DataType::Int32),
            ("engTechID".into(), DataType::Int32),
            ("fuelSubtypeID".into(), DataType::Int32),
            ("nrHPCategory".into(), DataType::Int32),
            ("atRatio".into(), DataType::Float64),
        ])
    }

    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
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
                    "engTechID".into(),
                    rows.iter().map(|r| r.eng_tech_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "fuelSubtypeID".into(),
                    rows.iter()
                        .map(|r| r.fuel_sub_type_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "nrHPCategory".into(),
                    rows.iter()
                        .map(|r| r.nr_hp_category as i32)
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
        let t = "nrATRatio";
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
        let pollutant = get_i32("pollutantID")?;
        let process = get_i32("processID")?;
        let eng_tech = get_i32("engTechID")?;
        let fuel_sub = get_i32("fuelSubtypeID")?;
        let hp_cat = get_i32("nrHPCategory")?;
        let ratio = get_f64("atRatio")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(AtRatioRow {
                    pollutant_id: pollutant.get(i).ok_or_else(|| null("pollutantID"))?,
                    process_id: process.get(i).ok_or_else(|| null("processID"))?,
                    eng_tech_id: eng_tech.get(i).ok_or_else(|| null("engTechID"))?,
                    fuel_sub_type_id: fuel_sub.get(i).ok_or_else(|| null("fuelSubtypeID"))?,
                    nr_hp_category: hp_cat.get(i).ok_or_else(|| null("nrHPCategory"))? as u8,
                    at_ratio: ratio.get(i).ok_or_else(|| null("atRatio"))?,
                })
            })
            .collect()
    }
}

impl TableRow for ProcFuelEngHpRow {
    fn table_name() -> &'static str {
        "nrPAHGasRatio"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("pollutantID".into(), DataType::Int32),
            ("processID".into(), DataType::Int32),
            ("fuelTypeID".into(), DataType::Int32),
            ("engTechID".into(), DataType::Int32),
            ("nrHPCategory".into(), DataType::Int32),
            ("ratio".into(), DataType::Float64),
        ])
    }

    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
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
                    "fuelTypeID".into(),
                    rows.iter().map(|r| r.fuel_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "engTechID".into(),
                    rows.iter().map(|r| r.eng_tech_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "nrHPCategory".into(),
                    rows.iter()
                        .map(|r| r.nr_hp_category as i32)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "ratio".into(),
                    rows.iter().map(|r| r.ratio).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "nrPAHGasRatio";
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
        let pollutant = get_i32("pollutantID")?;
        let process = get_i32("processID")?;
        let fuel_type = get_i32("fuelTypeID")?;
        let eng_tech = get_i32("engTechID")?;
        let hp_cat = get_i32("nrHPCategory")?;
        let ratio = get_f64("ratio")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(ProcFuelEngHpRow {
                    pollutant_id: pollutant.get(i).ok_or_else(|| null("pollutantID"))?,
                    process_id: process.get(i).ok_or_else(|| null("processID"))?,
                    fuel_type_id: fuel_type.get(i).ok_or_else(|| null("fuelTypeID"))?,
                    eng_tech_id: eng_tech.get(i).ok_or_else(|| null("engTechID"))?,
                    nr_hp_category: hp_cat.get(i).ok_or_else(|| null("nrHPCategory"))? as u8,
                    ratio: ratio.get(i).ok_or_else(|| null("ratio"))?,
                })
            })
            .collect()
    }
}

/// One `nrIntegratedSpecies` row — a pollutant id subtracted from NMOG to
/// form NonHAPTOG.
struct NrIntegratedSpeciesRow {
    pollutant_id: i32,
}

impl TableRow for NrIntegratedSpeciesRow {
    fn table_name() -> &'static str {
        "nrIntegratedSpecies"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([("pollutantID".into(), DataType::Int32)])
    }

    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![Series::new(
                "pollutantID".into(),
                rows.iter().map(|r| r.pollutant_id).collect::<Vec<i32>>(),
            )
            .into()],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "nrIntegratedSpecies";
        let pollutant = df
            .column("pollutantID")
            .map_err(|e| row_err(t, 0, "pollutantID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "pollutantID", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                Ok(NrIntegratedSpeciesRow {
                    pollutant_id: pollutant
                        .get(i)
                        .ok_or_else(|| row_err(t, i, "pollutantID", "null value".into()))?,
                })
            })
            .collect()
    }
}

/// One `FuelFormulation` row — maps a fuel formulation to its subtype.
struct FuelFormulationRow {
    fuel_formulation_id: i32,
    fuel_sub_type_id: i32,
}

impl TableRow for FuelFormulationRow {
    fn table_name() -> &'static str {
        "FuelFormulation"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("fuelFormulationID".into(), DataType::Int32),
            ("fuelSubTypeID".into(), DataType::Int32),
        ])
    }

    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "fuelFormulationID".into(),
                    rows.iter()
                        .map(|r| r.fuel_formulation_id)
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
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "FuelFormulation";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let form = get_i32("fuelFormulationID")?;
        let sub = get_i32("fuelSubTypeID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(FuelFormulationRow {
                    fuel_formulation_id: form.get(i).ok_or_else(|| null("fuelFormulationID"))?,
                    fuel_sub_type_id: sub.get(i).ok_or_else(|| null("fuelSubTypeID"))?,
                })
            })
            .collect()
    }
}

/// One `NRHPCategory` row — maps `(hpID, engTechID)` to a horse-power
/// category byte.
struct NrHpCategoryRow {
    hp_id: i32,
    eng_tech_id: i32,
    nr_hp_category: i32,
}

impl TableRow for NrHpCategoryRow {
    fn table_name() -> &'static str {
        "NRHPCategory"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("hpID".into(), DataType::Int32),
            ("engTechID".into(), DataType::Int32),
            ("nrHPCategory".into(), DataType::Int32),
        ])
    }

    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "hpID".into(),
                    rows.iter().map(|r| r.hp_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "engTechID".into(),
                    rows.iter().map(|r| r.eng_tech_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "nrHPCategory".into(),
                    rows.iter().map(|r| r.nr_hp_category).collect::<Vec<i32>>(),
                )
                .into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "NRHPCategory";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let hp = get_i32("hpID")?;
        let eng = get_i32("engTechID")?;
        let cat = get_i32("nrHPCategory")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(NrHpCategoryRow {
                    hp_id: hp.get(i).ok_or_else(|| null("hpID"))?,
                    eng_tech_id: eng.get(i).ok_or_else(|| null("engTechID"))?,
                    nr_hp_category: cat.get(i).ok_or_else(|| null("nrHPCategory"))?,
                })
            })
            .collect()
    }
}

/// One `NeededPolProcessIDs` row — a `polProcessID` the run needs output for.
struct NrNeededPolProcessRow {
    pol_process_id: i32,
}

impl TableRow for NrNeededPolProcessRow {
    fn table_name() -> &'static str {
        "NeededPolProcessIDs"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([("polProcessID".into(), DataType::Int32)])
    }

    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![Series::new(
                "polProcessID".into(),
                rows.iter().map(|r| r.pol_process_id).collect::<Vec<i32>>(),
            )
            .into()],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "NeededPolProcessIDs";
        let ppid = df
            .column("polProcessID")
            .map_err(|e| row_err(t, 0, "polProcessID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "polProcessID", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                Ok(NrNeededPolProcessRow {
                    pol_process_id: ppid
                        .get(i)
                        .ok_or_else(|| row_err(t, i, "polProcessID", "null value".into()))?,
                })
            })
            .collect()
    }
}

/// One `MOVESWorkerOutput` row as read/written by `NRAirToxicsCalculator`.
///
/// Includes the nonroad-specific `engTechID` and `hpID` columns in addition
/// to the standard worker-output fields.
struct NrAirToxicsMwoRow {
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
    fuel_type_id: i32,
    eng_tech_id: i32,
    hp_id: i32,
    model_year_id: i32,
    road_type_id: i32,
    emission_quant: f64,
    emission_rate: f64,
}

impl TableRow for NrAirToxicsMwoRow {
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
            ("fuelTypeID".into(), DataType::Int32),
            ("engTechID".into(), DataType::Int32),
            ("hpID".into(), DataType::Int32),
            ("modelYearID".into(), DataType::Int32),
            ("roadTypeID".into(), DataType::Int32),
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
                    "fuelTypeID".into(),
                    rows.iter().map(|r| r.fuel_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "engTechID".into(),
                    rows.iter().map(|r| r.eng_tech_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "hpID".into(),
                    rows.iter().map(|r| r.hp_id).collect::<Vec<i32>>(),
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
        let fuel_type = get_i32("fuelTypeID")?;
        let eng_tech = get_i32("engTechID")?;
        let hp = get_i32("hpID")?;
        let model_year = get_i32("modelYearID")?;
        let road_type = get_i32("roadTypeID")?;
        let emission_quant = get_f64("emissionQuant")?;
        let emission_rate = get_f64("emissionRate")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(NrAirToxicsMwoRow {
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
                    fuel_type_id: fuel_type.get(i).ok_or_else(|| null("fuelTypeID"))?,
                    eng_tech_id: eng_tech.get(i).ok_or_else(|| null("engTechID"))?,
                    hp_id: hp.get(i).ok_or_else(|| null("hpID"))?,
                    model_year_id: model_year.get(i).ok_or_else(|| null("modelYearID"))?,
                    road_type_id: road_type.get(i).ok_or_else(|| null("roadTypeID"))?,
                    emission_quant: emission_quant.get(i).ok_or_else(|| null("emissionQuant"))?,
                    emission_rate: emission_rate.get(i).ok_or_else(|| null("emissionRate"))?,
                })
            })
            .collect()
    }
}

/// `NRAirToxicsCalculator` as a chain-DAG [`Calculator`].
///
/// The numerically faithful work lives on [`NrAirToxics`]; this zero-sized
/// type carries the calculator's chain metadata —
/// [`name`](Calculator::name), [`registrations`](Calculator::registrations),
/// [`upstream`](Calculator::upstream) — so the registry can wire it into the
/// calculator chain.
#[derive(Debug, Clone, Copy, Default)]
pub struct NrAirToxicsCalculator;

impl NrAirToxicsCalculator {
    /// Chain-DAG name — matches the Java class / Go package and the
    /// `calculator-dag.json` entry.
    pub const NAME: &'static str = "NRAirToxicsCalculator";

    /// Construct the calculator.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl Calculator for NrAirToxicsCalculator {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    /// `NRAirToxicsCalculator` carries no master-loop subscription of its own:
    /// `calculator-dag.json` records `subscribes_directly: false`. It is a
    /// chained calculator — it runs when the calculators it chains to (its
    /// [`upstream`](Calculator::upstream) modules) run, deriving the toxics
    /// from their VOC / PM2.5 / fuel-consumption / NMOG output.
    fn subscriptions(&self) -> &[CalculatorSubscription] {
        NO_SUBSCRIPTIONS
    }

    fn registrations(&self) -> &[PollutantProcessAssociation] {
        &REGISTRATIONS
    }

    fn upstream(&self) -> &[&'static str] {
        UPSTREAM
    }

    fn input_tables(&self) -> &[&'static str] {
        INPUT_TABLES
    }

    fn execute(&self, ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
        let tables = ctx.tables();
        let nr_air_toxics = NrAirToxics::build(
            tables.iter_typed::<AtRatioRow>("nrATRatio")?,
            tables.iter_typed::<ProcFuelEngHpRow>("nrPAHGasRatio")?,
            tables.iter_typed::<ProcFuelEngHpRow>("nrPAHParticleRatio")?,
            tables.iter_typed::<ProcFuelEngHpRow>("nrDioxinEmissionRate")?,
            tables.iter_typed::<ProcFuelEngHpRow>("nrMetalEmissionRate")?,
            tables
                .iter_typed::<NrIntegratedSpeciesRow>("nrIntegratedSpecies")?
                .into_iter()
                .map(|r| r.pollutant_id),
        );
        let worker_tables = NonroadWorkerTables::new(
            tables
                .iter_typed::<FuelFormulationRow>("FuelFormulation")?
                .into_iter()
                .map(|r| (r.fuel_formulation_id, r.fuel_sub_type_id)),
            tables
                .iter_typed::<NrHpCategoryRow>("NRHPCategory")?
                .into_iter()
                .map(|r| ((r.hp_id, r.eng_tech_id), r.nr_hp_category as u8)),
            tables
                .iter_typed::<NrNeededPolProcessRow>("NeededPolProcessIDs")?
                .into_iter()
                .map(|r| r.pol_process_id),
        );
        let input_rows: Vec<NrAirToxicsMwoRow> = tables.iter_typed("MOVESWorkerOutput")?;
        let mut output_rows: Vec<NrAirToxicsMwoRow> = Vec::new();
        for row in &input_rows {
            let block = FuelBlock {
                key: FuelBlockKey {
                    pollutant_id: row.pollutant_id,
                    process_id: row.process_id,
                    eng_tech_id: row.eng_tech_id,
                    fuel_type_id: row.fuel_type_id,
                    hp_id: row.hp_id,
                },
                emissions: vec![Emission {
                    fuel_sub_type_id: 0,
                    fuel_formulation_id: 0,
                    emission_quant: row.emission_quant,
                    emission_rate: row.emission_rate,
                }],
            };
            for tblock in nr_air_toxics.air_toxics_block(&block, &worker_tables) {
                for emission in &tblock.emissions {
                    output_rows.push(NrAirToxicsMwoRow {
                        pollutant_id: tblock.pollutant_id,
                        process_id: row.process_id,
                        year_id: row.year_id,
                        month_id: row.month_id,
                        day_id: row.day_id,
                        hour_id: row.hour_id,
                        state_id: row.state_id,
                        county_id: row.county_id,
                        zone_id: row.zone_id,
                        link_id: row.link_id,
                        source_type_id: row.source_type_id,
                        fuel_type_id: row.fuel_type_id,
                        eng_tech_id: row.eng_tech_id,
                        hp_id: row.hp_id,
                        model_year_id: row.model_year_id,
                        road_type_id: row.road_type_id,
                        emission_quant: emission.emission_quant,
                        emission_rate: emission.emission_rate,
                    });
                }
            }
            if let Some(nhap) = nr_air_toxics.non_hap_tog_block(&block, &worker_tables) {
                for emission in &nhap.emissions {
                    output_rows.push(NrAirToxicsMwoRow {
                        pollutant_id: nhap.pollutant_id,
                        process_id: row.process_id,
                        year_id: row.year_id,
                        month_id: row.month_id,
                        day_id: row.day_id,
                        hour_id: row.hour_id,
                        state_id: row.state_id,
                        county_id: row.county_id,
                        zone_id: row.zone_id,
                        link_id: row.link_id,
                        source_type_id: row.source_type_id,
                        fuel_type_id: row.fuel_type_id,
                        eng_tech_id: row.eng_tech_id,
                        hp_id: row.hp_id,
                        model_year_id: row.model_year_id,
                        road_type_id: row.road_type_id,
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

    /// HP-category code used throughout the tests.
    const CAT: u8 = b'A';

    /// An `nrATRatio` row helper.
    fn at_row(
        pollutant_id: i32,
        process_id: i32,
        eng_tech_id: i32,
        fuel_sub_type_id: i32,
        nr_hp_category: u8,
        at_ratio: f64,
    ) -> AtRatioRow {
        AtRatioRow {
            pollutant_id,
            process_id,
            eng_tech_id,
            fuel_sub_type_id,
            nr_hp_category,
            at_ratio,
        }
    }

    /// A `ProcFuelEngHp`-keyed row helper (PAH / dioxin / metal tables).
    fn pfeh_row(
        pollutant_id: i32,
        process_id: i32,
        fuel_type_id: i32,
        eng_tech_id: i32,
        nr_hp_category: u8,
        ratio: f64,
    ) -> ProcFuelEngHpRow {
        ProcFuelEngHpRow {
            pollutant_id,
            process_id,
            fuel_type_id,
            eng_tech_id,
            nr_hp_category,
            ratio,
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

    /// Worker tables for process 1, fuel formulation 100 → subtype 20, HP id 5
    /// / engTech 10 → category `CAT`, with exactly `pollutants` needed (each
    /// for process 1).
    fn tables_needing(pollutants: &[i32]) -> NonroadWorkerTables {
        NonroadWorkerTables::new(
            [(100, 20)],
            [((5, 10), CAT)],
            pollutants.iter().map(|p| ppid(*p, 1)).collect::<Vec<_>>(),
        )
    }

    /// A VOC (87) fuel-block key: process 1, engTech 10, fuel type 1, HP id 5.
    fn voc_key() -> FuelBlockKey {
        FuelBlockKey {
            pollutant_id: VOC_POLLUTANT_ID,
            process_id: 1,
            eng_tech_id: 10,
            fuel_type_id: 1,
            hp_id: 5,
        }
    }

    #[test]
    fn build_indexes_all_six_tables() {
        let toxics = NrAirToxics::build(
            [at_row(20, 1, 10, 20, CAT, 0.5)],
            [pfeh_row(168, 1, 1, 10, CAT, 0.25)],
            [pfeh_row(23, 1, 1, 10, CAT, 0.1)],
            [pfeh_row(130, 1, 1, 10, CAT, 2.0)],
            [pfeh_row(60, 1, 1, 10, CAT, 3.0)],
            [21, 24],
        );
        assert_eq!(toxics.at_ratio.len(), 1);
        assert_eq!(toxics.pah_gas_ratio.len(), 1);
        assert_eq!(toxics.pah_particle_ratio.len(), 1);
        assert_eq!(toxics.dioxin_emission_rate.len(), 1);
        assert_eq!(toxics.metal_emission_rate.len(), 1);
        assert_eq!(toxics.integrated_species, HashSet::from([21, 24]));
        // Each table records the process ids it carries.
        assert!(toxics.at_ratio_processes.contains(&1));
        assert!(toxics.pah_gas_ratio_processes.contains(&1));
        assert!(toxics.pah_particle_ratio_processes.contains(&1));
        assert!(toxics.dioxin_emission_rate_processes.contains(&1));
        assert!(toxics.metal_emission_rate_processes.contains(&1));
    }

    #[test]
    fn build_computes_pol_process_id_and_keeps_file_order_on_a_shared_key() {
        // Two nrATRatio rows share a key — the Go appends both details.
        let toxics = NrAirToxics::build(
            [
                at_row(20, 1, 10, 20, CAT, 0.5),
                at_row(24, 1, 10, 20, CAT, 0.7),
            ],
            [],
            [],
            [],
            [],
            [],
        );
        let details = toxics
            .at_ratio
            .get(&AtRatioKey {
                process_id: 1,
                eng_tech_id: 10,
                fuel_sub_type_id: 20,
                nr_hp_category: CAT,
            })
            .expect("keyed details");
        assert_eq!(details.len(), 2);
        // File order preserved.
        assert_eq!(details[0].pollutant_id, 20);
        assert_eq!(details[1].pollutant_id, 24);
        // polProcessID = pollutantID * 100 + processID.
        assert_eq!(details[0].pol_process_id, 2001);
        assert_eq!(details[1].pol_process_id, 2401);
    }

    #[test]
    fn gallons_factor_matches_the_go_two_division_order() {
        let grams_to_pounds = 1.0_f64 / 453.592;
        // Gasoline, diesel, CNG, LPG — the Go fuel-density literals.
        assert_eq!(gallons_factor(1), grams_to_pounds / 6.17);
        assert_eq!(gallons_factor(2), grams_to_pounds / 7.1);
        assert_eq!(gallons_factor(3), grams_to_pounds / 0.0061);
        assert_eq!(gallons_factor(4), grams_to_pounds / 4.507);
        // Fuel types 23 and 24 are nonroad diesel variants.
        assert_eq!(gallons_factor(23), gallons_factor(2));
        assert_eq!(gallons_factor(24), gallons_factor(2));
        // Any other fuel type: the identity factor, no conversion.
        assert_eq!(gallons_factor(5), 1.0);
        assert_eq!(gallons_factor(0), 1.0);
    }

    #[test]
    fn emission_scaled_multiplies_both_quant_and_rate() {
        let e = emission(8.0, 4.0, 20, 100);
        assert_eq!(e.scaled(0.5), emission(4.0, 2.0, 20, 100));
        // Fuel ids carry through; scaling by zero still keeps the tags.
        assert_eq!(e.scaled(0.0), emission(0.0, 0.0, 20, 100));
        // Scaling by -1 negates — the NonHAPTOG integrated-species sign.
        assert_eq!(e.scaled(-1.0), emission(-8.0, -4.0, 20, 100));
    }

    #[test]
    fn voc_emission_scales_by_the_at_ratio() {
        let toxics = NrAirToxics::build([at_row(20, 1, 10, 20, CAT, 0.5)], [], [], [], [], []);
        let tables = tables_needing(&[20]);
        let produced = toxics
            .air_toxics_for_emission(&voc_key(), &emission(8.0, 4.0, 20, 100), &tables)
            .expect("formulation known");
        // benzene (20) = VOC * 0.5.
        assert_eq!(produced, vec![(20, emission(4.0, 2.0, 20, 100))]);
    }

    #[test]
    fn voc_emission_scales_by_the_pah_gas_ratio() {
        let toxics = NrAirToxics::build([], [pfeh_row(168, 1, 1, 10, CAT, 0.25)], [], [], [], []);
        let tables = tables_needing(&[168]);
        let produced = toxics
            .air_toxics_for_emission(&voc_key(), &emission(8.0, 4.0, 20, 100), &tables)
            .expect("formulation known");
        // gaseous PAH (168) = VOC * 0.25.
        assert_eq!(produced, vec![(168, emission(2.0, 1.0, 20, 100))]);
    }

    #[test]
    fn voc_emission_combines_at_ratio_and_pah_gas_ratio() {
        let toxics = NrAirToxics::build(
            [at_row(20, 1, 10, 20, CAT, 0.5)],
            [pfeh_row(168, 1, 1, 10, CAT, 0.25)],
            [],
            [],
            [],
            [],
        );
        let tables = tables_needing(&[20, 168]);
        let produced = toxics
            .air_toxics_for_emission(&voc_key(), &emission(8.0, 4.0, 20, 100), &tables)
            .expect("formulation known");
        // Both tables contribute; output is in ascending pollutant-id order.
        assert_eq!(
            produced,
            vec![
                (20, emission(4.0, 2.0, 20, 100)),
                (168, emission(2.0, 1.0, 20, 100)),
            ],
        );
    }

    #[test]
    fn voc_pah_gas_ratio_overwrites_at_ratio_on_a_shared_pollutant() {
        // Both tables tabulate pollutant 20 for the same key; the Go fills
        // nrATRatio first then nrPAHGasRatio, so the PAH gas value wins.
        let toxics = NrAirToxics::build(
            [at_row(20, 1, 10, 20, CAT, 0.5)],
            [pfeh_row(20, 1, 1, 10, CAT, 0.9)],
            [],
            [],
            [],
            [],
        );
        let tables = tables_needing(&[20]);
        let produced = toxics
            .air_toxics_for_emission(&voc_key(), &emission(8.0, 4.0, 20, 100), &tables)
            .expect("formulation known");
        // PAH gas ratio 0.9 wins over the AT ratio 0.5.
        assert_eq!(produced, vec![(20, emission(7.2, 3.6, 20, 100))]);
    }

    #[test]
    fn pm25_emission_scales_by_the_pah_particle_ratio() {
        let toxics = NrAirToxics::build([], [], [pfeh_row(23, 1, 1, 10, CAT, 0.1)], [], [], []);
        let tables = tables_needing(&[23]);
        let pm_key = FuelBlockKey {
            pollutant_id: PM25_POLLUTANT_ID,
            ..voc_key()
        };
        let produced = toxics
            .air_toxics_for_emission(&pm_key, &emission(8.0, 4.0, 20, 100), &tables)
            .expect("formulation known");
        // particulate PAH (23) = PM2.5 * 0.1.
        assert_eq!(produced, vec![(23, emission(0.8, 0.4, 20, 100))]);
    }

    #[test]
    fn fuel_consumption_scales_dioxins_and_metals_by_the_gallons_factor() {
        let toxics = NrAirToxics::build(
            [],
            [],
            [],
            [pfeh_row(130, 1, 1, 10, CAT, 2.0)],
            [pfeh_row(60, 1, 1, 10, CAT, 3.0)],
            [],
        );
        let tables = tables_needing(&[130, 60]);
        let fuel_key = FuelBlockKey {
            pollutant_id: FUEL_CONSUMPTION_POLLUTANT_ID,
            ..voc_key()
        };
        let input = emission(8.0, 4.0, 20, 100);
        let produced = toxics
            .air_toxics_for_emission(&fuel_key, &input, &tables)
            .expect("formulation known");
        // dioxin (130) = fuel * meanBaseRate * gallonsFactor;
        // metal (60)  = fuel * meanBaseRate * gallonsFactor.
        let gallons = gallons_factor(1);
        assert_eq!(
            produced,
            vec![
                (60, input.scaled(3.0 * gallons)),
                (130, input.scaled(2.0 * gallons)),
            ],
        );
    }

    #[test]
    fn fuel_consumption_metal_overwrites_dioxin_on_a_shared_pollutant() {
        // Both the dioxin and metal tables tabulate pollutant 130; the Go
        // fills dioxin first then metal, so the metal value wins.
        let toxics = NrAirToxics::build(
            [],
            [],
            [],
            [pfeh_row(130, 1, 1, 10, CAT, 2.0)],
            [pfeh_row(130, 1, 1, 10, CAT, 9.0)],
            [],
        );
        let tables = tables_needing(&[130]);
        let fuel_key = FuelBlockKey {
            pollutant_id: FUEL_CONSUMPTION_POLLUTANT_ID,
            ..voc_key()
        };
        let input = emission(8.0, 4.0, 20, 100);
        let produced = toxics
            .air_toxics_for_emission(&fuel_key, &input, &tables)
            .expect("formulation known");
        assert_eq!(produced, vec![(130, input.scaled(9.0 * gallons_factor(1)))]);
    }

    #[test]
    fn fuel_consumption_outside_running_exhaust_produces_nothing() {
        let toxics = NrAirToxics::build([], [], [], [pfeh_row(130, 15, 1, 10, CAT, 2.0)], [], []);
        let tables = NonroadWorkerTables::new([(100, 20)], [((5, 10), CAT)], [ppid(130, 15)]);
        // Fuel consumption from crankcase running exhaust (process 15) — the
        // dioxin/metal path runs only for running exhaust (process 1).
        let fuel_key = FuelBlockKey {
            pollutant_id: FUEL_CONSUMPTION_POLLUTANT_ID,
            process_id: 15,
            eng_tech_id: 10,
            fuel_type_id: 1,
            hp_id: 5,
        };
        let produced = toxics
            .air_toxics_for_emission(&fuel_key, &emission(8.0, 4.0, 20, 100), &tables)
            .expect("formulation known");
        assert!(produced.is_empty());
    }

    #[test]
    fn needed_set_gates_which_toxics_are_produced() {
        let toxics = NrAirToxics::build(
            [
                at_row(20, 1, 10, 20, CAT, 0.5),
                at_row(24, 1, 10, 20, CAT, 0.7),
            ],
            [],
            [],
            [],
            [],
            [],
        );
        // Only benzene (20) is needed; 1,3-butadiene (24) is not.
        let tables = tables_needing(&[20]);
        let produced = toxics
            .air_toxics_for_emission(&voc_key(), &emission(8.0, 4.0, 20, 100), &tables)
            .expect("formulation known");
        assert_eq!(produced, vec![(20, emission(4.0, 2.0, 20, 100))]);
    }

    #[test]
    fn emission_with_unknown_fuel_formulation_is_skipped() {
        let toxics = NrAirToxics::build([at_row(20, 1, 10, 20, CAT, 0.5)], [], [], [], [], []);
        // Worker tables that know no fuel formulations at all.
        let tables = NonroadWorkerTables::new([], [((5, 10), CAT)], [ppid(20, 1)]);
        assert!(toxics
            .air_toxics_for_emission(&voc_key(), &emission(8.0, 4.0, 20, 100), &tables)
            .is_none());
    }

    #[test]
    fn unknown_formulation_skips_even_a_pm25_emission() {
        // The PM2.5 path never reads the formulation's fuel subtype, yet the
        // Go's `ff == nil` check still skips the emission.
        let toxics = NrAirToxics::build([], [], [pfeh_row(23, 1, 1, 10, CAT, 0.1)], [], [], []);
        let tables = NonroadWorkerTables::new([], [((5, 10), CAT)], [ppid(23, 1)]);
        let pm_key = FuelBlockKey {
            pollutant_id: PM25_POLLUTANT_ID,
            ..voc_key()
        };
        assert!(toxics
            .air_toxics_for_emission(&pm_key, &emission(8.0, 4.0, 20, 100), &tables)
            .is_none());
    }

    #[test]
    fn at_ratio_keys_on_formulation_subtype_pah_keys_on_block_fuel_type() {
        // The emission's formulation maps to subtype 30, but the emission is
        // tagged subtype 99; the block's fuel type is 1. The nrATRatio lookup
        // must use the formulation's subtype (30); nrPAHGasRatio must use the
        // block's fuel type (1).
        let toxics = NrAirToxics::build(
            [at_row(20, 1, 10, 30, CAT, 0.5)],
            [pfeh_row(168, 1, 1, 10, CAT, 0.25)],
            [],
            [],
            [],
            [],
        );
        let tables = NonroadWorkerTables::new(
            [(100, 30)], // formulation 100 -> subtype 30
            [((5, 10), CAT)],
            [ppid(20, 1), ppid(168, 1)],
        );
        let produced = toxics
            .air_toxics_for_emission(&voc_key(), &emission(8.0, 4.0, 99, 100), &tables)
            .expect("formulation known");
        // Both lookups hit: AT ratio via subtype 30, PAH gas via fuel type 1.
        assert_eq!(
            produced,
            vec![
                (20, emission(4.0, 2.0, 99, 100)),
                (168, emission(2.0, 1.0, 99, 100)),
            ],
        );
    }

    #[test]
    fn missing_hp_category_falls_back_to_zero() {
        // The AT ratio is keyed with HP category 0; the worker tables carry
        // no NRHPCategory entry, so the lookup falls back to 0 and hits.
        let toxics = NrAirToxics::build([at_row(20, 1, 10, 20, 0, 0.5)], [], [], [], [], []);
        let tables = NonroadWorkerTables::new([(100, 20)], [], [ppid(20, 1)]);
        let produced = toxics
            .air_toxics_for_emission(&voc_key(), &emission(8.0, 4.0, 20, 100), &tables)
            .expect("formulation known");
        assert_eq!(produced, vec![(20, emission(4.0, 2.0, 20, 100))]);
    }

    #[test]
    fn non_input_pollutant_produces_nothing() {
        let toxics = NrAirToxics::build([at_row(20, 1, 10, 20, CAT, 0.5)], [], [], [], [], []);
        let tables = tables_needing(&[20]);
        // A THC (1) block is not one of the three usable inputs.
        let thc_key = FuelBlockKey {
            pollutant_id: 1,
            ..voc_key()
        };
        let produced = toxics
            .air_toxics_for_emission(&thc_key, &emission(8.0, 4.0, 20, 100), &tables)
            .expect("formulation known");
        assert!(produced.is_empty());
    }

    #[test]
    fn air_toxics_block_groups_emissions_by_pollutant() {
        let toxics = NrAirToxics::build(
            [
                at_row(20, 1, 10, 20, CAT, 0.5),
                at_row(24, 1, 10, 20, CAT, 0.25),
            ],
            [],
            [],
            [],
            [],
            [],
        );
        let tables = tables_needing(&[20, 24]);
        let block = FuelBlock {
            key: voc_key(),
            emissions: vec![emission(8.0, 4.0, 20, 100)],
        };
        let blocks = toxics.air_toxics_block(&block, &tables);
        let pollutants: Vec<i32> = blocks.iter().map(|b| b.pollutant_id).collect();
        assert_eq!(pollutants, vec![20, 24]);
        for b in &blocks {
            assert_eq!(b.emissions.len(), 1);
            // polProcessID = pollutantID * 100 + processID.
            assert_eq!(b.pol_process_id, b.pollutant_id * 100 + 1);
        }
    }

    #[test]
    fn air_toxics_block_accumulates_multiple_emissions_in_input_order() {
        let toxics = NrAirToxics::build([at_row(20, 1, 10, 20, CAT, 0.5)], [], [], [], [], []);
        let tables = tables_needing(&[20]);
        let block = FuelBlock {
            key: voc_key(),
            emissions: vec![emission(8.0, 4.0, 20, 100), emission(20.0, 10.0, 20, 100)],
        };
        let blocks = toxics.air_toxics_block(&block, &tables);
        assert_eq!(blocks.len(), 1);
        // Both emissions speciated, in input order: 8*0.5 then 20*0.5.
        assert_eq!(
            blocks[0].emissions,
            vec![emission(4.0, 2.0, 20, 100), emission(10.0, 5.0, 20, 100)],
        );
    }

    #[test]
    fn air_toxics_block_skips_an_emission_with_unknown_formulation() {
        let toxics = NrAirToxics::build([at_row(20, 1, 10, 20, CAT, 0.5)], [], [], [], [], []);
        let tables = tables_needing(&[20]);
        // First emission's formulation (100) is known; the second (999) is not.
        let block = FuelBlock {
            key: voc_key(),
            emissions: vec![emission(8.0, 4.0, 20, 100), emission(8.0, 4.0, 20, 999)],
        };
        let blocks = toxics.air_toxics_block(&block, &tables);
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].emissions, vec![emission(4.0, 2.0, 20, 100)]);
    }

    #[test]
    fn air_toxics_block_filters_a_non_input_pollutant() {
        let toxics = NrAirToxics::build([at_row(20, 1, 10, 20, CAT, 0.5)], [], [], [], [], []);
        let tables = tables_needing(&[20]);
        // A THC (1) block — not VOC, PM2.5 or fuel consumption.
        let block = FuelBlock {
            key: FuelBlockKey {
                pollutant_id: 1,
                ..voc_key()
            },
            emissions: vec![emission(8.0, 4.0, 20, 100)],
        };
        assert!(toxics.air_toxics_block(&block, &tables).is_empty());
    }

    #[test]
    fn air_toxics_block_filters_a_voc_block_whose_process_has_no_ratios() {
        // nrATRatio / nrPAHGasRatio only carry process 1.
        let toxics = NrAirToxics::build([at_row(20, 1, 10, 20, CAT, 0.5)], [], [], [], [], []);
        let tables = tables_needing(&[20]);
        // A VOC block for process 99 — absent from both ratio tables.
        let block = FuelBlock {
            key: FuelBlockKey {
                process_id: 99,
                ..voc_key()
            },
            emissions: vec![emission(8.0, 4.0, 20, 100)],
        };
        assert!(toxics.air_toxics_block(&block, &tables).is_empty());
    }

    #[test]
    fn air_toxics_block_filters_a_pm25_block_whose_process_has_no_particle_ratios() {
        // nrPAHParticleRatio carries only process 1.
        let toxics = NrAirToxics::build([], [], [pfeh_row(23, 1, 1, 10, CAT, 0.1)], [], [], []);
        let tables = tables_needing(&[23]);
        let block = FuelBlock {
            key: FuelBlockKey {
                pollutant_id: PM25_POLLUTANT_ID,
                process_id: 99,
                ..voc_key()
            },
            emissions: vec![emission(8.0, 4.0, 20, 100)],
        };
        assert!(toxics.air_toxics_block(&block, &tables).is_empty());
    }

    #[test]
    fn air_toxics_block_filters_fuel_consumption_outside_running_exhaust() {
        let toxics = NrAirToxics::build([], [], [], [pfeh_row(130, 15, 1, 10, CAT, 2.0)], [], []);
        let tables = NonroadWorkerTables::new([(100, 20)], [((5, 10), CAT)], [ppid(130, 15)]);
        // Fuel consumption for crankcase running exhaust (process 15).
        let block = FuelBlock {
            key: FuelBlockKey {
                pollutant_id: FUEL_CONSUMPTION_POLLUTANT_ID,
                process_id: 15,
                eng_tech_id: 10,
                fuel_type_id: 1,
                hp_id: 5,
            },
            emissions: vec![emission(8.0, 4.0, 20, 100)],
        };
        assert!(toxics.air_toxics_block(&block, &tables).is_empty());
    }

    #[test]
    fn non_hap_tog_block_adds_nmog_with_a_positive_sign() {
        let toxics = NrAirToxics::build([], [], [], [], [], [20, 24]);
        let tables = tables_needing(&[NON_HAP_TOG_POLLUTANT_ID]);
        let block = FuelBlock {
            key: FuelBlockKey {
                pollutant_id: NMOG_POLLUTANT_ID,
                ..voc_key()
            },
            emissions: vec![emission(8.0, 4.0, 20, 100), emission(2.0, 1.0, 20, 100)],
        };
        let out = toxics
            .non_hap_tog_block(&block, &tables)
            .expect("NMOG contributes to NonHAPTOG");
        assert_eq!(out.pollutant_id, NON_HAP_TOG_POLLUTANT_ID);
        assert_eq!(out.pol_process_id, ppid(NON_HAP_TOG_POLLUTANT_ID, 1));
        // NMOG contributes +NMOG, in input order.
        assert_eq!(
            out.emissions,
            vec![emission(8.0, 4.0, 20, 100), emission(2.0, 1.0, 20, 100)],
        );
    }

    #[test]
    fn non_hap_tog_block_subtracts_an_integrated_species() {
        let toxics = NrAirToxics::build([], [], [], [], [], [20]);
        let tables = tables_needing(&[NON_HAP_TOG_POLLUTANT_ID]);
        // Benzene (20) is an integrated species — it subtracts from NonHAPTOG.
        let block = FuelBlock {
            key: FuelBlockKey {
                pollutant_id: 20,
                ..voc_key()
            },
            emissions: vec![emission(8.0, 4.0, 20, 100)],
        };
        let out = toxics
            .non_hap_tog_block(&block, &tables)
            .expect("integrated species contributes to NonHAPTOG");
        assert_eq!(out.pollutant_id, NON_HAP_TOG_POLLUTANT_ID);
        // Integrated species contributes -itself.
        assert_eq!(out.emissions, vec![emission(-8.0, -4.0, 20, 100)]);
    }

    #[test]
    fn non_hap_tog_block_skips_a_non_integrated_non_nmog_block() {
        // Pollutant 20 is not in the integrated-species set here.
        let toxics = NrAirToxics::build([], [], [], [], [], [24]);
        let tables = tables_needing(&[NON_HAP_TOG_POLLUTANT_ID]);
        let block = FuelBlock {
            key: FuelBlockKey {
                pollutant_id: 20,
                ..voc_key()
            },
            emissions: vec![emission(8.0, 4.0, 20, 100)],
        };
        assert!(toxics.non_hap_tog_block(&block, &tables).is_none());
    }

    #[test]
    fn non_hap_tog_block_skips_when_nonhaptog_is_not_needed() {
        let toxics = NrAirToxics::build([], [], [], [], [], [20]);
        // NonHAPTOG (88) is absent from the needed set.
        let tables = tables_needing(&[20]);
        let block = FuelBlock {
            key: FuelBlockKey {
                pollutant_id: NMOG_POLLUTANT_ID,
                ..voc_key()
            },
            emissions: vec![emission(8.0, 4.0, 20, 100)],
        };
        assert!(toxics.non_hap_tog_block(&block, &tables).is_none());
    }

    #[test]
    fn non_hap_tog_block_skips_an_empty_block() {
        let toxics = NrAirToxics::build([], [], [], [], [], [20]);
        let tables = tables_needing(&[NON_HAP_TOG_POLLUTANT_ID]);
        let block = FuelBlock {
            key: FuelBlockKey {
                pollutant_id: NMOG_POLLUTANT_ID,
                ..voc_key()
            },
            emissions: vec![],
        };
        assert!(toxics.non_hap_tog_block(&block, &tables).is_none());
    }

    #[test]
    fn calculator_metadata() {
        let calc = NrAirToxicsCalculator::new();
        assert_eq!(calc.name(), "NRAirToxicsCalculator");
        // Chained calculator — no direct master-loop subscription.
        assert!(calc.subscriptions().is_empty());
        assert_eq!(
            calc.upstream(),
            &["NRHCSpeciationCalculator", "NonroadEmissionCalculator"],
        );
        // The six SQL-extracted tables.
        for table in [
            "nrATRatio",
            "nrDioxinEmissionRate",
            "nrIntegratedSpecies",
            "nrMetalEmissionRate",
            "nrPAHGasRatio",
            "nrPAHParticleRatio",
        ] {
            assert!(calc.input_tables().contains(&table), "missing {table}");
        }
    }

    #[test]
    fn calculator_registers_205_pollutant_process_pairs() {
        assert_eq!(REGISTRATION_COUNT, 205);
        let calc = NrAirToxicsCalculator::new();
        let regs = calc.registrations();
        assert_eq!(regs.len(), 205);

        // Spot-check a registration from each of the irregular process
        // groups: benzene (20) and a dioxin (130) in running exhaust (1),
        // NonHAPTOG (88) in crankcase (15), xylene (46) in RecMar neck-hose
        // permeation (22), NonHAPTOG in runningloss venting (32).
        assert!(regs.contains(&reg(20, 1)));
        assert!(regs.contains(&reg(130, 1)));
        assert!(regs.contains(&reg(88, 15)));
        assert!(regs.contains(&reg(46, 22)));
        assert!(regs.contains(&reg(88, 32)));

        // Metals (mercury 60) and dioxins (130) are running-exhaust only —
        // never crankcase (process 15).
        assert!(!regs.contains(&reg(60, 15)));
        assert!(!regs.contains(&reg(130, 15)));

        // The RecMar hose-permeation processes (22, 23, 24) carry no
        // NonHAPTOG (88).
        assert!(!regs.contains(&reg(88, 22)));

        // No registration is duplicated.
        let unique: HashSet<_> = regs.iter().collect();
        assert_eq!(unique.len(), regs.len());
    }

    #[test]
    fn execute_wires_through_data_plane() {
        use moves_framework::DataFrameStore;
        let calc = NrAirToxicsCalculator::new();
        let mut store = moves_framework::InMemoryStore::new();
        // Empty lookup tables for paths not exercised in this test.
        store.insert("nrATRatio", AtRatioRow::into_dataframe(vec![]).unwrap());
        store.insert(
            "nrPAHParticleRatio",
            ProcFuelEngHpRow::into_dataframe(vec![]).unwrap(),
        );
        store.insert(
            "nrDioxinEmissionRate",
            ProcFuelEngHpRow::into_dataframe(vec![]).unwrap(),
        );
        store.insert(
            "nrMetalEmissionRate",
            ProcFuelEngHpRow::into_dataframe(vec![]).unwrap(),
        );
        store.insert(
            "nrIntegratedSpecies",
            NrIntegratedSpeciesRow::into_dataframe(vec![]).unwrap(),
        );
        // FuelFormulation: formulation 0 → subtype 0 (makes fuel_formulation_id=0 lookup succeed).
        store.insert(
            "FuelFormulation",
            FuelFormulationRow::into_dataframe(vec![FuelFormulationRow {
                fuel_formulation_id: 0,
                fuel_sub_type_id: 0,
            }])
            .unwrap(),
        );
        // NRHPCategory: (hp=5, eng=10) → category b'A'=65.
        store.insert(
            "NRHPCategory",
            NrHpCategoryRow::into_dataframe(vec![NrHpCategoryRow {
                hp_id: 5,
                eng_tech_id: 10,
                nr_hp_category: 65,
            }])
            .unwrap(),
        );
        // NeededPolProcessIDs: benzene (20) for process 1 → polProcessID 2001.
        store.insert(
            "NeededPolProcessIDs",
            NrNeededPolProcessRow::into_dataframe(vec![NrNeededPolProcessRow {
                pol_process_id: 2001,
            }])
            .unwrap(),
        );
        // nrPAHGasRatio: VOC (87), process 1, fuel type 1, eng 10, hp cat 65 → pollutant 20, ratio 0.5.
        store.insert(
            "nrPAHGasRatio",
            ProcFuelEngHpRow::into_dataframe(vec![ProcFuelEngHpRow {
                pollutant_id: 20,
                process_id: 1,
                fuel_type_id: 1,
                eng_tech_id: 10,
                nr_hp_category: 65,
                ratio: 0.5,
            }])
            .unwrap(),
        );
        // Input: one VOC (87) row — will be scaled by nrPAHGasRatio.
        store.insert(
            "MOVESWorkerOutput",
            NrAirToxicsMwoRow::into_dataframe(vec![NrAirToxicsMwoRow {
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
                source_type_id: 0,
                fuel_type_id: 1,
                eng_tech_id: 10,
                hp_id: 5,
                model_year_id: 0,
                road_type_id: 0,
                emission_quant: 100.0,
                emission_rate: 0.0,
            }])
            .unwrap(),
        );
        let ctx = CalculatorContext::with_tables(store);
        let out = calc.execute(&ctx).expect("execute ok");
        let df = out.dataframe().expect("output should contain a DataFrame");
        // nrPAHGasRatio fires: VOC 87 * 0.5 → pollutant 20.
        assert_eq!(
            df.height(),
            1,
            "expected 1 output row from nrPAHGasRatio path"
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
        let calc: Box<dyn Calculator> = Box::new(NrAirToxicsCalculator::new());
        assert_eq!(calc.name(), "NRAirToxicsCalculator");
        assert_eq!(calc.registrations().len(), 205);
    }
}
