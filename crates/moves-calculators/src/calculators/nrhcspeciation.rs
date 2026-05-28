//! Port of `calc/nrhcspeciation/nrhcspeciation.go` — the `NRHCSpeciationCalculator`,
//! the Nonroad hydrocarbon-speciation calculator.
//!
//! Migration plan: Phase 3, Task 49 (the Nonroad equivalent of Task 48,
//! `HCSpeciationCalculator`).
//!
//! # What this calculator does
//!
//! Nonroad emission calculators produce total hydrocarbons (THC,
//! pollutant 1). `NRHCSpeciationCalculator` splits that THC tally into the
//! five hydrocarbon species MOVES reports separately:
//!
//! * methane — `CH4`, pollutant 5;
//! * non-methane hydrocarbons — NMHC, pollutant 79;
//! * non-methane organic gases — NMOG, pollutant 80;
//! * total organic gases — TOG, pollutant 86;
//! * volatile organic compounds — VOC, pollutant 87.
//!
//! It speciates THC for every nonroad process that emits it — running and
//! crankcase exhaust, refueling, and the evaporative and fuel-vapor-venting
//! processes (see the [`registrations`](Calculator::registrations)).
//!
//! # The algorithm
//!
//! For one THC emission, with `r = CH4THCRatio` from the
//! `nrMethaneTHCRatio` table and `s = speciationConstant` from the
//! `nrHCSpeciation` table:
//!
//! ```text
//! methane = THC  * r
//! NMHC    = THC  * (1 - r)
//! NMOG    = NMHC * s(NMOG)      ( = 0 when no constant is tabulated)
//! VOC     = NMHC * s(VOC)       ( = 0 when no constant is tabulated)
//! TOG     = NMOG + methane
//! ```
//!
//! Each emission carries both an emission quantity and an emission rate;
//! every scaling above multiplies *both*, and the TOG sum adds *both*.
//!
//! # The two lookup tables
//!
//! * `nrMethaneTHCRatio` — the methane-to-THC ratio, keyed by
//!   `(processID, engTechID, fuelSubTypeID, nrHPCategory)`
//!   ([`MethaneThcRatioRow`]).
//! * `nrHCSpeciation` — the NMOG/VOC speciation constants, keyed by
//!   `(pollutantID, processID, engTechID, fuelSubTypeID, nrHPCategory)`
//!   ([`NrHcSpeciationRow`]).
//!
//! `nrHPCategory` is a single-character horse-power-category code; the Go
//! reads it as a byte, so it is a [`u8`] here.
//!
//! # Relationship to Task 48 (`HCSpeciationCalculator`)
//!
//! The onroad `HCSpeciationCalculator` shares the methane/NMHC/NMOG/TOG/VOC
//! shape but is considerably more involved: it carries model-year ranges in
//! its lookup keys, an `oxySpeciation` oxygenate term added to the
//! speciation constant, and an `altTHC`/`altNMHC` special case for E10
//! fuels on ethanol-fueled 2001+ vehicles. None of that applies to nonroad
//! equipment — the Nonroad calculator's keys carry no model year, the
//! speciation constant is used directly, and there is no E10 path. This
//! port mirrors the (simpler) Nonroad Go file exactly.
//!
//! # Scope of this port
//!
//! The pinned Go file is the whole `nrhcspeciation` package: the in-memory
//! lookup-table load (`StartSetup`) and the per-block speciation pass
//! (`calculate`). Both are ported in full — [`NrHcSpeciation::build`] and
//! [`NrHcSpeciation::speciate_block`] respectively.
//!
//! The Go ran `calculate` as a pool of goroutines draining a channel of
//! `MWOBlock`s; that worker plumbing is not part of the calculation and is
//! dropped. This port keeps the **computation** — the lookups, the five
//! speciation formulas, the per-pollutant grouping into new fuel blocks —
//! and replaces the channel boundary with plain values: [`FuelBlock`]s in,
//! [`SpeciatedFuelBlock`]s out.
//!
//! # Fidelity notes
//!
//! * **NMOG/VOC operand.** The Go computes `emissions[79]` (NMHC) only when
//!   NMHC output is requested, yet reuses that same gated map entry as the
//!   operand for the NMOG and VOC formulas. A run that requests NMOG or VOC
//!   without NMHC therefore dereferences a nil pointer in the Go. This port
//!   computes the NMHC operand value unconditionally (see
//!   [`NrHcSpeciation::speciate_emission`]); the NMHC *output* is still
//!   gated. For every needed-set closed under the NMOG/VOC → NMHC
//!   dependency — i.e. every real MOVES run, because the pollutant chain
//!   pulls NMHC in as an intermediate whenever NMOG or VOC is requested —
//!   the result is numerically identical, and the degenerate set yields the
//!   correct number instead of a crash.
//! * **Two fuel-subtype ids.** The `nrMethaneTHCRatio` lookup keys on the
//!   *fuel formulation's* `fuelSubTypeID`; the `nrHCSpeciation` lookup keys
//!   on the *emission's* `fuelSubTypeID`. The Go reads them from those two
//!   distinct places and this port preserves the distinction.
//! * **Output order.** The Go grouped output emissions into new fuel blocks
//!   keyed in a Go `map`, whose iteration order is randomised.
//!   [`NrHcSpeciation::speciate_block`] returns the blocks in ascending
//!   pollutant-id order so the output is deterministic; a fuel-block set is
//!   unordered, so this is a presentation choice only.
//!
//! # Data plane (Task 50)
//!
//! [`Calculator::execute`] receives a [`CalculatorContext`] whose
//! `ExecutionTables` / `ScratchNamespace` are Phase-2 placeholders until the
//! `DataFrameStore` lands (migration-plan Task 50), so `execute` cannot yet
//! read the `nrMethaneTHCRatio` / `nrHCSpeciation` tables nor the upstream
//! THC emission blocks, nor write the speciated blocks back. The
//! numerically faithful algorithm is fully ported and unit-tested on
//! [`NrHcSpeciation`]; once the data plane exists, `execute` builds an
//! [`NrHcSpeciation`] from `ctx.tables()`, reads the THC [`FuelBlock`]s,
//! applies [`speciate_block`](NrHcSpeciation::speciate_block), and stores
//! the resulting [`SpeciatedFuelBlock`]s.

use std::collections::{BTreeMap, HashMap, HashSet};

use moves_data::{PollutantId, PollutantProcessAssociation, ProcessId};
use moves_framework::{
    Calculator, CalculatorContext, CalculatorOutput, CalculatorSubscription, DataFrameStoreTyped,
    Error, TableRow,
};
use polars::prelude::{DataFrame, DataType, NamedFrom, PolarsResult, Schema, Series};

/// THC — total hydrocarbons, pollutant 1. The calculator's only input
/// pollutant: it speciates THC fuel blocks and ignores every other block.
const THC_POLLUTANT_ID: i32 = 1;
/// Methane (`CH4`), pollutant 5 — `THC * CH4THCRatio`.
const METHANE_POLLUTANT_ID: i32 = 5;
/// Non-methane hydrocarbons, pollutant 79 — `THC * (1 - CH4THCRatio)`.
const NMHC_POLLUTANT_ID: i32 = 79;
/// Non-methane organic gases, pollutant 80 — `NMHC * speciationConstant`.
const NMOG_POLLUTANT_ID: i32 = 80;
/// Total organic gases, pollutant 86 — `NMOG + methane`.
const TOG_POLLUTANT_ID: i32 = 86;
/// Volatile organic compounds, pollutant 87 — `NMHC * speciationConstant`.
const VOC_POLLUTANT_ID: i32 = 87;

/// One emission record — the Go `mwo.MWOEmission`, restricted to the fields
/// the speciation calculator reads and writes.
///
/// An emission carries a quantity and a rate; the speciation formulas scale
/// or sum *both*. `fuel_sub_type_id` and `fuel_formulation_id` identify the
/// fuel the emission belongs to and are carried through unchanged onto every
/// speciated emission derived from it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Emission {
    /// `fuelSubTypeID` — the emission's fuel subtype. Keys the
    /// `nrHCSpeciation` lookup (note: *not* the formulation's subtype; see
    /// the module docs).
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

/// Sum two optional emissions — the Go `mwo.NewEmissionSum`.
///
/// `None` is treated as a zero contribution: with both present the
/// quantities and rates add and the fuel ids come from `a`; with one
/// present the other is copied; with neither present the result is `None`
/// (the Go returns a nil `*MWOEmission`, so no TOG emission is produced).
fn emission_sum(a: Option<&Emission>, b: Option<&Emission>) -> Option<Emission> {
    match (a, b) {
        (None, None) => None,
        (Some(a), Some(b)) => Some(Emission {
            fuel_sub_type_id: a.fuel_sub_type_id,
            fuel_formulation_id: a.fuel_formulation_id,
            emission_quant: a.emission_quant + b.emission_quant,
            emission_rate: a.emission_rate + b.emission_rate,
        }),
        (Some(a), None) => Some(*a),
        (None, Some(b)) => Some(*b),
    }
}

/// Key of the `nrMethaneTHCRatio` lookup — the Go `methaneTHCRatioKey`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct MethaneThcRatioKey {
    /// `processID`.
    process_id: i32,
    /// `engTechID`.
    eng_tech_id: i32,
    /// `fuelSubTypeID` — the *fuel formulation's* subtype.
    fuel_sub_type_id: i32,
    /// `nrHPCategory` — the single-character horse-power-category code.
    nr_hp_category: u8,
}

/// Key of the `nrHCSpeciation` lookup — the Go `NRHCSpeciationKey`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct NrHcSpeciationKey {
    /// `pollutantID` — 80 (NMOG) or 87 (VOC).
    pollutant_id: i32,
    /// `processID`.
    process_id: i32,
    /// `engTechID`.
    eng_tech_id: i32,
    /// `fuelSubTypeID` — the *emission's* subtype.
    fuel_sub_type_id: i32,
    /// `nrHPCategory` — the single-character horse-power-category code.
    nr_hp_category: u8,
}

/// One `nrMethaneTHCRatio` table row — input to [`NrHcSpeciation::build`].
///
/// The Go `StartSetup` reads these columns from the `nrmethanethcratio`
/// extract file: `processID, engTechID, fuelSubtypeID, nrHPCategory,
/// CH4THCRatio`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MethaneThcRatioRow {
    /// `processID`.
    pub process_id: i32,
    /// `engTechID`.
    pub eng_tech_id: i32,
    /// `fuelSubtypeID`.
    pub fuel_sub_type_id: i32,
    /// `nrHPCategory` — the horse-power-category code byte.
    pub nr_hp_category: u8,
    /// `CH4THCRatio` — the methane-to-THC ratio.
    pub ch4_thc_ratio: f64,
}

/// One `nrHCSpeciation` table row — input to [`NrHcSpeciation::build`].
///
/// The Go `StartSetup` reads these columns from the `nrhcspeciation`
/// extract file: `pollutantID, processID, engTechID, fuelSubTypeID,
/// nrHPCategory, speciationConstant`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct NrHcSpeciationRow {
    /// `pollutantID` — 80 (NMOG) or 87 (VOC).
    pub pollutant_id: i32,
    /// `processID`.
    pub process_id: i32,
    /// `engTechID`.
    pub eng_tech_id: i32,
    /// `fuelSubTypeID`.
    pub fuel_sub_type_id: i32,
    /// `nrHPCategory` — the horse-power-category code byte.
    pub nr_hp_category: u8,
    /// `speciationConstant` — the NMOG/VOC speciation multiplier.
    pub speciation_constant: f64,
}

/// The key fields of an MWO `FuelBlock` that the NR HC speciation
/// calculator reads — a subset of the Go `mwo.MWOKey`.
///
/// The Go `calculate` reads only these four fields of the input block's
/// key; the rest of `MWOKey` (geography, time, source type, …) is opaque
/// passthrough that the data-plane integration copies onto the output
/// blocks, so it is not modeled here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FuelBlockKey {
    /// `pollutantID` — the calculator speciates only THC (1) blocks.
    pub pollutant_id: i32,
    /// `processID`.
    pub process_id: i32,
    /// `engTechID`.
    pub eng_tech_id: i32,
    /// `hpID` — keys the `nrHPCategory` horse-power-category lookup.
    pub hp_id: i32,
}

/// One input fuel block — the Go `mwo.FuelBlock`, restricted to the key
/// fields and emissions the calculator consumes.
///
/// The calculator speciates the emissions of a THC (pollutant 1) block; a
/// block of any other pollutant is ignored.
#[derive(Debug, Clone, PartialEq)]
pub struct FuelBlock {
    /// The block's key fields.
    pub key: FuelBlockKey,
    /// The per-fuel-formulation emissions in the block.
    pub emissions: Vec<Emission>,
}

/// The speciated emissions produced from one THC [`Emission`].
///
/// Each field is the emission for one output pollutant, or `None` when that
/// pollutant is not in the run's needed set (or, for TOG, when neither of
/// its NMOG and methane summands is present).
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct SpeciatedEmission {
    /// Methane (pollutant 5).
    pub methane: Option<Emission>,
    /// Non-methane hydrocarbons (pollutant 79).
    pub nmhc: Option<Emission>,
    /// Non-methane organic gases (pollutant 80).
    pub nmog: Option<Emission>,
    /// Total organic gases (pollutant 86).
    pub tog: Option<Emission>,
    /// Volatile organic compounds (pollutant 87).
    pub voc: Option<Emission>,
}

impl SpeciatedEmission {
    /// The present `(pollutant_id, emission)` pairs, in ascending
    /// pollutant-id order (5, 79, 80, 86, 87).
    #[must_use]
    pub fn pollutant_emissions(&self) -> Vec<(i32, Emission)> {
        [
            (METHANE_POLLUTANT_ID, self.methane),
            (NMHC_POLLUTANT_ID, self.nmhc),
            (NMOG_POLLUTANT_ID, self.nmog),
            (TOG_POLLUTANT_ID, self.tog),
            (VOC_POLLUTANT_ID, self.voc),
        ]
        .into_iter()
        .filter_map(|(id, emission)| emission.map(|e| (id, e)))
        .collect()
    }
}

/// One output fuel block — a single speciated pollutant's emissions.
///
/// The Go `calculate` produced these by copying the input THC block's key,
/// overwriting `pollutantID` and `polProcessID`, and attaching the
/// speciated emissions. This port returns the two computed key fields plus
/// the emissions; copying the rest of the input key is data-plane plumbing
/// the caller handles.
#[derive(Debug, Clone, PartialEq)]
pub struct SpeciatedFuelBlock {
    /// `pollutantID` of the speciated species (5, 79, 80, 86 or 87).
    pub pollutant_id: i32,
    /// `polProcessID` — `pollutantID * 100 + processID`.
    pub pol_process_id: i32,
    /// The speciated emissions, one per input emission that produced this
    /// species, in input-emission order.
    pub emissions: Vec<Emission>,
}

/// The ambient nonroad-worker lookup tables the calculator consults but does
/// not own — the Go `mwo` package globals.
///
/// `nrMethaneTHCRatio` and `nrHCSpeciation` are loaded by the calculator
/// itself ([`NrHcSpeciation`]); the tables here are populated elsewhere in
/// the nonroad worker setup and shared across calculators:
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

    /// The fuel subtype of a fuel formulation, or `None` when the
    /// formulation is unknown — the Go `mwo.FuelFormulations[id]` returning
    /// nil, which makes `calculate` skip the emission.
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

    /// Whether `pollutantID * 100 + processID` is in the run's needed set —
    /// the Go `mwo.NeededPolProcessIDs[ppid]`.
    fn is_needed(&self, pollutant_id: i32, process_id: i32) -> bool {
        self.needed_pol_process_ids
            .contains(&(pollutant_id * 100 + process_id))
    }
}

/// The Nonroad HC speciation lookup tables and the speciation algorithm —
/// the in-memory state and `calculate` body of the Go `nrhcspeciation`
/// package.
#[derive(Debug, Clone, Default)]
pub struct NrHcSpeciation {
    /// `nrMethaneTHCRatio` — methane-to-THC ratios.
    methane_thc_ratio: HashMap<MethaneThcRatioKey, f64>,
    /// `nrHCSpeciation` — NMOG/VOC speciation constants.
    hc_speciation: HashMap<NrHcSpeciationKey, f64>,
}

impl NrHcSpeciation {
    /// Build the lookup tables from `nrMethaneTHCRatio` and `nrHCSpeciation`
    /// table rows — the in-memory half of the Go `StartSetup`.
    ///
    /// When two rows share a key the last one wins. The Go logs a diagnostic
    /// to stdout on a duplicate `nrHCSpeciation` key and then overwrites
    /// (`nrMethaneTHCRatio` overwrites silently); either way the resulting
    /// map keeps the last row, which this port reproduces without the log.
    #[must_use]
    pub fn build(
        methane_thc_ratio_rows: impl IntoIterator<Item = MethaneThcRatioRow>,
        nr_hc_speciation_rows: impl IntoIterator<Item = NrHcSpeciationRow>,
    ) -> Self {
        let mut methane_thc_ratio = HashMap::new();
        for row in methane_thc_ratio_rows {
            methane_thc_ratio.insert(
                MethaneThcRatioKey {
                    process_id: row.process_id,
                    eng_tech_id: row.eng_tech_id,
                    fuel_sub_type_id: row.fuel_sub_type_id,
                    nr_hp_category: row.nr_hp_category,
                },
                row.ch4_thc_ratio,
            );
        }

        let mut hc_speciation = HashMap::new();
        for row in nr_hc_speciation_rows {
            hc_speciation.insert(
                NrHcSpeciationKey {
                    pollutant_id: row.pollutant_id,
                    process_id: row.process_id,
                    eng_tech_id: row.eng_tech_id,
                    fuel_sub_type_id: row.fuel_sub_type_id,
                    nr_hp_category: row.nr_hp_category,
                },
                row.speciation_constant,
            );
        }

        Self {
            methane_thc_ratio,
            hc_speciation,
        }
    }

    /// The `speciationConstant` for an output pollutant, or `None` when no
    /// `nrHCSpeciation` row matches — the Go `HCSpeciation[...]` returning
    /// nil, which makes the NMOG/VOC formula fall back to a zero emission.
    fn speciation_constant(
        &self,
        pollutant_id: i32,
        process_id: i32,
        eng_tech_id: i32,
        fuel_sub_type_id: i32,
        nr_hp_category: u8,
    ) -> Option<f64> {
        self.hc_speciation
            .get(&NrHcSpeciationKey {
                pollutant_id,
                process_id,
                eng_tech_id,
                fuel_sub_type_id,
                nr_hp_category,
            })
            .copied()
    }

    /// Speciate one THC [`Emission`] into its methane / NMHC / NMOG / TOG /
    /// VOC species — the inner per-emission body of the Go `calculate`.
    ///
    /// `block_key` is the key of the THC fuel block the emission belongs to;
    /// the calculator reads `process_id`, `eng_tech_id` and `hp_id` from it.
    /// `block_key.pollutant_id` is *not* checked here — [`speciate_block`]
    /// makes the THC-block test; calling this directly is meaningful only
    /// for a THC block's emission.
    ///
    /// Returns `None` when the emission cannot be speciated at all — its
    /// fuel formulation is unknown, or no `nrMethaneTHCRatio` row matches
    /// (both make the Go `calculate` `continue` past the emission). A
    /// returned [`SpeciatedEmission`] may still have every field `None` if
    /// the run's needed set asks for none of the five species.
    ///
    /// [`speciate_block`]: Self::speciate_block
    #[must_use]
    pub fn speciate_emission(
        &self,
        block_key: &FuelBlockKey,
        emission: &Emission,
        tables: &NonroadWorkerTables,
    ) -> Option<SpeciatedEmission> {
        // Go: ff := mwo.FuelFormulations[e.FuelFormulationID]; if ff == nil { continue }
        let formulation_fuel_sub_type_id = tables.fuel_sub_type_id(emission.fuel_formulation_id)?;

        // Go: hpCategory := mwo.NRHPCategory[NRHPCategoryKey{HPID, EngTechID}]
        let nr_hp_category = tables.hp_category(block_key.hp_id, block_key.eng_tech_id);

        // Go: r := methaneTHCRatio[...]; if r == nil { continue }.
        // The ratio lookup keys on the *formulation's* fuel subtype.
        let ch4_thc_ratio = *self.methane_thc_ratio.get(&MethaneThcRatioKey {
            process_id: block_key.process_id,
            eng_tech_id: block_key.eng_tech_id,
            fuel_sub_type_id: formulation_fuel_sub_type_id,
            nr_hp_category,
        })?;

        let process_id = block_key.process_id;

        // methane (5) = THC * CH4THCRatio.
        let methane = tables
            .is_needed(METHANE_POLLUTANT_ID, process_id)
            .then(|| emission.scaled(ch4_thc_ratio));

        // NMHC (79) = THC * (1 - CH4THCRatio).
        //
        // The NMHC value is computed unconditionally because the NMOG and
        // VOC formulas below take it as their operand; the Go gates this on
        // the NMHC needed-flag and so nil-panics for a needed-set with NMOG
        // or VOC but not NMHC (see the module-level fidelity note). The NMHC
        // *output* is still gated.
        let nmhc_value = emission.scaled(1.0 - ch4_thc_ratio);
        let nmhc = tables
            .is_needed(NMHC_POLLUTANT_ID, process_id)
            .then_some(nmhc_value);

        // NMOG (80) = NMHC * speciationConstant; a zero emission when no
        // speciation constant is tabulated for the pollutant.
        let nmog = tables.is_needed(NMOG_POLLUTANT_ID, process_id).then(|| {
            match self.speciation_constant(
                NMOG_POLLUTANT_ID,
                process_id,
                block_key.eng_tech_id,
                emission.fuel_sub_type_id,
                nr_hp_category,
            ) {
                Some(constant) => nmhc_value.scaled(constant),
                None => emission.scaled(0.0),
            }
        });

        // VOC (87) = NMHC * speciationConstant; same zero fallback as NMOG.
        let voc = tables.is_needed(VOC_POLLUTANT_ID, process_id).then(|| {
            match self.speciation_constant(
                VOC_POLLUTANT_ID,
                process_id,
                block_key.eng_tech_id,
                emission.fuel_sub_type_id,
                nr_hp_category,
            ) {
                Some(constant) => nmhc_value.scaled(constant),
                None => emission.scaled(0.0),
            }
        });

        // TOG (86) = NMOG (80) + methane (5), summing the *gated* species:
        // an un-needed summand contributes nothing, and TOG is omitted when
        // both summands are absent.
        let tog = tables
            .is_needed(TOG_POLLUTANT_ID, process_id)
            .then(|| emission_sum(nmog.as_ref(), methane.as_ref()))
            .flatten();

        Some(SpeciatedEmission {
            methane,
            nmhc,
            nmog,
            tog,
            voc,
        })
    }

    /// Speciate a whole THC fuel block into one output block per produced
    /// pollutant — the Go `calculate`'s per-`FuelBlock` body.
    ///
    /// A block whose pollutant is not THC (pollutant 1) yields no output
    /// (the Go `if fb.Key.PollutantID != 1 { continue }`). Otherwise
    /// each emission is speciated and the resulting species emissions are
    /// grouped into [`SpeciatedFuelBlock`]s by pollutant — the emissions
    /// within a block keep input-emission order, and the blocks are returned
    /// in ascending pollutant-id order (see the module-level fidelity note
    /// on output order).
    #[must_use]
    pub fn speciate_block(
        &self,
        block: &FuelBlock,
        tables: &NonroadWorkerTables,
    ) -> Vec<SpeciatedFuelBlock> {
        // Go: only THC blocks are speciated.
        if block.key.pollutant_id != THC_POLLUTANT_ID {
            return Vec::new();
        }

        // Group the speciated emissions by output pollutant. A BTreeMap
        // keeps the output blocks in ascending pollutant-id order; each
        // Vec keeps input-emission order.
        let mut by_pollutant: BTreeMap<i32, Vec<Emission>> = BTreeMap::new();
        for emission in &block.emissions {
            let Some(speciated) = self.speciate_emission(&block.key, emission, tables) else {
                continue;
            };
            for (pollutant_id, species_emission) in speciated.pollutant_emissions() {
                by_pollutant
                    .entry(pollutant_id)
                    .or_default()
                    .push(species_emission);
            }
        }

        by_pollutant
            .into_iter()
            .map(|(pollutant_id, emissions)| SpeciatedFuelBlock {
                pollutant_id,
                pol_process_id: pollutant_id * 100 + block.key.process_id,
                emissions,
            })
            .collect()
    }
}

/// `(pollutant, process)` registration helper — keeps [`REGISTRATIONS`]
/// readable.
const fn reg(pollutant: u16, process: u16) -> PollutantProcessAssociation {
    PollutantProcessAssociation {
        pollutant_id: PollutantId(pollutant),
        process_id: ProcessId(process),
    }
}

/// The 45 `(pollutant, process)` pairs `NRHCSpeciationCalculator` registers
/// — its five output species (methane 5, NMHC 79, NMOG 80, TOG 86, VOC 87)
/// across the nine nonroad processes that emit THC.
///
/// Matches the `NRHCSpeciationCalculator` registrations in
/// `characterization/calculator-chains/calculator-dag.json`
/// (`registrations_count: 45`).
static REGISTRATIONS: [PollutantProcessAssociation; 45] = [
    // Running Exhaust (1)
    reg(5, 1),
    reg(79, 1),
    reg(80, 1),
    reg(86, 1),
    reg(87, 1),
    // Crankcase Running Exhaust (15)
    reg(5, 15),
    reg(79, 15),
    reg(80, 15),
    reg(86, 15),
    reg(87, 15),
    // Refueling Displacement Vapor Loss (18)
    reg(5, 18),
    reg(79, 18),
    reg(80, 18),
    reg(86, 18),
    reg(87, 18),
    // Refueling Spillage Loss (19)
    reg(5, 19),
    reg(79, 19),
    reg(80, 19),
    reg(86, 19),
    reg(87, 19),
    // Evap Tank Permeation (20)
    reg(5, 20),
    reg(79, 20),
    reg(80, 20),
    reg(86, 20),
    reg(87, 20),
    // Evap Hose Permeation (21)
    reg(5, 21),
    reg(79, 21),
    reg(80, 21),
    reg(86, 21),
    reg(87, 21),
    // Diurnal Fuel Vapor Venting (30)
    reg(5, 30),
    reg(79, 30),
    reg(80, 30),
    reg(86, 30),
    reg(87, 30),
    // HotSoak Fuel Vapor Venting (31)
    reg(5, 31),
    reg(79, 31),
    reg(80, 31),
    reg(86, 31),
    reg(87, 31),
    // RunningLoss Fuel Vapor Venting (32)
    reg(5, 32),
    reg(79, 32),
    reg(80, 32),
    reg(86, 32),
    reg(87, 32),
];

fn row_err(table: &'static str, row: usize, column: &'static str, msg: String) -> Error {
    Error::RowExtraction {
        table: table.into(),
        row,
        column: column.into(),
        message: msg,
    }
}

/// One `FuelFormulation` row — only the `fuelSubTypeID` projection needed
/// by the NR speciation lookup.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct NrFuelFormulationRow {
    /// `fuelFormulationID`.
    pub fuel_formulation_id: i32,
    /// `fuelSubTypeID`.
    pub fuel_sub_type_id: i32,
}

impl TableRow for NrFuelFormulationRow {
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
        let ff_id = get_i32("fuelFormulationID")?;
        let sub_type = get_i32("fuelSubTypeID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(NrFuelFormulationRow {
                    fuel_formulation_id: ff_id.get(i).ok_or_else(|| null("fuelFormulationID"))?,
                    fuel_sub_type_id: sub_type.get(i).ok_or_else(|| null("fuelSubTypeID"))?,
                })
            })
            .collect()
    }
}

/// One `nrHPCategory` row — the `(hpID, engTechID)` → hp-category mapping.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct NrHpCategoryRow {
    /// `hpID`.
    pub hp_id: i32,
    /// `engTechID`.
    pub eng_tech_id: i32,
    /// `nrHPCategory` — stored as `i32`; converted to `u8` at use.
    pub nr_hp_category: i32,
}

impl TableRow for NrHpCategoryRow {
    fn table_name() -> &'static str {
        "nrHPCategory"
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
        let t = "nrHPCategory";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let hp = get_i32("hpID")?;
        let eng_tech = get_i32("engTechID")?;
        let category = get_i32("nrHPCategory")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(NrHpCategoryRow {
                    hp_id: hp.get(i).ok_or_else(|| null("hpID"))?,
                    eng_tech_id: eng_tech.get(i).ok_or_else(|| null("engTechID"))?,
                    nr_hp_category: category.get(i).ok_or_else(|| null("nrHPCategory"))?,
                })
            })
            .collect()
    }
}

/// One `MOVESWorkerOutput` input or output row for the NR HC speciation
/// calculator.
#[derive(Debug, Clone, PartialEq)]
pub struct NrThcWorkerRow {
    pub year_id: i32,
    pub month_id: i32,
    pub day_id: i32,
    pub hour_id: i32,
    pub state_id: i32,
    pub county_id: i32,
    pub zone_id: i32,
    pub link_id: i32,
    pub pollutant_id: i32,
    pub process_id: i32,
    pub source_type_id: i32,
    pub road_type_id: i32,
    pub sector_id: i32,
    pub eng_tech_id: i32,
    pub hp_id: i32,
    pub fuel_sub_type_id: i32,
    pub fuel_formulation_id: i32,
    pub emission_quant: f64,
    pub emission_rate: f64,
}

impl TableRow for NrThcWorkerRow {
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
            ("roadTypeID".into(), DataType::Int32),
            ("sectorID".into(), DataType::Int32),
            ("engTechID".into(), DataType::Int32),
            ("hpID".into(), DataType::Int32),
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
                    "roadTypeID".into(),
                    rows.iter().map(|r| r.road_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sectorID".into(),
                    rows.iter().map(|r| r.sector_id).collect::<Vec<i32>>(),
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
        let road_type = get_i32("roadTypeID")?;
        let sector = get_i32("sectorID")?;
        let eng_tech = get_i32("engTechID")?;
        let hp = get_i32("hpID")?;
        let fuel_sub_type = get_i32("fuelSubTypeID")?;
        let fuel_formulation = get_i32("fuelFormulationID")?;
        let emission_quant = get_f64("emissionQuant")?;
        let emission_rate = get_f64("emissionRate")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(NrThcWorkerRow {
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
                    road_type_id: road_type.get(i).ok_or_else(|| null("roadTypeID"))?,
                    sector_id: sector.get(i).ok_or_else(|| null("sectorID"))?,
                    eng_tech_id: eng_tech.get(i).ok_or_else(|| null("engTechID"))?,
                    hp_id: hp.get(i).ok_or_else(|| null("hpID"))?,
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

impl TableRow for MethaneThcRatioRow {
    fn table_name() -> &'static str {
        "nrMethaneTHCRatio"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("processID".into(), DataType::Int32),
            ("engTechID".into(), DataType::Int32),
            ("fuelSubtypeID".into(), DataType::Int32),
            ("nrHPCategory".into(), DataType::Int32),
            ("CH4THCRatio".into(), DataType::Float64),
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
                        .map(|r| i32::from(r.nr_hp_category))
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "CH4THCRatio".into(),
                    rows.iter().map(|r| r.ch4_thc_ratio).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "nrMethaneTHCRatio";
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
        let eng_tech = get_i32("engTechID")?;
        let fuel_sub = get_i32("fuelSubtypeID")?;
        let category = get_i32("nrHPCategory")?;
        let ratio = get_f64("CH4THCRatio")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(MethaneThcRatioRow {
                    process_id: process.get(i).ok_or_else(|| null("processID"))?,
                    eng_tech_id: eng_tech.get(i).ok_or_else(|| null("engTechID"))?,
                    fuel_sub_type_id: fuel_sub.get(i).ok_or_else(|| null("fuelSubtypeID"))?,
                    nr_hp_category: category.get(i).ok_or_else(|| null("nrHPCategory"))? as u8,
                    ch4_thc_ratio: ratio.get(i).ok_or_else(|| null("CH4THCRatio"))?,
                })
            })
            .collect()
    }
}

impl TableRow for NrHcSpeciationRow {
    fn table_name() -> &'static str {
        "nrHCSpeciation"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("pollutantID".into(), DataType::Int32),
            ("processID".into(), DataType::Int32),
            ("engTechID".into(), DataType::Int32),
            ("fuelSubTypeID".into(), DataType::Int32),
            ("nrHPCategory".into(), DataType::Int32),
            ("speciationConstant".into(), DataType::Float64),
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
                    "fuelSubTypeID".into(),
                    rows.iter()
                        .map(|r| r.fuel_sub_type_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "nrHPCategory".into(),
                    rows.iter()
                        .map(|r| i32::from(r.nr_hp_category))
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "speciationConstant".into(),
                    rows.iter()
                        .map(|r| r.speciation_constant)
                        .collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "nrHCSpeciation";
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
        let fuel_sub = get_i32("fuelSubTypeID")?;
        let category = get_i32("nrHPCategory")?;
        let spec_const = get_f64("speciationConstant")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(NrHcSpeciationRow {
                    pollutant_id: pollutant.get(i).ok_or_else(|| null("pollutantID"))?,
                    process_id: process.get(i).ok_or_else(|| null("processID"))?,
                    eng_tech_id: eng_tech.get(i).ok_or_else(|| null("engTechID"))?,
                    fuel_sub_type_id: fuel_sub.get(i).ok_or_else(|| null("fuelSubTypeID"))?,
                    nr_hp_category: category.get(i).ok_or_else(|| null("nrHPCategory"))? as u8,
                    speciation_constant: spec_const
                        .get(i)
                        .ok_or_else(|| null("speciationConstant"))?,
                })
            })
            .collect()
    }
}

/// `NRHCSpeciationCalculator` declares no master-loop subscription of its
/// own; see the [`Calculator::subscriptions`] impl.
static NO_SUBSCRIPTIONS: &[CalculatorSubscription] = &[];

/// Upstream module — `NRHCSpeciationCalculator` chains to
/// `NonroadEmissionCalculator`, which produces the THC it speciates
/// (`depends_on` in `calculator-dag.json`).
static UPSTREAM: &[&str] = &["NonroadEmissionCalculator"];

/// Default-DB tables the calculator's SQL extracts: `nrHCSpeciation` (the
/// NMOG/VOC speciation constants) and `nrMethaneTHCRatio` (the methane-to-THC
/// ratios). The speciation pass also consults the shared nonroad worker
/// tables `FuelFormulation` and `nrHPCategory`, which other calculators load.
static INPUT_TABLES: &[&str] = &["nrHCSpeciation", "nrMethaneTHCRatio"];

/// `NRHCSpeciationCalculator` as a chain-DAG [`Calculator`].
///
/// The numerically faithful work lives on [`NrHcSpeciation`]; this
/// zero-sized type carries the calculator's chain metadata —
/// [`name`](Calculator::name), [`registrations`](Calculator::registrations),
/// [`upstream`](Calculator::upstream) — so the registry can wire it into the
/// calculator chain.
#[derive(Debug, Clone, Copy, Default)]
pub struct NrHcSpeciationCalculator;

impl NrHcSpeciationCalculator {
    /// Chain-DAG name — matches the Java class / Go package and the
    /// `calculator-dag.json` entry.
    pub const NAME: &'static str = "NRHCSpeciationCalculator";

    /// Construct the calculator.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl Calculator for NrHcSpeciationCalculator {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    /// `NRHCSpeciationCalculator` carries no master-loop subscription of its
    /// own: `calculator-dag.json` records `subscribes_directly: false`. It
    /// is a chained calculator — it runs when the calculator it chains to
    /// (its [`upstream`](Calculator::upstream) module) runs, speciating that
    /// calculator's THC output.
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

        let speciation = NrHcSpeciation::build(
            tables.iter_typed::<MethaneThcRatioRow>("nrMethaneTHCRatio")?,
            tables.iter_typed::<NrHcSpeciationRow>("nrHCSpeciation")?,
        );

        let fuel_formulations = tables
            .iter_typed::<NrFuelFormulationRow>("FuelFormulation")?
            .into_iter()
            .map(|r| (r.fuel_formulation_id, r.fuel_sub_type_id));
        let hp_categories = tables
            .iter_typed::<NrHpCategoryRow>("nrHPCategory")?
            .into_iter()
            .map(|r| ((r.hp_id, r.eng_tech_id), r.nr_hp_category as u8));
        let needed_pol_process_ids = REGISTRATIONS
            .iter()
            .map(|r| i32::from(r.pollutant_id.0) * 100 + i32::from(r.process_id.0));
        let worker_tables =
            NonroadWorkerTables::new(fuel_formulations, hp_categories, needed_pol_process_ids);

        let thc_rows: Vec<NrThcWorkerRow> = tables.iter_typed("MOVESWorkerOutput")?;
        let mut output: Vec<NrThcWorkerRow> = Vec::new();
        for row in &thc_rows {
            let block = FuelBlock {
                key: FuelBlockKey {
                    pollutant_id: row.pollutant_id,
                    process_id: row.process_id,
                    eng_tech_id: row.eng_tech_id,
                    hp_id: row.hp_id,
                },
                emissions: vec![Emission {
                    fuel_sub_type_id: row.fuel_sub_type_id,
                    fuel_formulation_id: row.fuel_formulation_id,
                    emission_quant: row.emission_quant,
                    emission_rate: row.emission_rate,
                }],
            };
            for speciated in speciation.speciate_block(&block, &worker_tables) {
                for em in &speciated.emissions {
                    output.push(NrThcWorkerRow {
                        pollutant_id: speciated.pollutant_id,
                        fuel_sub_type_id: em.fuel_sub_type_id,
                        fuel_formulation_id: em.fuel_formulation_id,
                        emission_quant: em.emission_quant,
                        emission_rate: em.emission_rate,
                        ..*row
                    });
                }
            }
        }

        crate::wiring::emit_rows(output)
    }
}

/// Construct the calculator as a boxed trait object — matches the engine's
/// calculator-factory signature so the registry can register it.
#[must_use]
pub fn factory() -> Box<dyn Calculator> {
    Box::new(NrHcSpeciationCalculator)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// HP-category code used throughout the tests.
    const CAT: u8 = b'A';

    /// A `nrMethaneTHCRatio` row helper.
    fn methane_row(
        process_id: i32,
        eng_tech_id: i32,
        fuel_sub_type_id: i32,
        nr_hp_category: u8,
        ch4_thc_ratio: f64,
    ) -> MethaneThcRatioRow {
        MethaneThcRatioRow {
            process_id,
            eng_tech_id,
            fuel_sub_type_id,
            nr_hp_category,
            ch4_thc_ratio,
        }
    }

    /// A `nrHCSpeciation` row helper.
    fn hc_row(
        pollutant_id: i32,
        process_id: i32,
        eng_tech_id: i32,
        fuel_sub_type_id: i32,
        nr_hp_category: u8,
        speciation_constant: f64,
    ) -> NrHcSpeciationRow {
        NrHcSpeciationRow {
            pollutant_id,
            process_id,
            eng_tech_id,
            fuel_sub_type_id,
            nr_hp_category,
            speciation_constant,
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

    /// The standard test fixture: process 1, engTech 10, fuel subtype 20,
    /// formulation 100, HP id 5. The methane ratio is `0.25` and the NMOG
    /// and VOC speciation constants `0.5` and `0.125` — all exactly
    /// representable in `f64`, so the tests can use exact equality.
    fn fixture() -> (NrHcSpeciation, NonroadWorkerTables, FuelBlockKey) {
        let speciation = NrHcSpeciation::build(
            [methane_row(1, 10, 20, CAT, 0.25)],
            [
                hc_row(NMOG_POLLUTANT_ID, 1, 10, 20, CAT, 0.5),
                hc_row(VOC_POLLUTANT_ID, 1, 10, 20, CAT, 0.125),
            ],
        );
        // All five output species needed for process 1.
        let needed = [5, 79, 80, 86, 87].map(|p| p * 100 + 1);
        let tables = NonroadWorkerTables::new([(100, 20)], [((5, 10), CAT)], needed);
        let key = FuelBlockKey {
            pollutant_id: THC_POLLUTANT_ID,
            process_id: 1,
            eng_tech_id: 10,
            hp_id: 5,
        };
        (speciation, tables, key)
    }

    /// Worker tables for process 1 with exactly `pollutants` needed.
    fn tables_needing(pollutants: &[i32]) -> NonroadWorkerTables {
        NonroadWorkerTables::new(
            [(100, 20)],
            [((5, 10), CAT)],
            pollutants.iter().map(|p| p * 100 + 1).collect::<Vec<_>>(),
        )
    }

    #[test]
    fn build_populates_both_lookup_tables() {
        let (speciation, ..) = fixture();
        assert_eq!(speciation.methane_thc_ratio.len(), 1);
        assert_eq!(speciation.hc_speciation.len(), 2);
        assert_eq!(
            speciation.speciation_constant(NMOG_POLLUTANT_ID, 1, 10, 20, CAT),
            Some(0.5),
        );
        assert_eq!(
            speciation.speciation_constant(VOC_POLLUTANT_ID, 1, 10, 20, CAT),
            Some(0.125),
        );
        // An unkeyed lookup misses.
        assert_eq!(
            speciation.speciation_constant(NMOG_POLLUTANT_ID, 1, 10, 99, CAT),
            None,
        );
    }

    #[test]
    fn build_last_row_wins_on_duplicate_key() {
        // Two rows share a methaneTHCRatio key and two share an
        // nrHCSpeciation key; the Go map keeps the last row of each.
        let speciation = NrHcSpeciation::build(
            [
                methane_row(1, 10, 20, CAT, 0.1),
                methane_row(1, 10, 20, CAT, 0.9),
            ],
            [
                hc_row(NMOG_POLLUTANT_ID, 1, 10, 20, CAT, 0.2),
                hc_row(NMOG_POLLUTANT_ID, 1, 10, 20, CAT, 0.8),
            ],
        );
        assert_eq!(speciation.methane_thc_ratio.len(), 1);
        assert_eq!(speciation.hc_speciation.len(), 1);
        assert_eq!(
            speciation.speciation_constant(NMOG_POLLUTANT_ID, 1, 10, 20, CAT),
            Some(0.8),
        );
    }

    #[test]
    fn emission_scaled_multiplies_both_quant_and_rate() {
        let e = emission(8.0, 4.0, 20, 100);
        let scaled = e.scaled(0.25);
        assert_eq!(scaled, emission(2.0, 1.0, 20, 100));
        // Fuel ids carry through; scaling by zero yields a zero emission
        // still tagged with those ids.
        assert_eq!(e.scaled(0.0), emission(0.0, 0.0, 20, 100));
    }

    #[test]
    fn speciate_emission_methane_is_thc_times_ratio() {
        let (speciation, tables, key) = fixture();
        let speciated = speciation
            .speciate_emission(&key, &emission(8.0, 4.0, 20, 100), &tables)
            .expect("speciation produced");
        // methane = THC * 0.25.
        assert_eq!(speciated.methane, Some(emission(2.0, 1.0, 20, 100)));
    }

    #[test]
    fn speciate_emission_nmhc_is_thc_times_one_minus_ratio() {
        let (speciation, tables, key) = fixture();
        let speciated = speciation
            .speciate_emission(&key, &emission(8.0, 4.0, 20, 100), &tables)
            .expect("speciation produced");
        // NMHC = THC * (1 - 0.25) = THC * 0.75.
        assert_eq!(speciated.nmhc, Some(emission(6.0, 3.0, 20, 100)));
    }

    #[test]
    fn speciate_emission_nmog_and_voc_are_nmhc_times_speciation_constant() {
        let (speciation, tables, key) = fixture();
        let speciated = speciation
            .speciate_emission(&key, &emission(8.0, 4.0, 20, 100), &tables)
            .expect("speciation produced");
        // NMHC = (6.0, 3.0); NMOG = NMHC * 0.5, VOC = NMHC * 0.125.
        assert_eq!(speciated.nmog, Some(emission(3.0, 1.5, 20, 100)));
        assert_eq!(speciated.voc, Some(emission(0.75, 0.375, 20, 100)));
    }

    #[test]
    fn speciate_emission_nmog_and_voc_are_zero_without_a_speciation_constant() {
        // Lookup tables with the ratio but no nrHCSpeciation rows at all.
        let speciation = NrHcSpeciation::build([methane_row(1, 10, 20, CAT, 0.25)], []);
        let (_, tables, key) = fixture();
        let speciated = speciation
            .speciate_emission(&key, &emission(8.0, 4.0, 20, 100), &tables)
            .expect("speciation produced");
        // No constant -> NMOG/VOC fall back to a zero emission (the Go
        // NewEmissionScaled(e, 0)), still tagged with the THC fuel ids.
        assert_eq!(speciated.nmog, Some(emission(0.0, 0.0, 20, 100)));
        assert_eq!(speciated.voc, Some(emission(0.0, 0.0, 20, 100)));
        // NMHC is unaffected — it does not need a speciation constant.
        assert_eq!(speciated.nmhc, Some(emission(6.0, 3.0, 20, 100)));
    }

    #[test]
    fn speciate_emission_tog_is_nmog_plus_methane() {
        let (speciation, tables, key) = fixture();
        let speciated = speciation
            .speciate_emission(&key, &emission(8.0, 4.0, 20, 100), &tables)
            .expect("speciation produced");
        // TOG = NMOG (3.0, 1.5) + methane (2.0, 1.0).
        assert_eq!(speciated.tog, Some(emission(5.0, 2.5, 20, 100)));
    }

    #[test]
    fn speciate_emission_tog_is_nmog_alone_when_methane_not_needed() {
        let (speciation, key) = {
            let (s, _, k) = fixture();
            (s, k)
        };
        // Methane (5) absent from the needed set; NMOG and TOG present.
        let tables = tables_needing(&[79, 80, 86]);
        let speciated = speciation
            .speciate_emission(&key, &emission(8.0, 4.0, 20, 100), &tables)
            .expect("speciation produced");
        assert_eq!(speciated.methane, None);
        // TOG = NMOG + (absent methane) = NMOG.
        assert_eq!(speciated.tog, speciated.nmog);
        assert_eq!(speciated.tog, Some(emission(3.0, 1.5, 20, 100)));
    }

    #[test]
    fn speciate_emission_tog_is_methane_alone_when_nmog_not_needed() {
        let (speciation, key) = {
            let (s, _, k) = fixture();
            (s, k)
        };
        // NMOG (80) absent from the needed set; methane and TOG present.
        let tables = tables_needing(&[5, 79, 86]);
        let speciated = speciation
            .speciate_emission(&key, &emission(8.0, 4.0, 20, 100), &tables)
            .expect("speciation produced");
        assert_eq!(speciated.nmog, None);
        // TOG = (absent NMOG) + methane = methane.
        assert_eq!(speciated.tog, speciated.methane);
        assert_eq!(speciated.tog, Some(emission(2.0, 1.0, 20, 100)));
    }

    #[test]
    fn speciate_emission_tog_absent_when_both_summands_absent() {
        let (speciation, key) = {
            let (s, _, k) = fixture();
            (s, k)
        };
        // TOG (86) needed, but neither methane nor NMOG is — the Go
        // NewEmissionSum(nil, nil) returns nil, so no TOG is produced.
        let tables = tables_needing(&[79, 86]);
        let speciated = speciation
            .speciate_emission(&key, &emission(8.0, 4.0, 20, 100), &tables)
            .expect("speciation produced");
        assert_eq!(speciated.methane, None);
        assert_eq!(speciated.nmog, None);
        assert_eq!(speciated.tog, None);
    }

    #[test]
    fn speciate_emission_needed_set_gates_each_species_independently() {
        let (speciation, key) = {
            let (s, _, k) = fixture();
            (s, k)
        };
        // Only methane is requested.
        let tables = tables_needing(&[5]);
        let speciated = speciation
            .speciate_emission(&key, &emission(8.0, 4.0, 20, 100), &tables)
            .expect("speciation produced");
        assert_eq!(speciated.methane, Some(emission(2.0, 1.0, 20, 100)));
        assert_eq!(speciated.nmhc, None);
        assert_eq!(speciated.nmog, None);
        assert_eq!(speciated.tog, None);
        assert_eq!(speciated.voc, None);
        assert_eq!(speciated.pollutant_emissions().len(), 1);
    }

    #[test]
    fn speciate_emission_nmog_operand_computed_even_when_nmhc_not_needed() {
        // Documented fidelity deviation: the needed set has NMOG but not
        // NMHC. The Go reuses the gated `emissions[79]` entry as the NMOG
        // operand and nil-panics here; this port computes the NMHC operand
        // unconditionally and produces the correct NMOG.
        let (speciation, key) = {
            let (s, _, k) = fixture();
            (s, k)
        };
        let tables = tables_needing(&[80]);
        let speciated = speciation
            .speciate_emission(&key, &emission(8.0, 4.0, 20, 100), &tables)
            .expect("speciation produced");
        assert_eq!(speciated.nmhc, None);
        // NMOG = NMHC(6.0, 3.0) * 0.5 — the operand was still computed.
        assert_eq!(speciated.nmog, Some(emission(3.0, 1.5, 20, 100)));
    }

    #[test]
    fn speciate_emission_none_when_methane_ratio_missing() {
        // Lookup tables with no methaneTHCRatio row for this key.
        let speciation =
            NrHcSpeciation::build([], [hc_row(NMOG_POLLUTANT_ID, 1, 10, 20, CAT, 0.5)]);
        let (_, tables, key) = fixture();
        assert!(speciation
            .speciate_emission(&key, &emission(8.0, 4.0, 20, 100), &tables)
            .is_none());
    }

    #[test]
    fn speciate_emission_none_when_fuel_formulation_unknown() {
        let (speciation, _, key) = fixture();
        // Worker tables that know no fuel formulations at all.
        let tables = NonroadWorkerTables::new([], [((5, 10), CAT)], [5 * 100 + 1]);
        assert!(speciation
            .speciate_emission(&key, &emission(8.0, 4.0, 20, 100), &tables)
            .is_none());
    }

    #[test]
    fn speciate_emission_uses_zero_hp_category_when_absent() {
        // The methane ratio is keyed with HP category 0; the worker tables
        // carry no NRHPCategory entry, so the lookup falls back to 0 and the
        // ratio is found.
        let speciation = NrHcSpeciation::build([methane_row(1, 10, 20, 0, 0.25)], []);
        let tables = NonroadWorkerTables::new([(100, 20)], [], [5 * 100 + 1]);
        let key = FuelBlockKey {
            pollutant_id: THC_POLLUTANT_ID,
            process_id: 1,
            eng_tech_id: 10,
            hp_id: 5,
        };
        let speciated = speciation
            .speciate_emission(&key, &emission(8.0, 4.0, 20, 100), &tables)
            .expect("speciation produced with zero HP category");
        assert_eq!(speciated.methane, Some(emission(2.0, 1.0, 20, 100)));
    }

    #[test]
    fn methane_ratio_keys_on_formulation_subtype_speciation_keys_on_emission_subtype() {
        // The emission's fuel subtype (20) differs from its formulation's
        // (30). The methaneTHCRatio lookup must use the formulation's (30);
        // the nrHCSpeciation lookup must use the emission's (20).
        let speciation = NrHcSpeciation::build(
            [methane_row(1, 10, 30, CAT, 0.25)],
            [hc_row(NMOG_POLLUTANT_ID, 1, 10, 20, CAT, 0.5)],
        );
        let tables = NonroadWorkerTables::new(
            [(100, 30)], // formulation 100 -> subtype 30
            [((5, 10), CAT)],
            [5 * 100 + 1, 79 * 100 + 1, 80 * 100 + 1],
        );
        let key = FuelBlockKey {
            pollutant_id: THC_POLLUTANT_ID,
            process_id: 1,
            eng_tech_id: 10,
            hp_id: 5,
        };
        // Emission tagged with subtype 20.
        let speciated = speciation
            .speciate_emission(&key, &emission(8.0, 4.0, 20, 100), &tables)
            .expect("ratio found via the formulation subtype");
        // Ratio found (keyed 30) -> methane computed; NMOG constant found
        // (keyed 20) -> NMOG is the scaled value, not the zero fallback.
        assert_eq!(speciated.methane, Some(emission(2.0, 1.0, 20, 100)));
        assert_eq!(speciated.nmog, Some(emission(3.0, 1.5, 20, 100)));
    }

    #[test]
    fn speciate_block_groups_emissions_by_pollutant() {
        let (speciation, tables, key) = fixture();
        let block = FuelBlock {
            key,
            emissions: vec![emission(8.0, 4.0, 20, 100)],
        };
        let blocks = speciation.speciate_block(&block, &tables);
        // One block per output species: 5, 79, 80, 86, 87.
        let pollutants: Vec<i32> = blocks.iter().map(|b| b.pollutant_id).collect();
        assert_eq!(pollutants, vec![5, 79, 80, 86, 87]);
        // Each output block carries exactly the one speciated emission.
        for b in &blocks {
            assert_eq!(b.emissions.len(), 1);
        }
    }

    #[test]
    fn speciate_block_pol_process_id_is_pollutant_times_100_plus_process() {
        let (speciation, tables, key) = fixture();
        let block = FuelBlock {
            key,
            emissions: vec![emission(8.0, 4.0, 20, 100)],
        };
        for b in speciation.speciate_block(&block, &tables) {
            assert_eq!(b.pol_process_id, b.pollutant_id * 100 + 1);
        }
    }

    #[test]
    fn speciate_block_returns_empty_for_a_non_thc_block() {
        let (speciation, tables, key) = fixture();
        // A block of some other pollutant — the Go skips it entirely.
        let block = FuelBlock {
            key: FuelBlockKey {
                pollutant_id: 3,
                ..key
            },
            emissions: vec![emission(8.0, 4.0, 20, 100)],
        };
        assert!(speciation.speciate_block(&block, &tables).is_empty());
    }

    #[test]
    fn speciate_block_accumulates_multiple_emissions_in_input_order() {
        let (speciation, tables, key) = fixture();
        // Two THC emissions from two fuel formulations (both mapping to
        // subtype 20 so both speciate).
        let block = FuelBlock {
            key,
            emissions: vec![emission(8.0, 4.0, 20, 100), emission(16.0, 8.0, 20, 100)],
        };
        let blocks = speciation.speciate_block(&block, &tables);
        let methane = blocks
            .iter()
            .find(|b| b.pollutant_id == METHANE_POLLUTANT_ID)
            .expect("methane block");
        // Both emissions speciated, in input order: 8*0.25 then 16*0.25.
        assert_eq!(
            methane.emissions,
            vec![emission(2.0, 1.0, 20, 100), emission(4.0, 2.0, 20, 100)],
        );
    }

    #[test]
    fn speciate_block_skips_an_emission_with_no_matching_ratio() {
        let (speciation, tables, key) = fixture();
        // First emission speciates (formulation 100 -> subtype 20, ratio
        // keyed 20); the second uses an unknown formulation and is skipped.
        let block = FuelBlock {
            key,
            emissions: vec![emission(8.0, 4.0, 20, 100), emission(8.0, 4.0, 20, 999)],
        };
        let blocks = speciation.speciate_block(&block, &tables);
        let methane = blocks
            .iter()
            .find(|b| b.pollutant_id == METHANE_POLLUTANT_ID)
            .expect("methane block");
        assert_eq!(methane.emissions, vec![emission(2.0, 1.0, 20, 100)]);
    }

    #[test]
    fn calculator_metadata() {
        let calc = NrHcSpeciationCalculator::new();
        assert_eq!(calc.name(), "NRHCSpeciationCalculator");
        // Chained calculator — no direct master-loop subscription.
        assert!(calc.subscriptions().is_empty());
        assert_eq!(calc.upstream(), &["NonroadEmissionCalculator"]);
        assert!(calc.input_tables().contains(&"nrHCSpeciation"));
        assert!(calc.input_tables().contains(&"nrMethaneTHCRatio"));
    }

    #[test]
    fn calculator_registers_45_pollutant_process_pairs() {
        let calc = NrHcSpeciationCalculator::new();
        let regs = calc.registrations();
        assert_eq!(regs.len(), 45);
        // The five output species across the nine THC-emitting processes.
        let species = [5_u16, 79, 80, 86, 87];
        let processes = [1_u16, 15, 18, 19, 20, 21, 30, 31, 32];
        for &p in &processes {
            for &s in &species {
                assert!(
                    regs.contains(&reg(s, p)),
                    "missing registration for pollutant {s} process {p}",
                );
            }
        }
    }

    #[test]
    fn execute_wires_through_data_plane() {
        use moves_framework::DataFrameStore;
        // Fixture from fixture(): process 1, engTech 10, fuel_sub_type 20,
        // formulation 100, hp_id 5, category b'A'=65.
        // methane ratio 0.25, NMOG constant 0.5, VOC constant 0.125.
        // Expected: methane=2.0, NMHC=6.0, NMOG=3.0, TOG=5.0, VOC=0.75.
        let worker_rows = vec![NrThcWorkerRow {
            year_id: 2020,
            month_id: 7,
            day_id: 5,
            hour_id: 8,
            state_id: 26,
            county_id: 26_161,
            zone_id: 261_610,
            link_id: 2_616_101,
            pollutant_id: THC_POLLUTANT_ID,
            process_id: 1,
            source_type_id: 0,
            road_type_id: 4,
            sector_id: 1,
            eng_tech_id: 10,
            hp_id: 5,
            fuel_sub_type_id: 20,
            fuel_formulation_id: 100,
            emission_quant: 8.0,
            emission_rate: 4.0,
        }];
        let methane_rows = vec![MethaneThcRatioRow {
            process_id: 1,
            eng_tech_id: 10,
            fuel_sub_type_id: 20,
            nr_hp_category: CAT,
            ch4_thc_ratio: 0.25,
        }];
        let hc_rows = vec![
            NrHcSpeciationRow {
                pollutant_id: NMOG_POLLUTANT_ID,
                process_id: 1,
                eng_tech_id: 10,
                fuel_sub_type_id: 20,
                nr_hp_category: CAT,
                speciation_constant: 0.5,
            },
            NrHcSpeciationRow {
                pollutant_id: VOC_POLLUTANT_ID,
                process_id: 1,
                eng_tech_id: 10,
                fuel_sub_type_id: 20,
                nr_hp_category: CAT,
                speciation_constant: 0.125,
            },
        ];
        let ff_rows = vec![NrFuelFormulationRow {
            fuel_formulation_id: 100,
            fuel_sub_type_id: 20,
        }];
        let hp_rows = vec![NrHpCategoryRow {
            hp_id: 5,
            eng_tech_id: 10,
            nr_hp_category: i32::from(CAT),
        }];
        let mut store = moves_framework::InMemoryStore::new();
        store.insert(
            "MOVESWorkerOutput",
            NrThcWorkerRow::into_dataframe(worker_rows).unwrap(),
        );
        store.insert(
            "nrMethaneTHCRatio",
            MethaneThcRatioRow::into_dataframe(methane_rows).unwrap(),
        );
        store.insert(
            "nrHCSpeciation",
            NrHcSpeciationRow::into_dataframe(hc_rows).unwrap(),
        );
        store.insert(
            "FuelFormulation",
            NrFuelFormulationRow::into_dataframe(ff_rows).unwrap(),
        );
        store.insert(
            "nrHPCategory",
            NrHpCategoryRow::into_dataframe(hp_rows).unwrap(),
        );
        let ctx = CalculatorContext::with_tables(store);
        let out = NrHcSpeciationCalculator::new()
            .execute(&ctx)
            .expect("execute ok");
        let df = out.dataframe().expect("output should contain a DataFrame");
        // 5 speciated species: methane(5), NMHC(79), NMOG(80), TOG(86), VOC(87)
        assert_eq!(df.height(), 5, "one THC row should yield 5 speciated rows");
        let pollutants: Vec<i32> = df
            .column("pollutantID")
            .unwrap()
            .i32()
            .unwrap()
            .into_iter()
            .map(|v| v.unwrap())
            .collect();
        let quants: Vec<f64> = df
            .column("emissionQuant")
            .unwrap()
            .f64()
            .unwrap()
            .into_iter()
            .map(|v| v.unwrap())
            .collect();
        let find = |pid: i32| -> f64 {
            pollutants
                .iter()
                .zip(quants.iter())
                .find(|(&p, _)| p == pid)
                .map(|(_, &q)| q)
                .unwrap()
        };
        assert!((find(METHANE_POLLUTANT_ID) - 2.0).abs() < 1e-9, "methane");
        assert!((find(NMHC_POLLUTANT_ID) - 6.0).abs() < 1e-9, "NMHC");
        assert!((find(NMOG_POLLUTANT_ID) - 3.0).abs() < 1e-9, "NMOG");
        assert!((find(TOG_POLLUTANT_ID) - 5.0).abs() < 1e-9, "TOG");
        assert!((find(VOC_POLLUTANT_ID) - 0.75).abs() < 1e-9, "VOC");
    }

    #[test]
    fn calculator_is_object_safe() {
        // The registry stores calculators as Box<dyn Calculator>.
        let calc: Box<dyn Calculator> = Box::new(NrHcSpeciationCalculator::new());
        assert_eq!(calc.name(), "NRHCSpeciationCalculator");
        assert_eq!(calc.registrations().len(), 45);
    }
}
