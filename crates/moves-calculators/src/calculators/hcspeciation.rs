//! Port of `calc/hcspeciation/hcspeciation.go` — the onroad
//! `HCSpeciationCalculator`, which speciates total hydrocarbons into the five
//! hydrocarbon species MOVES reports separately.
//!
//! Migration plan: Phase 3, Task 48. The Nonroad counterpart, Task 49
//! (`NRHCSpeciationCalculator`), is the [`super::nrhcspeciation`] module — see
//! "Relationship to the Nonroad calculator" below for how the two differ.
//!
//! # What this calculator does
//!
//! Onroad emission calculators produce total hydrocarbons (THC, pollutant 1).
//! `HCSpeciationCalculator` splits that THC tally into the five hydrocarbon
//! species MOVES reports separately:
//!
//! * methane — `CH4`, pollutant 5;
//! * non-methane hydrocarbons — NMHC, pollutant 79;
//! * non-methane organic gases — NMOG, pollutant 80;
//! * total organic gases — TOG, pollutant 86;
//! * volatile organic compounds — VOC, pollutant 87.
//!
//! It speciates THC for every onroad process that emits it — running and
//! start exhaust, the three evaporative processes, the two refueling
//! processes, and extended-idle and auxiliary-power exhaust (see
//! [`registrations`](Calculator::registrations)).
//!
//! # The algorithm
//!
//! For one THC emission, with `r = CH4THCRatio` from the `methaneTHCRatio`
//! table and a speciation `(speciationConstant, oxySpeciation)` pair from the
//! `HCSpeciation` table:
//!
//! ```text
//! methane = THC  * r
//! NMHC    = THC  * (1 - r)
//! factor  = speciationConstant + oxySpeciation * volToWtPercentOxy * totalOxygenate
//! NMOG    = NMHC * factor(NMOG)   ( = 0 when no HCSpeciation row matches)
//! VOC     = NMHC * factor(VOC)    ( = 0 when no HCSpeciation row matches)
//! TOG     = NMOG + methane
//! ```
//!
//! `totalOxygenate` is the fuel formulation's `MTBEVolume + ETBEVolume +
//! TAMEVolume + ETOHVolume`; `volToWtPercentOxy` is another formulation
//! property. Each emission carries both an emission quantity and an emission
//! rate; every scaling above multiplies *both*, and the TOG sum adds *both*.
//!
//! # The two lookup tables
//!
//! * `methaneTHCRatio` — the methane-to-THC ratio, keyed by `(processID,
//!   fuelSubtypeID, regClassID, modelYearID)`. Its `CreateDefault.sql` rows
//!   carry a `(beginModelYearID, endModelYearID)` model-year *range*;
//!   [`MethaneThcRatioRow`] is one such row and [`HcSpeciation::build`]
//!   expands the range to one map entry per model year, exactly as the Go
//!   `StartSetup` does.
//! * `HCSpeciation` — the NMOG/VOC `(speciationConstant, oxySpeciation)`
//!   pairs, keyed by `(polProcessID, fuelSubtypeID, regClassID,
//!   modelYearID)`, again from a model-year-range row ([`HcSpeciationRow`]).
//!
//! # The E10 `altTHC` / `altNMHC` special case
//!
//! Ethanol-fueled (`fuelTypeID` 5) running- and start-exhaust emissions of
//! 2001-and-later model years on the E70/E85 fuel subtypes (50, 51, 52) are
//! speciated from a parallel `altTHC` tally (pollutant 10001) instead of the
//! ordinary THC: such vehicles burn E10-like gasoline blends in practice, so
//! their NMHC/NMOG/VOC are computed with E10's ratios. For those emissions
//! the ordinary THC block still yields methane and NMHC, while the `altTHC`
//! block yields `altNMHC` (pollutant 10079) and the NMOG/VOC speciated from
//! it — both lookups keyed on the E10 fuel subtype (12) rather than the
//! emission's own. The two paths are mutually exclusive per emission, so no
//! double counting occurs.
//!
//! # Relationship to the Nonroad calculator
//!
//! The Nonroad [`NRHCSpeciationCalculator`](super::nrhcspeciation) shares the
//! methane/NMHC/NMOG/TOG/VOC shape but is simpler: its keys carry no model
//! year, its speciation constant is used directly with no oxygenate term, and
//! it has no E10 `altTHC` path. This onroad port adds all three.
//!
//! # Scope of this port — Go, not SQL
//!
//! `HCSpeciationCalculator` has two implementations in the pin: the legacy
//! scripted `database/HCSpeciationCalculator.sql` and the modern worker
//! `calc/hcspeciation/hcspeciation.go`. The SQL script is marked
//! `-- @fileNotUsed` and references an *older* schema of `methaneTHCRatio` /
//! `HCSpeciation` (`fuelTypeID, sourceTypeID, modelYearGroupID, ageGroupID` /
//! `oxyThreshID, etohThreshID, fuelMYGroupID`) that no longer matches
//! `CreateDefault.sql`. The Go worker reads the *current* schema. This port
//! therefore mirrors the **Go** file; the SQL is reference only.
//!
//! The Go ran `calculate` as a pool of goroutines draining a channel of
//! `MWOBlock`s; that worker plumbing is not part of the calculation and is
//! dropped. This port keeps the **computation** — the two lookups, the five
//! speciation formulas, the E10 `altTHC` path, the per-pollutant grouping
//! into new fuel blocks — and replaces the channel boundary with plain
//! values: [`FuelBlock`]s in, [`SpeciatedFuelBlock`]s out.
//!
//! # Fidelity notes
//!
//! * **SQL `+ 10` vs Go `ETOHVolume`.** The legacy SQL computes the E10 path's
//!   oxygenate sum as `MTBEVolume + ETBEVolume + TAMEVolume + 10` (a hardcoded
//!   10% ethanol). The Go worker uses the formulation's actual `ETOHVolume`,
//!   the same `totalOxygenate` as the ordinary path. This port follows the Go.
//! * **NMOG/VOC operand.** The Go computes `emissions[79]` (NMHC) only when
//!   NMHC output is requested, yet reuses that gated map entry as the operand
//!   for the NMOG and VOC formulas. A run that requests NMOG or VOC without
//!   NMHC therefore dereferences a nil pointer in the Go. This port computes
//!   the NMHC operand value unconditionally (likewise the `altNMHC` operand);
//!   the NMHC / `altNMHC` *outputs* are still gated. For every needed-set
//!   closed under the NMOG/VOC → NMHC dependency — i.e. every real MOVES run,
//!   because the pollutant chain pulls NMHC in as an intermediate whenever
//!   NMOG or VOC is requested — the result is numerically identical, and the
//!   degenerate set yields the correct number instead of a crash.
//! * **Output order.** The Go grouped output emissions into new fuel blocks
//!   keyed in a Go `map`, whose iteration order is randomised.
//!   [`HcSpeciation::speciate_block`] returns the blocks in ascending
//!   pollutant-id order so the output is deterministic; a fuel-block set is
//!   unordered, so this is a presentation choice only.
//!
//! # Data plane (Task 50)
//!
//! [`Calculator::execute`] receives a [`CalculatorContext`] whose
//! `ExecutionTables` / `ScratchNamespace` are Phase-2 placeholders until the
//! `DataFrameStore` lands (migration-plan Task 50), so `execute` cannot yet
//! read the `methaneTHCRatio` / `HCSpeciation` tables nor the upstream THC
//! emission blocks, nor write the speciated blocks back. The numerically
//! faithful algorithm is fully ported and unit-tested on [`HcSpeciation`];
//! once the data plane exists, `execute` builds an [`HcSpeciation`] from
//! `ctx.tables()`, reads the THC and `altTHC` [`FuelBlock`]s, applies
//! [`speciate_block`](HcSpeciation::speciate_block), and stores the resulting
//! [`SpeciatedFuelBlock`]s.

use std::collections::{BTreeMap, HashMap, HashSet};

use moves_data::{PollutantId, PollutantProcessAssociation, ProcessId};
use moves_framework::{
    Calculator, CalculatorContext, CalculatorOutput, CalculatorSubscription, Error,
};

/// THC — total hydrocarbons, pollutant 1. The ordinary input pollutant: its
/// fuel blocks speciate into methane, NMHC, NMOG, TOG and VOC.
const THC_POLLUTANT_ID: i32 = 1;
/// `altTHC` — pollutant 10001. The parallel THC tally used to speciate
/// ethanol-fueled E70/E85 2001+ running/start emissions; see the module docs.
const ALT_THC_POLLUTANT_ID: i32 = 10001;
/// Methane (`CH4`), pollutant 5 — `THC * CH4THCRatio`.
const METHANE_POLLUTANT_ID: i32 = 5;
/// Non-methane hydrocarbons, pollutant 79 — `THC * (1 - CH4THCRatio)`.
const NMHC_POLLUTANT_ID: i32 = 79;
/// `altNMHC` — pollutant 10079. NMHC speciated from `altTHC` on the E10 path.
const ALT_NMHC_POLLUTANT_ID: i32 = 10079;
/// Non-methane organic gases, pollutant 80 — `NMHC * factor`.
const NMOG_POLLUTANT_ID: i32 = 80;
/// Total organic gases, pollutant 86 — `NMOG + methane`.
const TOG_POLLUTANT_ID: i32 = 86;
/// Volatile organic compounds, pollutant 87 — `NMHC * factor`.
const VOC_POLLUTANT_ID: i32 = 87;

/// Running Exhaust, process 1 — one of the two processes on the E10 `altTHC`
/// path.
const RUNNING_EXHAUST_PROCESS_ID: i32 = 1;
/// Start Exhaust, process 2 — the other process on the E10 `altTHC` path.
const START_EXHAUST_PROCESS_ID: i32 = 2;
/// Ethanol fuel type (`fuelTypeID` 5) — required for the E10 `altTHC` path.
const ETHANOL_FUEL_TYPE_ID: i32 = 5;
/// The E70/E85 fuel subtypes (50, 51, 52) that take the E10 `altTHC` path.
const E70_E85_FUEL_SUBTYPE_IDS: [i32; 3] = [50, 51, 52];
/// The E10 fuel subtype (12) — the `methaneTHCRatio` / `HCSpeciation` lookups
/// on the `altTHC` path key on this subtype rather than the emission's own.
const E10_FUEL_SUBTYPE_ID: i32 = 12;
/// Earliest model year on the E10 `altTHC` path (2001 and later).
const ALT_THC_MIN_MODEL_YEAR: i32 = 2001;

/// One emission record — the Go `mwo.MWOEmission`, restricted to the fields
/// the speciation calculator reads and writes.
///
/// An emission carries a quantity and a rate; the speciation formulas scale or
/// sum *both*. `fuel_sub_type_id` and `fuel_formulation_id` identify the fuel
/// the emission belongs to: the subtype keys both lookup tables and the
/// formulation supplies the oxygenate volumes. Both are carried through
/// unchanged onto every speciated emission derived from this one.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Emission {
    /// `fuelSubTypeID` — the emission's fuel subtype. Keys the
    /// `methaneTHCRatio` and `HCSpeciation` lookups on the ordinary THC path.
    pub fuel_sub_type_id: i32,
    /// `fuelFormulationID` — the emission's fuel formulation; selects the
    /// [`FuelFormulation`] supplying the oxygenate volumes.
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
/// `None` is treated as a zero contribution: with both present the quantities
/// and rates add and the fuel ids come from `a`; with one present the other is
/// copied; with neither present the result is `None` (the Go returns a nil
/// `*MWOEmission`, so no TOG emission is produced).
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

/// The fuel-formulation properties the speciation calculator reads — the Go
/// `mwo.FuelFormulation`, restricted to the oxygenate fields.
///
/// `volToWtPercentOxy` and the four oxygenate volumes feed the NMOG/VOC
/// speciation `factor`; nothing else of the formulation is consulted.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FuelFormulation {
    /// `MTBEVolume` — methyl tert-butyl ether volume percent.
    pub mtbe_volume: f64,
    /// `ETBEVolume` — ethyl tert-butyl ether volume percent.
    pub etbe_volume: f64,
    /// `TAMEVolume` — tert-amyl methyl ether volume percent.
    pub tame_volume: f64,
    /// `ETOHVolume` — ethanol volume percent.
    pub etoh_volume: f64,
    /// `volToWtPercentOxy` — volume-to-weight oxygen conversion factor.
    pub vol_to_wt_percent_oxy: f64,
}

impl FuelFormulation {
    /// `totalOxygenate` — `MTBEVolume + ETBEVolume + TAMEVolume + ETOHVolume`.
    #[must_use]
    pub fn total_oxygenate(&self) -> f64 {
        self.mtbe_volume + self.etbe_volume + self.tame_volume + self.etoh_volume
    }
}

/// Key of the `methaneTHCRatio` lookup — the Go `methaneTHCRatioKey`, with the
/// model-year range expanded to a single model year.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct MethaneThcRatioKey {
    /// `processID`.
    process_id: i32,
    /// `fuelSubtypeID`.
    fuel_sub_type_id: i32,
    /// `regClassID`.
    reg_class_id: i32,
    /// `modelYearID` — one year within the row's model-year range.
    model_year_id: i32,
}

/// Key of the `HCSpeciation` lookup — the Go `HCSpeciationKey`, with the
/// model-year range expanded to a single model year.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct HcSpeciationKey {
    /// `polProcessID` — `pollutantID * 100 + processID` for NMOG (80) or
    /// VOC (87).
    pol_process_id: i32,
    /// `fuelSubtypeID`.
    fuel_sub_type_id: i32,
    /// `regClassID`.
    reg_class_id: i32,
    /// `modelYearID` — one year within the row's model-year range.
    model_year_id: i32,
}

/// The `(speciationConstant, oxySpeciation)` pair of one `HCSpeciation` row —
/// the Go `HCSpeciationDetail`.
#[derive(Debug, Clone, Copy, PartialEq)]
struct HcSpeciationDetail {
    /// `speciationConstant` — the base NMOG/VOC speciation multiplier.
    speciation_constant: f64,
    /// `oxySpeciation` — the oxygenate-sensitive part of the multiplier.
    oxy_speciation: f64,
}

/// One `methaneTHCRatio` table row — input to [`HcSpeciation::build`].
///
/// Mirrors the `CreateDefault.sql` `methaneTHCRatio` columns the Go reads:
/// `processID, fuelSubtypeID, regClassID, beginModelYearID, endModelYearID,
/// CH4THCRatio` (the trailing `dataSourceID` is not used by the calculation).
/// `build` expands `begin_model_year_id..=end_model_year_id` to one lookup
/// entry per model year.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MethaneThcRatioRow {
    /// `processID`.
    pub process_id: i32,
    /// `fuelSubtypeID`.
    pub fuel_sub_type_id: i32,
    /// `regClassID`.
    pub reg_class_id: i32,
    /// `beginModelYearID` — first model year the row covers.
    pub begin_model_year_id: i32,
    /// `endModelYearID` — last model year the row covers (inclusive).
    pub end_model_year_id: i32,
    /// `CH4THCRatio` — the methane-to-THC ratio.
    pub ch4_thc_ratio: f64,
}

/// One `HCSpeciation` table row — input to [`HcSpeciation::build`].
///
/// Mirrors the `CreateDefault.sql` `HCSpeciation` columns the Go reads:
/// `polProcessID, fuelSubtypeID, regClassID, beginModelYearID,
/// endModelYearID, speciationConstant, oxySpeciation` (the trailing
/// `dataSourceID` is not used by the calculation). `build` expands
/// `begin_model_year_id..=end_model_year_id` to one lookup entry per model
/// year.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HcSpeciationRow {
    /// `polProcessID` — `pollutantID * 100 + processID`, for NMOG (80) or
    /// VOC (87).
    pub pol_process_id: i32,
    /// `fuelSubtypeID`.
    pub fuel_sub_type_id: i32,
    /// `regClassID`.
    pub reg_class_id: i32,
    /// `beginModelYearID` — first model year the row covers.
    pub begin_model_year_id: i32,
    /// `endModelYearID` — last model year the row covers (inclusive).
    pub end_model_year_id: i32,
    /// `speciationConstant` — the base NMOG/VOC speciation multiplier.
    pub speciation_constant: f64,
    /// `oxySpeciation` — the oxygenate-sensitive part of the multiplier.
    pub oxy_speciation: f64,
}

/// The key fields of an MWO `FuelBlock` that the HC speciation calculator
/// reads — a subset of the Go `mwo.MWOKey`.
///
/// The Go `calculate` reads only these five fields of the input block's key;
/// the rest of `MWOKey` (geography, time, source type, …) is opaque
/// passthrough that the data-plane integration copies onto the output blocks,
/// so it is not modeled here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FuelBlockKey {
    /// `pollutantID` — the calculator speciates only THC (1) and `altTHC`
    /// (10001) blocks.
    pub pollutant_id: i32,
    /// `processID`.
    pub process_id: i32,
    /// `fuelTypeID` — gates the E10 `altTHC` path (ethanol is 5).
    pub fuel_type_id: i32,
    /// `regClassID` — keys both lookup tables.
    pub reg_class_id: i32,
    /// `modelYearID` — keys both lookup tables and gates the E10 `altTHC`
    /// path (2001 and later).
    pub model_year_id: i32,
}

/// One input fuel block — the Go `mwo.FuelBlock`, restricted to the key fields
/// and emissions the calculator consumes.
///
/// The calculator speciates the emissions of a THC (pollutant 1) or `altTHC`
/// (pollutant 10001) block; a block of any other pollutant is ignored.
#[derive(Debug, Clone, PartialEq)]
pub struct FuelBlock {
    /// The block's key fields.
    pub key: FuelBlockKey,
    /// The per-fuel-formulation emissions in the block.
    pub emissions: Vec<Emission>,
}

/// The speciated emissions produced from one input [`Emission`].
///
/// Each field is the emission for one output pollutant, or `None` when that
/// pollutant is not in the run's needed set (or, for TOG, when neither of its
/// NMOG and methane summands is present). A THC-block emission yields
/// methane / NMHC / NMOG / TOG / VOC; an `altTHC`-block emission yields
/// `altNMHC` / NMOG / TOG / VOC — the two paths never both fire for one
/// emission, so the unused fields stay `None`.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct SpeciatedEmission {
    /// Methane (pollutant 5) — THC path only.
    pub methane: Option<Emission>,
    /// Non-methane hydrocarbons (pollutant 79) — THC path only.
    pub nmhc: Option<Emission>,
    /// `altNMHC` (pollutant 10079) — `altTHC` path only.
    pub alt_nmhc: Option<Emission>,
    /// Non-methane organic gases (pollutant 80).
    pub nmog: Option<Emission>,
    /// Total organic gases (pollutant 86).
    pub tog: Option<Emission>,
    /// Volatile organic compounds (pollutant 87).
    pub voc: Option<Emission>,
}

impl SpeciatedEmission {
    /// The present `(pollutant_id, emission)` pairs, in ascending pollutant-id
    /// order (5, 79, 80, 86, 87, 10079).
    #[must_use]
    pub fn pollutant_emissions(&self) -> Vec<(i32, Emission)> {
        [
            (METHANE_POLLUTANT_ID, self.methane),
            (NMHC_POLLUTANT_ID, self.nmhc),
            (NMOG_POLLUTANT_ID, self.nmog),
            (TOG_POLLUTANT_ID, self.tog),
            (VOC_POLLUTANT_ID, self.voc),
            (ALT_NMHC_POLLUTANT_ID, self.alt_nmhc),
        ]
        .into_iter()
        .filter_map(|(id, emission)| emission.map(|e| (id, e)))
        .collect()
    }
}

/// One output fuel block — a single speciated pollutant's emissions.
///
/// The Go `calculate` produced these by copying the input block's key,
/// overwriting `pollutantID` and `polProcessID`, and attaching the speciated
/// emissions. This port returns the two computed key fields plus the
/// emissions; copying the rest of the input key is data-plane plumbing the
/// caller handles.
#[derive(Debug, Clone, PartialEq)]
pub struct SpeciatedFuelBlock {
    /// `pollutantID` of the speciated species (5, 79, 80, 86, 87 or 10079).
    pub pollutant_id: i32,
    /// `polProcessID` — `pollutantID * 100 + processID`.
    pub pol_process_id: i32,
    /// The speciated emissions, one per input emission that produced this
    /// species, in input-emission order.
    pub emissions: Vec<Emission>,
}

/// The ambient onroad-worker lookup tables the calculator consults but does
/// not own — the Go `mwo` package globals.
///
/// `methaneTHCRatio` and `HCSpeciation` are loaded by the calculator itself
/// ([`HcSpeciation`]); the tables here are populated elsewhere in the worker
/// setup and shared across calculators:
///
/// * `FuelFormulations` — the calculator reads each formulation's oxygenate
///   volumes, kept as a [`FuelFormulation`] per `fuelFormulationID`;
/// * `NeededPolProcessIDs` — the set of `polProcessID`s the run requires,
///   which gates which output pollutants are computed.
#[derive(Debug, Clone, Default)]
pub struct OnroadWorkerTables {
    /// `mwo.FuelFormulations` — `fuelFormulationID` → oxygenate properties.
    fuel_formulations: HashMap<i32, FuelFormulation>,
    /// `mwo.NeededPolProcessIDs` — the set of needed `polProcessID`s.
    needed_pol_process_ids: HashSet<i32>,
}

impl OnroadWorkerTables {
    /// Assemble the worker tables from their two inputs.
    ///
    /// * `fuel_formulations` — `(fuelFormulationID, FuelFormulation)` pairs;
    /// * `needed_pol_process_ids` — the needed `polProcessID`s.
    #[must_use]
    pub fn new(
        fuel_formulations: impl IntoIterator<Item = (i32, FuelFormulation)>,
        needed_pol_process_ids: impl IntoIterator<Item = i32>,
    ) -> Self {
        Self {
            fuel_formulations: fuel_formulations.into_iter().collect(),
            needed_pol_process_ids: needed_pol_process_ids.into_iter().collect(),
        }
    }

    /// The oxygenate properties of a fuel formulation, or `None` when the
    /// formulation is unknown — the Go `mwo.FuelFormulations[id]` returning
    /// nil, which makes `calculate` skip the emission.
    fn fuel_formulation(&self, fuel_formulation_id: i32) -> Option<&FuelFormulation> {
        self.fuel_formulations.get(&fuel_formulation_id)
    }

    /// Whether `pollutantID * 100 + processID` is in the run's needed set —
    /// the Go `mwo.NeededPolProcessIDs[ppid]`.
    fn is_needed(&self, pollutant_id: i32, process_id: i32) -> bool {
        self.needed_pol_process_ids
            .contains(&(pollutant_id * 100 + process_id))
    }
}

/// The HC speciation lookup tables and the speciation algorithm — the
/// in-memory state and `calculate` body of the Go `hcspeciation` package.
#[derive(Debug, Clone, Default)]
pub struct HcSpeciation {
    /// `methaneTHCRatio` — methane-to-THC ratios, one entry per model year.
    methane_thc_ratio: HashMap<MethaneThcRatioKey, f64>,
    /// `HCSpeciation` — NMOG/VOC speciation pairs, one entry per model year.
    hc_speciation: HashMap<HcSpeciationKey, HcSpeciationDetail>,
}

impl HcSpeciation {
    /// Build the lookup tables from `methaneTHCRatio` and `HCSpeciation` table
    /// rows — the in-memory half of the Go `StartSetup`.
    ///
    /// Each row's `[begin_model_year_id, end_model_year_id]` range is expanded
    /// to one map entry per model year, exactly as the Go `StartSetup` loop
    /// does; a row whose begin exceeds its end contributes nothing. When two
    /// expanded entries share a key the last one wins. The Go logs a
    /// diagnostic to stdout on a duplicate `HCSpeciation` key and then
    /// overwrites (`methaneTHCRatio` overwrites silently); either way the
    /// resulting map keeps the last entry, which this port reproduces without
    /// the log.
    #[must_use]
    pub fn build(
        methane_thc_ratio_rows: impl IntoIterator<Item = MethaneThcRatioRow>,
        hc_speciation_rows: impl IntoIterator<Item = HcSpeciationRow>,
    ) -> Self {
        let mut methane_thc_ratio = HashMap::new();
        for row in methane_thc_ratio_rows {
            for model_year_id in row.begin_model_year_id..=row.end_model_year_id {
                methane_thc_ratio.insert(
                    MethaneThcRatioKey {
                        process_id: row.process_id,
                        fuel_sub_type_id: row.fuel_sub_type_id,
                        reg_class_id: row.reg_class_id,
                        model_year_id,
                    },
                    row.ch4_thc_ratio,
                );
            }
        }

        let mut hc_speciation = HashMap::new();
        for row in hc_speciation_rows {
            for model_year_id in row.begin_model_year_id..=row.end_model_year_id {
                hc_speciation.insert(
                    HcSpeciationKey {
                        pol_process_id: row.pol_process_id,
                        fuel_sub_type_id: row.fuel_sub_type_id,
                        reg_class_id: row.reg_class_id,
                        model_year_id,
                    },
                    HcSpeciationDetail {
                        speciation_constant: row.speciation_constant,
                        oxy_speciation: row.oxy_speciation,
                    },
                );
            }
        }

        Self {
            methane_thc_ratio,
            hc_speciation,
        }
    }

    /// The `CH4THCRatio` for a `methaneTHCRatio` key, or `None` when no row
    /// matches — the Go `methaneTHCRatio[...]` returning nil.
    fn methane_ratio(&self, key: &MethaneThcRatioKey) -> Option<f64> {
        self.methane_thc_ratio.get(key).copied()
    }

    /// The `(speciationConstant, oxySpeciation)` pair for an `HCSpeciation`
    /// key, or `None` when no row matches — the Go `HCSpeciation[...]`
    /// returning nil, which makes the NMOG/VOC formula fall back to a zero
    /// emission.
    fn speciation_detail(&self, key: &HcSpeciationKey) -> Option<HcSpeciationDetail> {
        self.hc_speciation.get(key).copied()
    }

    /// Speciate one NMOG (80) or VOC (87) output from its NMHC operand — the
    /// inner `emissions[80]` / `emissions[87]` assignment of the Go
    /// `calculate`, shared by the ordinary THC and the E10 `altTHC` paths.
    ///
    /// The `HCSpeciation` lookup keys on `speciation_fuel_sub_type_id` — the
    /// emission's own subtype on the THC path, the E10 subtype (12) on the
    /// `altTHC` path. With a row found, the output is `operand` (NMHC or
    /// `altNMHC`) scaled by `speciationConstant + oxySpeciation *
    /// volToWtPercentOxy * totalOxygenate`; with no row found, it is the
    /// original THC emission scaled by zero (a zero emission still tagged
    /// with the THC fuel ids, the Go `NewEmissionScaled(e, 0)`).
    #[allow(clippy::too_many_arguments)]
    fn speciate_nmog_or_voc(
        &self,
        output_pollutant_id: i32,
        process_id: i32,
        speciation_fuel_sub_type_id: i32,
        reg_class_id: i32,
        model_year_id: i32,
        formulation: &FuelFormulation,
        total_oxygenate: f64,
        operand: &Emission,
        thc_emission: &Emission,
    ) -> Emission {
        let key = HcSpeciationKey {
            pol_process_id: output_pollutant_id * 100 + process_id,
            fuel_sub_type_id: speciation_fuel_sub_type_id,
            reg_class_id,
            model_year_id,
        };
        match self.speciation_detail(&key) {
            Some(detail) => {
                let factor = detail.speciation_constant
                    + detail.oxy_speciation * formulation.vol_to_wt_percent_oxy * total_oxygenate;
                operand.scaled(factor)
            }
            None => thc_emission.scaled(0.0),
        }
    }

    /// Speciate one input [`Emission`] into its methane / NMHC / `altNMHC` /
    /// NMOG / TOG / VOC species — the inner per-emission body of the Go
    /// `calculate`.
    ///
    /// `block_key` is the key of the fuel block the emission belongs to; the
    /// calculator reads `pollutant_id`, `process_id`, `fuel_type_id`,
    /// `reg_class_id` and `model_year_id` from it. A THC (1) block takes the
    /// ordinary path; an `altTHC` (10001) block takes the E10 path, but only
    /// for emissions that meet the E70/E85-ethanol-2001+ condition. Any other
    /// `pollutant_id` produces an all-`None` result — [`speciate_block`] makes
    /// the THC/`altTHC`-block test, so calling this directly is meaningful
    /// only for those two block kinds.
    ///
    /// Returns `None` only when the emission's fuel formulation is unknown
    /// (the Go `ff == nil` `continue`). A returned [`SpeciatedEmission`] may
    /// still have every field `None` — when no lookup row matches, or the
    /// run's needed set asks for none of the species.
    ///
    /// [`speciate_block`]: Self::speciate_block
    #[must_use]
    pub fn speciate_emission(
        &self,
        block_key: &FuelBlockKey,
        emission: &Emission,
        tables: &OnroadWorkerTables,
    ) -> Option<SpeciatedEmission> {
        // Go: ff := mwo.FuelFormulations[e.FuelFormulationID]; if ff == nil { continue }
        let formulation = *tables.fuel_formulation(emission.fuel_formulation_id)?;
        let total_oxygenate = formulation.total_oxygenate();
        let process_id = block_key.process_id;

        // The E70/E85-ethanol-2001+ condition: the THC path suppresses NMOG
        // and VOC for it, and the altTHC path runs only for it.
        let is_ethanol_alt_case = E70_E85_FUEL_SUBTYPE_IDS.contains(&emission.fuel_sub_type_id)
            && (process_id == RUNNING_EXHAUST_PROCESS_ID || process_id == START_EXHAUST_PROCESS_ID)
            && block_key.fuel_type_id == ETHANOL_FUEL_TYPE_ID
            && block_key.model_year_id >= ALT_THC_MIN_MODEL_YEAR;

        let mut result = SpeciatedEmission::default();

        // Ordinary THC path: methane and NMHC always; NMOG and VOC unless the
        // emission is on the E10 altTHC path instead.
        if block_key.pollutant_id == THC_POLLUTANT_ID {
            if let Some(ratio) = self.methane_ratio(&MethaneThcRatioKey {
                process_id,
                fuel_sub_type_id: emission.fuel_sub_type_id,
                reg_class_id: block_key.reg_class_id,
                model_year_id: block_key.model_year_id,
            }) {
                // methane (5) = THC * CH4THCRatio.
                let methane = emission.scaled(ratio);
                result.methane = tables
                    .is_needed(METHANE_POLLUTANT_ID, process_id)
                    .then_some(methane);

                // NMHC (79) = THC * (1 - CH4THCRatio). Computed unconditionally
                // as the NMOG/VOC operand; the NMHC output is still gated (see
                // the module-level fidelity note).
                let nmhc = emission.scaled(1.0 - ratio);
                result.nmhc = tables
                    .is_needed(NMHC_POLLUTANT_ID, process_id)
                    .then_some(nmhc);

                if !is_ethanol_alt_case {
                    if tables.is_needed(NMOG_POLLUTANT_ID, process_id) {
                        result.nmog = Some(self.speciate_nmog_or_voc(
                            NMOG_POLLUTANT_ID,
                            process_id,
                            emission.fuel_sub_type_id,
                            block_key.reg_class_id,
                            block_key.model_year_id,
                            &formulation,
                            total_oxygenate,
                            &nmhc,
                            emission,
                        ));
                    }
                    if tables.is_needed(VOC_POLLUTANT_ID, process_id) {
                        result.voc = Some(self.speciate_nmog_or_voc(
                            VOC_POLLUTANT_ID,
                            process_id,
                            emission.fuel_sub_type_id,
                            block_key.reg_class_id,
                            block_key.model_year_id,
                            &formulation,
                            total_oxygenate,
                            &nmhc,
                            emission,
                        ));
                    }
                }
            }
        }

        // E10 altTHC path: altNMHC, NMOG and VOC computed with E10's ratios
        // for ethanol-fueled E70/E85 2001+ running/start emissions.
        if block_key.pollutant_id == ALT_THC_POLLUTANT_ID && is_ethanol_alt_case {
            if let Some(ethanol_ratio) = self.methane_ratio(&MethaneThcRatioKey {
                process_id,
                fuel_sub_type_id: E10_FUEL_SUBTYPE_ID,
                reg_class_id: block_key.reg_class_id,
                model_year_id: block_key.model_year_id,
            }) {
                // altNMHC (10079) = altTHC * (1 - CH4THCRatio[E10]). Computed
                // unconditionally as the NMOG/VOC operand; the altNMHC output
                // is gated on the NMHC (79) needed-flag, as in the Go.
                let alt_nmhc = emission.scaled(1.0 - ethanol_ratio);
                result.alt_nmhc = tables
                    .is_needed(NMHC_POLLUTANT_ID, process_id)
                    .then_some(alt_nmhc);

                // The HCSpeciation lookup keys on the E10 subtype (12); the
                // oxygenate term still uses the actual emission's formulation.
                if tables.is_needed(NMOG_POLLUTANT_ID, process_id) {
                    result.nmog = Some(self.speciate_nmog_or_voc(
                        NMOG_POLLUTANT_ID,
                        process_id,
                        E10_FUEL_SUBTYPE_ID,
                        block_key.reg_class_id,
                        block_key.model_year_id,
                        &formulation,
                        total_oxygenate,
                        &alt_nmhc,
                        emission,
                    ));
                }
                if tables.is_needed(VOC_POLLUTANT_ID, process_id) {
                    result.voc = Some(self.speciate_nmog_or_voc(
                        VOC_POLLUTANT_ID,
                        process_id,
                        E10_FUEL_SUBTYPE_ID,
                        block_key.reg_class_id,
                        block_key.model_year_id,
                        &formulation,
                        total_oxygenate,
                        &alt_nmhc,
                        emission,
                    ));
                }
            }
        }

        // TOG (86) = NMOG (80) + methane (5), summing the *gated* species: an
        // un-needed summand contributes nothing, and TOG is omitted when both
        // summands are absent. On the altTHC path methane is absent, so TOG
        // there is NMOG alone.
        if tables.is_needed(TOG_POLLUTANT_ID, process_id) {
            result.tog = emission_sum(result.nmog.as_ref(), result.methane.as_ref());
        }

        Some(result)
    }

    /// Speciate a whole fuel block into one output block per produced
    /// pollutant — the Go `calculate`'s per-`FuelBlock` body.
    ///
    /// A block whose pollutant is neither THC (1) nor `altTHC` (10001) yields
    /// no output (the Go `if PollutantID != 1 && PollutantID != 10001 {
    /// continue }`). Otherwise each emission is speciated and the resulting
    /// species emissions are grouped into [`SpeciatedFuelBlock`]s by
    /// pollutant — the emissions within a block keep input-emission order, and
    /// the blocks are returned in ascending pollutant-id order (see the
    /// module-level fidelity note on output order).
    #[must_use]
    pub fn speciate_block(
        &self,
        block: &FuelBlock,
        tables: &OnroadWorkerTables,
    ) -> Vec<SpeciatedFuelBlock> {
        // Go: only THC and altTHC blocks are speciated.
        if block.key.pollutant_id != THC_POLLUTANT_ID
            && block.key.pollutant_id != ALT_THC_POLLUTANT_ID
        {
            return Vec::new();
        }

        // Group the speciated emissions by output pollutant. A BTreeMap keeps
        // the output blocks in ascending pollutant-id order; each Vec keeps
        // input-emission order.
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

/// The 40 `(pollutant, process)` pairs `HCSpeciationCalculator` registers —
/// its five output species across the nine onroad processes that emit THC.
///
/// Methane (5) is registered only for the four exhaust processes (running,
/// start, extended-idle, auxiliary-power); the five evaporative and refueling
/// processes register only NMHC (79), NMOG (80), TOG (86) and VOC (87) — so
/// `4` methane pairs `+ 9 * 4` other-species pairs `= 40`. This matches the 40
/// `Registration` lines for `HCSpeciationCalculator` in `CalculatorInfo.txt`
/// (per-process species: 1/2/90/91 → 5 species each, 11/12/13/18/19 → 4 each).
static REGISTRATIONS: [PollutantProcessAssociation; 40] = [
    // Running Exhaust (1)
    reg(5, 1),
    reg(79, 1),
    reg(80, 1),
    reg(86, 1),
    reg(87, 1),
    // Start Exhaust (2)
    reg(5, 2),
    reg(79, 2),
    reg(80, 2),
    reg(86, 2),
    reg(87, 2),
    // Evap Permeation (11)
    reg(79, 11),
    reg(80, 11),
    reg(86, 11),
    reg(87, 11),
    // Evap Fuel Vapor Venting (12)
    reg(79, 12),
    reg(80, 12),
    reg(86, 12),
    reg(87, 12),
    // Evap Fuel Leaks (13)
    reg(79, 13),
    reg(80, 13),
    reg(86, 13),
    reg(87, 13),
    // Refueling Displacement Vapor Loss (18)
    reg(79, 18),
    reg(80, 18),
    reg(86, 18),
    reg(87, 18),
    // Refueling Spillage Loss (19)
    reg(79, 19),
    reg(80, 19),
    reg(86, 19),
    reg(87, 19),
    // Extended Idle Exhaust (90)
    reg(5, 90),
    reg(79, 90),
    reg(80, 90),
    reg(86, 90),
    reg(87, 90),
    // Auxiliary Power Exhaust (91)
    reg(5, 91),
    reg(79, 91),
    reg(80, 91),
    reg(86, 91),
    reg(87, 91),
];

/// `HCSpeciationCalculator` declares no master-loop subscription of its own;
/// see the [`Calculator::subscriptions`] impl.
static NO_SUBSCRIPTIONS: &[CalculatorSubscription] = &[];

/// Upstream modules — `HCSpeciationCalculator` chains to every calculator that
/// produces the THC it speciates. From the `Chain` directives in
/// `CalculatorInfo.txt` (`Chain  HCSpeciationCalculator  <inModule>`):
/// `BaseRateCalculator` (exhaust THC), `EvaporativePermeationCalculator`,
/// `TankVaporVentingCalculator`, `LiquidLeakingCalculator` and
/// `RefuelingLossCalculator` (the evaporative and refueling THC).
static UPSTREAM: &[&str] = &[
    "BaseRateCalculator",
    "EvaporativePermeationCalculator",
    "TankVaporVentingCalculator",
    "LiquidLeakingCalculator",
    "RefuelingLossCalculator",
];

/// Default-DB tables the calculator's SQL extracts: `HCSpeciation` (the
/// NMOG/VOC speciation pairs) and `methaneTHCRatio` (the methane-to-THC
/// ratios). The speciation pass also consults the shared onroad worker table
/// `FuelFormulation`, which other calculators load.
static INPUT_TABLES: &[&str] = &["HCSpeciation", "methaneTHCRatio"];

/// `HCSpeciationCalculator` as a chain-DAG [`Calculator`].
///
/// The numerically faithful work lives on [`HcSpeciation`]; this zero-sized
/// type carries the calculator's chain metadata —
/// [`name`](Calculator::name), [`registrations`](Calculator::registrations),
/// [`upstream`](Calculator::upstream) — so the registry can wire it into the
/// calculator chain.
#[derive(Debug, Clone, Copy, Default)]
pub struct HcSpeciationCalculator;

impl HcSpeciationCalculator {
    /// Chain-DAG name — matches the Java class / Go package and the
    /// `CalculatorInfo.txt` module name.
    pub const NAME: &'static str = "HCSpeciationCalculator";

    /// Construct the calculator.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl Calculator for HcSpeciationCalculator {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    /// `HCSpeciationCalculator` carries no master-loop subscription of its
    /// own: `CalculatorInfo.txt` has no `Subscribe` directive for it. It is a
    /// chained calculator — its Java `subscribeToMe` calls `chainCalculator`
    /// rather than `targetLoop.subscribe` — so it runs when an
    /// [`upstream`](Calculator::upstream) calculator it chains to runs,
    /// speciating that calculator's THC output.
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

    /// Run the calculator for the current master-loop iteration.
    ///
    /// **Data plane pending (Task 50).** [`CalculatorContext`] exposes only
    /// placeholder `ExecutionTables` / `ScratchNamespace` today, so this body
    /// cannot read the `methaneTHCRatio` / `HCSpeciation` tables nor the
    /// upstream THC / `altTHC` fuel blocks, nor write the speciated blocks
    /// back. The faithful algorithm is ported and tested on [`HcSpeciation`];
    /// once the `DataFrameStore` lands, `execute` builds an [`HcSpeciation`]
    /// from `ctx.tables()`, reads the [`FuelBlock`]s, applies
    /// [`speciate_block`](HcSpeciation::speciate_block), and stores the
    /// resulting [`SpeciatedFuelBlock`]s.
    fn execute(&self, _ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
        Ok(CalculatorOutput::empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A `methaneTHCRatio` row helper.
    fn methane_row(
        process_id: i32,
        fuel_sub_type_id: i32,
        reg_class_id: i32,
        begin_model_year_id: i32,
        end_model_year_id: i32,
        ch4_thc_ratio: f64,
    ) -> MethaneThcRatioRow {
        MethaneThcRatioRow {
            process_id,
            fuel_sub_type_id,
            reg_class_id,
            begin_model_year_id,
            end_model_year_id,
            ch4_thc_ratio,
        }
    }

    /// An `HCSpeciation` row helper.
    fn hc_row(
        pol_process_id: i32,
        fuel_sub_type_id: i32,
        reg_class_id: i32,
        begin_model_year_id: i32,
        end_model_year_id: i32,
        speciation_constant: f64,
        oxy_speciation: f64,
    ) -> HcSpeciationRow {
        HcSpeciationRow {
            pol_process_id,
            fuel_sub_type_id,
            reg_class_id,
            begin_model_year_id,
            end_model_year_id,
            speciation_constant,
            oxy_speciation,
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

    /// A [`FuelFormulation`] with `totalOxygenate` 4.0 (MTBE 2 + ETOH 2) and
    /// `volToWtPercentOxy` 0.5 — the values used throughout the tests. Both
    /// chosen so every product is exactly representable in `f64`.
    fn formulation() -> FuelFormulation {
        FuelFormulation {
            mtbe_volume: 2.0,
            etbe_volume: 0.0,
            tame_volume: 0.0,
            etoh_volume: 2.0,
            vol_to_wt_percent_oxy: 0.5,
        }
    }

    /// The standard THC-path fixture: process 1, fuel subtype 20, formulation
    /// 100, reg class 0, model year 2005. The methane ratio is `0.25`; the
    /// NMOG speciation is `(0.25, 0.25)` — `factor = 0.25 + 0.25 * 0.5 * 4.0 =
    /// 0.75` — and the VOC speciation `(0.5, 0.0)` — `factor = 0.5`. Every
    /// value is exactly representable in `f64`, so the tests use exact
    /// equality.
    fn fixture() -> (HcSpeciation, OnroadWorkerTables, FuelBlockKey) {
        let speciation = HcSpeciation::build(
            [methane_row(1, 20, 0, 2000, 2010, 0.25)],
            [
                hc_row(NMOG_POLLUTANT_ID * 100 + 1, 20, 0, 2000, 2010, 0.25, 0.25),
                hc_row(VOC_POLLUTANT_ID * 100 + 1, 20, 0, 2000, 2010, 0.5, 0.0),
            ],
        );
        let needed = [5, 79, 80, 86, 87].map(|p| p * 100 + 1);
        let tables = OnroadWorkerTables::new([(100, formulation())], needed);
        let key = FuelBlockKey {
            pollutant_id: THC_POLLUTANT_ID,
            process_id: 1,
            fuel_type_id: 1,
            reg_class_id: 0,
            model_year_id: 2005,
        };
        (speciation, tables, key)
    }

    /// Worker tables for process 1 with exactly `pollutants` needed.
    fn tables_needing(pollutants: &[i32]) -> OnroadWorkerTables {
        OnroadWorkerTables::new(
            [(100, formulation())],
            pollutants.iter().map(|p| p * 100 + 1).collect::<Vec<_>>(),
        )
    }

    #[test]
    fn build_populates_both_lookup_tables() {
        let (speciation, ..) = fixture();
        // Two rows, each spanning model years 2000..=2010 -> 11 entries each.
        assert_eq!(speciation.methane_thc_ratio.len(), 11);
        assert_eq!(speciation.hc_speciation.len(), 22);
    }

    #[test]
    fn build_expands_each_model_year_range() {
        // One row covering model years 2000..=2002 -> three lookup entries.
        let speciation = HcSpeciation::build([methane_row(1, 20, 0, 2000, 2002, 0.4)], []);
        assert_eq!(speciation.methane_thc_ratio.len(), 3);
        for year in [2000, 2001, 2002] {
            assert_eq!(
                speciation.methane_ratio(&MethaneThcRatioKey {
                    process_id: 1,
                    fuel_sub_type_id: 20,
                    reg_class_id: 0,
                    model_year_id: year,
                }),
                Some(0.4),
            );
        }
        // A year outside the range misses.
        assert_eq!(
            speciation.methane_ratio(&MethaneThcRatioKey {
                process_id: 1,
                fuel_sub_type_id: 20,
                reg_class_id: 0,
                model_year_id: 2003,
            }),
            None,
        );
    }

    #[test]
    fn build_empty_when_begin_year_exceeds_end_year() {
        // begin > end -> the range is empty, the row contributes nothing.
        let speciation = HcSpeciation::build([methane_row(1, 20, 0, 2010, 2000, 0.4)], []);
        assert!(speciation.methane_thc_ratio.is_empty());
    }

    #[test]
    fn build_last_row_wins_on_duplicate_key() {
        // Two rows whose expanded keys collide; the map keeps the last.
        let speciation = HcSpeciation::build(
            [
                methane_row(1, 20, 0, 2000, 2005, 0.1),
                methane_row(1, 20, 0, 2003, 2008, 0.9),
            ],
            [],
        );
        // Year 2004 is in both ranges -> the second row wins.
        assert_eq!(
            speciation.methane_ratio(&MethaneThcRatioKey {
                process_id: 1,
                fuel_sub_type_id: 20,
                reg_class_id: 0,
                model_year_id: 2004,
            }),
            Some(0.9),
        );
        // Year 2001 is only in the first range.
        assert_eq!(
            speciation.methane_ratio(&MethaneThcRatioKey {
                process_id: 1,
                fuel_sub_type_id: 20,
                reg_class_id: 0,
                model_year_id: 2001,
            }),
            Some(0.1),
        );
    }

    #[test]
    fn total_oxygenate_sums_the_four_volumes() {
        let ff = FuelFormulation {
            mtbe_volume: 1.0,
            etbe_volume: 2.0,
            tame_volume: 4.0,
            etoh_volume: 8.0,
            vol_to_wt_percent_oxy: 0.5,
        };
        assert_eq!(ff.total_oxygenate(), 15.0);
    }

    #[test]
    fn emission_scaled_multiplies_both_quant_and_rate() {
        let e = emission(8.0, 4.0, 20, 100);
        assert_eq!(e.scaled(0.25), emission(2.0, 1.0, 20, 100));
        // Fuel ids carry through; scaling by zero yields a zero emission still
        // tagged with those ids.
        assert_eq!(e.scaled(0.0), emission(0.0, 0.0, 20, 100));
    }

    #[test]
    fn emission_sum_handles_present_and_absent_summands() {
        let a = emission(3.0, 1.0, 20, 100);
        let b = emission(4.0, 2.0, 21, 101);
        // Both present: quantities and rates add, fuel ids come from `a`.
        assert_eq!(
            emission_sum(Some(&a), Some(&b)),
            Some(emission(7.0, 3.0, 20, 100)),
        );
        // One present: the other copied through.
        assert_eq!(emission_sum(Some(&a), None), Some(a));
        assert_eq!(emission_sum(None, Some(&b)), Some(b));
        // Neither present: no emission.
        assert_eq!(emission_sum(None, None), None);
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
    fn speciate_emission_nmog_factor_includes_the_oxygenate_term() {
        let (speciation, tables, key) = fixture();
        let speciated = speciation
            .speciate_emission(&key, &emission(8.0, 4.0, 20, 100), &tables)
            .expect("speciation produced");
        // NMHC = (6.0, 3.0). NMOG factor = speciationConstant 0.25 +
        // oxySpeciation 0.25 * volToWtPercentOxy 0.5 * totalOxygenate 4.0
        // = 0.25 + 0.5 = 0.75. NMOG = NMHC * 0.75.
        assert_eq!(speciated.nmog, Some(emission(4.5, 2.25, 20, 100)));
    }

    #[test]
    fn speciate_emission_voc_is_nmhc_times_factor() {
        let (speciation, tables, key) = fixture();
        let speciated = speciation
            .speciate_emission(&key, &emission(8.0, 4.0, 20, 100), &tables)
            .expect("speciation produced");
        // VOC factor = 0.5 + 0.0 = 0.5. VOC = NMHC (6.0, 3.0) * 0.5.
        assert_eq!(speciated.voc, Some(emission(3.0, 1.5, 20, 100)));
    }

    #[test]
    fn speciate_emission_nmog_and_voc_are_zero_without_a_speciation_row() {
        // Lookup tables with the methane ratio but no HCSpeciation rows.
        let speciation = HcSpeciation::build([methane_row(1, 20, 0, 2000, 2010, 0.25)], []);
        let (_, tables, key) = fixture();
        let speciated = speciation
            .speciate_emission(&key, &emission(8.0, 4.0, 20, 100), &tables)
            .expect("speciation produced");
        // No row -> NMOG/VOC fall back to the original THC emission scaled by
        // zero, still tagged with the THC fuel ids.
        assert_eq!(speciated.nmog, Some(emission(0.0, 0.0, 20, 100)));
        assert_eq!(speciated.voc, Some(emission(0.0, 0.0, 20, 100)));
        // NMHC is unaffected — it needs no speciation row.
        assert_eq!(speciated.nmhc, Some(emission(6.0, 3.0, 20, 100)));
    }

    #[test]
    fn speciate_emission_tog_is_nmog_plus_methane() {
        let (speciation, tables, key) = fixture();
        let speciated = speciation
            .speciate_emission(&key, &emission(8.0, 4.0, 20, 100), &tables)
            .expect("speciation produced");
        // TOG = NMOG (4.5, 2.25) + methane (2.0, 1.0).
        assert_eq!(speciated.tog, Some(emission(6.5, 3.25, 20, 100)));
    }

    #[test]
    fn speciate_emission_tog_is_nmog_alone_when_methane_not_needed() {
        let (speciation, _, key) = fixture();
        // Methane (5) absent from the needed set; NMOG and TOG present.
        let tables = tables_needing(&[79, 80, 86]);
        let speciated = speciation
            .speciate_emission(&key, &emission(8.0, 4.0, 20, 100), &tables)
            .expect("speciation produced");
        assert_eq!(speciated.methane, None);
        // TOG = NMOG + (absent methane) = NMOG.
        assert_eq!(speciated.tog, speciated.nmog);
        assert_eq!(speciated.tog, Some(emission(4.5, 2.25, 20, 100)));
    }

    #[test]
    fn speciate_emission_tog_is_methane_alone_when_nmog_not_needed() {
        let (speciation, _, key) = fixture();
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
        let (speciation, _, key) = fixture();
        // TOG (86) needed, but neither methane nor NMOG is.
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
        let (speciation, _, key) = fixture();
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
        // Documented fidelity deviation: the needed set has NMOG but not NMHC.
        // The Go reuses the gated `emissions[79]` entry as the NMOG operand
        // and nil-panics here; this port computes the NMHC operand
        // unconditionally and produces the correct NMOG.
        let (speciation, _, key) = fixture();
        let tables = tables_needing(&[80]);
        let speciated = speciation
            .speciate_emission(&key, &emission(8.0, 4.0, 20, 100), &tables)
            .expect("speciation produced");
        assert_eq!(speciated.nmhc, None);
        // NMOG = NMHC(6.0, 3.0) * 0.75 — the operand was still computed.
        assert_eq!(speciated.nmog, Some(emission(4.5, 2.25, 20, 100)));
    }

    #[test]
    fn speciate_emission_none_when_methane_ratio_missing() {
        // Lookup tables with no methaneTHCRatio row for model year 2005.
        let speciation = HcSpeciation::build(
            [methane_row(1, 20, 0, 1990, 1999, 0.25)],
            [hc_row(
                NMOG_POLLUTANT_ID * 100 + 1,
                20,
                0,
                2000,
                2010,
                0.25,
                0.25,
            )],
        );
        let (_, tables, key) = fixture();
        // No ratio -> the THC path produces nothing; TOG sums two absent
        // summands -> all-None, but the call still succeeds (the Go does not
        // `continue` on a missing ratio).
        let speciated = speciation
            .speciate_emission(&key, &emission(8.0, 4.0, 20, 100), &tables)
            .expect("speciation succeeds with an empty result");
        assert_eq!(speciated, SpeciatedEmission::default());
    }

    #[test]
    fn speciate_emission_none_when_fuel_formulation_unknown() {
        let (speciation, _, key) = fixture();
        // Worker tables that know no fuel formulations at all.
        let tables = OnroadWorkerTables::new([], [5 * 100 + 1]);
        assert!(speciation
            .speciate_emission(&key, &emission(8.0, 4.0, 20, 100), &tables)
            .is_none());
    }

    #[test]
    fn speciate_emission_reg_class_and_model_year_key_the_lookups() {
        let (speciation, tables, _) = fixture();
        // The fixture rows are keyed reg class 0, model years 2000..=2010.
        // A block with reg class 7 misses the methane ratio entirely.
        let wrong_reg_class = FuelBlockKey {
            pollutant_id: THC_POLLUTANT_ID,
            process_id: 1,
            fuel_type_id: 1,
            reg_class_id: 7,
            model_year_id: 2005,
        };
        let speciated = speciation
            .speciate_emission(&wrong_reg_class, &emission(8.0, 4.0, 20, 100), &tables)
            .expect("call succeeds");
        assert_eq!(speciated, SpeciatedEmission::default());
        // A block of model year 2020 likewise misses the range.
        let wrong_year = FuelBlockKey {
            model_year_id: 2020,
            ..wrong_reg_class
        };
        let speciated = speciation
            .speciate_emission(&wrong_year, &emission(8.0, 4.0, 20, 100), &tables)
            .expect("call succeeds");
        assert_eq!(speciated, SpeciatedEmission::default());
    }

    /// The E10 `altTHC` fixture: an `altTHC` (10001) block, process 1, fuel
    /// type 5 (ethanol), model year 2005, with an emission on the E85 fuel
    /// subtype 51. The E10 (subtype 12) methane ratio is `0.5`, the E10 NMOG
    /// speciation `(0.5, 0.0)` and VOC `(0.25, 0.0)`. Fuel formulation 200 is
    /// pure-ethanol-heavy: `totalOxygenate` 8.0.
    fn alt_fixture() -> (HcSpeciation, OnroadWorkerTables, FuelBlockKey) {
        let speciation = HcSpeciation::build(
            [methane_row(1, E10_FUEL_SUBTYPE_ID, 0, 2000, 2010, 0.5)],
            [
                hc_row(
                    NMOG_POLLUTANT_ID * 100 + 1,
                    E10_FUEL_SUBTYPE_ID,
                    0,
                    2000,
                    2010,
                    0.5,
                    0.0,
                ),
                hc_row(
                    VOC_POLLUTANT_ID * 100 + 1,
                    E10_FUEL_SUBTYPE_ID,
                    0,
                    2000,
                    2010,
                    0.25,
                    0.0,
                ),
            ],
        );
        let e85_formulation = FuelFormulation {
            mtbe_volume: 0.0,
            etbe_volume: 0.0,
            tame_volume: 0.0,
            etoh_volume: 8.0,
            vol_to_wt_percent_oxy: 0.5,
        };
        let needed = [5, 79, 80, 86, 87].map(|p| p * 100 + 1);
        let tables = OnroadWorkerTables::new([(200, e85_formulation)], needed);
        let key = FuelBlockKey {
            pollutant_id: ALT_THC_POLLUTANT_ID,
            process_id: 1,
            fuel_type_id: ETHANOL_FUEL_TYPE_ID,
            reg_class_id: 0,
            model_year_id: 2005,
        };
        (speciation, tables, key)
    }

    #[test]
    fn speciate_emission_alt_path_alt_nmhc_uses_the_e10_ratio() {
        let (speciation, tables, key) = alt_fixture();
        let speciated = speciation
            .speciate_emission(&key, &emission(8.0, 4.0, 51, 200), &tables)
            .expect("speciation produced");
        // altNMHC = altTHC * (1 - CH4THCRatio[E10] 0.5) = altTHC * 0.5.
        assert_eq!(speciated.alt_nmhc, Some(emission(4.0, 2.0, 51, 200)));
        // No methane or NMHC on the altTHC path.
        assert_eq!(speciated.methane, None);
        assert_eq!(speciated.nmhc, None);
    }

    #[test]
    fn speciate_emission_alt_path_nmog_and_voc_speciate_from_alt_nmhc() {
        let (speciation, tables, key) = alt_fixture();
        let speciated = speciation
            .speciate_emission(&key, &emission(8.0, 4.0, 51, 200), &tables)
            .expect("speciation produced");
        // altNMHC = (4.0, 2.0). NMOG factor = 0.5; VOC factor = 0.25.
        assert_eq!(speciated.nmog, Some(emission(2.0, 1.0, 51, 200)));
        assert_eq!(speciated.voc, Some(emission(1.0, 0.5, 51, 200)));
        // TOG = NMOG + (absent methane) = NMOG.
        assert_eq!(speciated.tog, Some(emission(2.0, 1.0, 51, 200)));
    }

    #[test]
    fn speciate_emission_alt_path_alt_nmhc_gated_by_the_nmhc_needed_flag() {
        let (speciation, _, key) = alt_fixture();
        // The Go gates `emissions[10079]` on `79 * 100 + process`; with NMHC
        // (79) absent from the needed set the altNMHC output is suppressed,
        // even though NMOG (which uses it as an operand) is still produced.
        let tables = OnroadWorkerTables::new(
            [(
                200,
                FuelFormulation {
                    mtbe_volume: 0.0,
                    etbe_volume: 0.0,
                    tame_volume: 0.0,
                    etoh_volume: 8.0,
                    vol_to_wt_percent_oxy: 0.5,
                },
            )],
            [80 * 100 + 1],
        );
        let speciated = speciation
            .speciate_emission(&key, &emission(8.0, 4.0, 51, 200), &tables)
            .expect("speciation produced");
        assert_eq!(speciated.alt_nmhc, None);
        // NMOG still computed from the unconditionally-evaluated operand.
        assert_eq!(speciated.nmog, Some(emission(2.0, 1.0, 51, 200)));
    }

    #[test]
    fn speciate_emission_thc_path_suppresses_nmog_voc_for_the_ethanol_alt_case() {
        // A THC block emission that meets the E70/E85-ethanol-2001+ condition:
        // methane and NMHC are still produced, but NMOG and VOC are not — they
        // come from the altTHC block instead.
        let speciation = HcSpeciation::build(
            [methane_row(1, 51, 0, 2000, 2010, 0.25)],
            [hc_row(
                NMOG_POLLUTANT_ID * 100 + 1,
                51,
                0,
                2000,
                2010,
                0.5,
                0.0,
            )],
        );
        let tables = OnroadWorkerTables::new(
            [(100, formulation())],
            [5, 79, 80, 86, 87].map(|p| p * 100 + 1),
        );
        let key = FuelBlockKey {
            pollutant_id: THC_POLLUTANT_ID,
            process_id: 1,
            fuel_type_id: ETHANOL_FUEL_TYPE_ID,
            reg_class_id: 0,
            model_year_id: 2005,
        };
        let speciated = speciation
            .speciate_emission(&key, &emission(8.0, 4.0, 51, 100), &tables)
            .expect("speciation produced");
        assert_eq!(speciated.methane, Some(emission(2.0, 1.0, 51, 100)));
        assert_eq!(speciated.nmhc, Some(emission(6.0, 3.0, 51, 100)));
        assert_eq!(speciated.nmog, None);
        assert_eq!(speciated.voc, None);
        // TOG = (absent NMOG) + methane = methane.
        assert_eq!(speciated.tog, Some(emission(2.0, 1.0, 51, 100)));
    }

    #[test]
    fn speciate_emission_alt_path_inert_when_the_ethanol_condition_is_unmet() {
        let (speciation, tables, key) = alt_fixture();
        // A non-E70/E85 fuel subtype fails the condition: the altTHC block
        // produces nothing.
        let speciated = speciation
            .speciate_emission(&key, &emission(8.0, 4.0, 20, 200), &tables)
            .expect("call succeeds");
        assert_eq!(speciated, SpeciatedEmission::default());
        // A process other than running/start exhaust likewise fails it.
        let process_3 = FuelBlockKey {
            process_id: 3,
            ..key
        };
        let speciated = speciation
            .speciate_emission(&process_3, &emission(8.0, 4.0, 51, 200), &tables)
            .expect("call succeeds");
        assert_eq!(speciated, SpeciatedEmission::default());
        // A pre-2001 model year likewise fails it.
        let old_year = FuelBlockKey {
            model_year_id: 2000,
            ..key
        };
        let speciated = speciation
            .speciate_emission(&old_year, &emission(8.0, 4.0, 51, 200), &tables)
            .expect("call succeeds");
        assert_eq!(speciated, SpeciatedEmission::default());
    }

    #[test]
    fn speciate_block_groups_thc_emissions_by_pollutant() {
        let (speciation, tables, key) = fixture();
        let block = FuelBlock {
            key,
            emissions: vec![emission(8.0, 4.0, 20, 100)],
        };
        let blocks = speciation.speciate_block(&block, &tables);
        // One block per output species: 5, 79, 80, 86, 87.
        let pollutants: Vec<i32> = blocks.iter().map(|b| b.pollutant_id).collect();
        assert_eq!(pollutants, vec![5, 79, 80, 86, 87]);
        for b in &blocks {
            assert_eq!(b.emissions.len(), 1);
        }
    }

    #[test]
    fn speciate_block_groups_alt_thc_emissions_by_pollutant() {
        let (speciation, tables, key) = alt_fixture();
        let block = FuelBlock {
            key,
            emissions: vec![emission(8.0, 4.0, 51, 200)],
        };
        let blocks = speciation.speciate_block(&block, &tables);
        // The altTHC path produces NMOG (80), TOG (86), VOC (87) and altNMHC
        // (10079); ascending pollutant-id order puts 10079 last.
        let pollutants: Vec<i32> = blocks.iter().map(|b| b.pollutant_id).collect();
        assert_eq!(pollutants, vec![80, 86, 87, 10079]);
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
    fn speciate_block_skips_an_emission_with_an_unknown_formulation() {
        let (speciation, tables, key) = fixture();
        // First emission speciates (formulation 100 is known); the second
        // uses an unknown formulation and is skipped.
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
        let calc = HcSpeciationCalculator::new();
        assert_eq!(calc.name(), "HCSpeciationCalculator");
        // Chained calculator — no direct master-loop subscription.
        assert!(calc.subscriptions().is_empty());
        assert_eq!(
            calc.upstream(),
            &[
                "BaseRateCalculator",
                "EvaporativePermeationCalculator",
                "TankVaporVentingCalculator",
                "LiquidLeakingCalculator",
                "RefuelingLossCalculator",
            ],
        );
        assert!(calc.input_tables().contains(&"HCSpeciation"));
        assert!(calc.input_tables().contains(&"methaneTHCRatio"));
    }

    #[test]
    fn calculator_registers_40_pollutant_process_pairs() {
        let calc = HcSpeciationCalculator::new();
        let regs = calc.registrations();
        assert_eq!(regs.len(), 40);
        // Methane (5) only for the four exhaust processes.
        for &p in &[1_u16, 2, 90, 91] {
            assert!(regs.contains(&reg(5, p)), "missing methane for process {p}");
        }
        for &p in &[11_u16, 12, 13, 18, 19] {
            assert!(
                !regs.contains(&reg(5, p)),
                "methane should not be registered for process {p}",
            );
        }
        // The other four species across all nine THC-emitting processes.
        let processes = [1_u16, 2, 11, 12, 13, 18, 19, 90, 91];
        for &p in &processes {
            for &s in &[79_u16, 80, 86, 87] {
                assert!(
                    regs.contains(&reg(s, p)),
                    "missing registration for pollutant {s} process {p}",
                );
            }
        }
    }

    #[test]
    fn calculator_execute_returns_placeholder_until_data_plane() {
        // execute is a documented placeholder until Task 50; it must still
        // honour the trait contract and return Ok.
        let calc = HcSpeciationCalculator::new();
        let ctx = CalculatorContext::new();
        assert!(calc.execute(&ctx).is_ok());
    }

    #[test]
    fn calculator_is_object_safe() {
        // The registry stores calculators as Box<dyn Calculator>.
        let calc: Box<dyn Calculator> = Box::new(HcSpeciationCalculator::new());
        assert_eq!(calc.name(), "HCSpeciationCalculator");
        assert_eq!(calc.registrations().len(), 40);
    }
}
