//! Port of `calc/hcspeciation/hcspeciation.go` — the onroad
//! `HCSpeciationCalculator`, which speciates total hydrocarbons into the five
//! hydrocarbon species MOVES reports separately.
//!
//! The Nonroad counterpart,
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
//! methane = THC * r
//! NMHC = THC * (1 - r)
//! factor = speciationConstant + oxySpeciation * volToWtPercentOxy * totalOxygenate
//! NMOG = NMHC * factor(NMOG) ( = 0 when no HCSpeciation row matches)
//! VOC = NMHC * factor(VOC) ( = 0 when no HCSpeciation row matches)
//! TOG = NMOG + methane
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
//! fuelSubtypeID, regClassID, modelYearID)`. Its `CreateDefault.sql` rows
//! carry a `(beginModelYearID, endModelYearID)` model-year *range*;
//! [`MethaneThcRatioRow`] is one such row and [`HcSpeciation::build`]
//! expands the range to one map entry per model year, exactly as the Go
//! `StartSetup` does.
//! * `HCSpeciation` — the NMOG/VOC `(speciationConstant, oxySpeciation)`
//! pairs, keyed by `(polProcessID, fuelSubtypeID, regClassID,
//! modelYearID)`, again from a model-year-range row ([`HcSpeciationRow`]).
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
//! oxygenate sum as `MTBEVolume + ETBEVolume + TAMEVolume + 10` (a hardcoded
//! 10% ethanol). The Go worker uses the formulation's actual `ETOHVolume`,
//! the same `totalOxygenate` as the ordinary path. This port follows the Go.
//! * **NMOG/VOC operand.** The Go computes `emissions[79]` (NMHC) only when
//! NMHC output is requested, yet reuses that gated map entry as the operand
//! for the NMOG and VOC formulas. A run that requests NMOG or VOC without
//! NMHC therefore dereferences a nil pointer in the Go. This port computes
//! the NMHC operand value unconditionally (likewise the `altNMHC` operand);
//! the NMHC / `altNMHC` *outputs* are still gated. For every needed-set
//! closed under the NMOG/VOC → NMHC dependency — i.e. every real MOVES run,
//! because the pollutant chain pulls NMHC in as an intermediate whenever
//! NMOG or VOC is requested — the result is numerically identical, and the
//! degenerate set yields the correct number instead of a crash.
//! * **Output order.** The Go grouped output emissions into new fuel blocks
//! keyed in a Go `map`, whose iteration order is randomised.
//! [`HcSpeciation::speciate_block`] returns the blocks in ascending
//! pollutant-id order so the output is deterministic; a fuel-block set is
//! unordered, so this is a presentation choice only.
//!
//! # Data plane
//!
//! [`Calculator::execute`] receives a [`CalculatorContext`] whose
//! `ExecutionTables` / `ScratchNamespace` are Phase-2 placeholders until the
//! `DataFrameStore` lands (), so `execute` cannot yet
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
    Calculator, CalculatorContext, CalculatorOutput, CalculatorSubscription, DataFrameStoreTyped,
    Error, TableRow,
};
use polars::prelude::{DataFrame, DataType, NamedFrom, PolarsResult, Schema, Series};

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

/// The `(speciationConstant, oxySpeciation)` pair of one `HCSpeciation` row/// the Go `HCSpeciationDetail`.
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
/// volumes, kept as a [`FuelFormulation`] per `fuelFormulationID`;
/// * `NeededPolProcessIDs` — the set of `polProcessID`s the run requires,
/// which gates which output pollutants are computed.
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

 /// Whether `pollutantID * 100 + processID` is in the run's needed set /// the Go `mwo.NeededPolProcessIDs[ppid]`.
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

/// The 40 `(pollutant, process)` pairs `HCSpeciationCalculator` registers/// its five output species across the nine onroad processes that emit THC.
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

fn row_err(table: &'static str, row: usize, column: &'static str, msg: String) -> Error {
    Error::RowExtraction {
        table: table.into(),
        row,
        column: column.into(),
        message: msg,
    }
}

/// One `FuelFormulation` row — the oxygenate fields the speciation formula reads.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HcFuelFormulationRow {
 /// `fuelFormulationID`.
    pub fuel_formulation_id: i32,
 /// `MTBEVolume`.
    pub mtbe_volume: f64,
 /// `ETBEVolume`.
    pub etbe_volume: f64,
 /// `TAMEVolume`.
    pub tame_volume: f64,
 /// `ETOHVolume`.
    pub etoh_volume: f64,
 /// `volToWtPercentOxy`.
    pub vol_to_wt_percent_oxy: f64,
}

impl TableRow for HcFuelFormulationRow {
    fn table_name() -> &'static str {
        "FuelFormulation"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("fuelFormulationID".into(), DataType::Int32),
            ("MTBEVolume".into(), DataType::Float64),
            ("ETBEVolume".into(), DataType::Float64),
            ("TAMEVolume".into(), DataType::Float64),
            ("ETOHVolume".into(), DataType::Float64),
            ("volToWtPercentOxy".into(), DataType::Float64),
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
                    "MTBEVolume".into(),
                    rows.iter().map(|r| r.mtbe_volume).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "ETBEVolume".into(),
                    rows.iter().map(|r| r.etbe_volume).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "TAMEVolume".into(),
                    rows.iter().map(|r| r.tame_volume).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "ETOHVolume".into(),
                    rows.iter().map(|r| r.etoh_volume).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "volToWtPercentOxy".into(),
                    rows.iter()
                        .map(|r| r.vol_to_wt_percent_oxy)
                        .collect::<Vec<f64>>(),
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
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .f64()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let ff_id = get_i32("fuelFormulationID")?;
        let mtbe = get_f64("MTBEVolume")?;
        let etbe = get_f64("ETBEVolume")?;
        let tame = get_f64("TAMEVolume")?;
        let etoh = get_f64("ETOHVolume")?;
        let vol_to_wt = get_f64("volToWtPercentOxy")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(HcFuelFormulationRow {
                    fuel_formulation_id: ff_id.get(i).ok_or_else(|| null("fuelFormulationID"))?,
                    mtbe_volume: mtbe.get(i).ok_or_else(|| null("MTBEVolume"))?,
                    etbe_volume: etbe.get(i).ok_or_else(|| null("ETBEVolume"))?,
                    tame_volume: tame.get(i).ok_or_else(|| null("TAMEVolume"))?,
                    etoh_volume: etoh.get(i).ok_or_else(|| null("ETOHVolume"))?,
                    vol_to_wt_percent_oxy: vol_to_wt
                        .get(i)
                        .ok_or_else(|| null("volToWtPercentOxy"))?,
                })
            })
            .collect()
    }
}

/// One `MOVESWorkerOutput` input or output row for the HC speciation calculator.
///
/// Carries the dimensional context needed to reconstruct the output alongside
/// the fuel-block key and emission values the algorithm reads.
#[derive(Debug, Clone, PartialEq)]
pub struct ThcWorkerRow {
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
    pub reg_class_id: i32,
    pub fuel_type_id: i32,
    pub model_year_id: i32,
    pub road_type_id: i32,
    pub fuel_sub_type_id: i32,
    pub fuel_formulation_id: i32,
    pub emission_quant: f64,
    pub emission_rate: f64,
}

impl TableRow for ThcWorkerRow {
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
        // `fuelSubTypeID` / `fuelFormulationID` are not reliably present on the
        // accumulated `MOVESWorkerOutput` (the standard worker schema carries
        // `fuelSubTypeID` only, and never `fuelFormulationID`). The HCFuelSupply
        // expansion in `execute` joins each THC row to the fuel supply and
        // overwrites both, so read them when present and default to 0 otherwise.
        let fuel_sub_type = df.column("fuelSubTypeID").ok().and_then(|c| c.i32().ok().cloned());
        let fuel_formulation = df
            .column("fuelFormulationID")
            .ok()
            .and_then(|c| c.i32().ok().cloned());
        let emission_quant = get_f64("emissionQuant")?;
        let emission_rate = get_f64("emissionRate")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(ThcWorkerRow {
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
                    fuel_sub_type_id: fuel_sub_type.as_ref().and_then(|c| c.get(i)).unwrap_or(0),
                    fuel_formulation_id: fuel_formulation
                        .as_ref()
                        .and_then(|c| c.get(i))
                        .unwrap_or(0),
                    emission_quant: emission_quant.get(i).ok_or_else(|| null("emissionQuant"))?,
                    emission_rate: emission_rate.get(i).ok_or_else(|| null("emissionRate"))?,
                })
            })
            .collect()
    }
}

impl TableRow for MethaneThcRatioRow {
    fn table_name() -> &'static str {
        "methaneTHCRatio"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("processID".into(), DataType::Int32),
            ("fuelSubtypeID".into(), DataType::Int32),
            ("regClassID".into(), DataType::Int32),
            ("beginModelYearID".into(), DataType::Int32),
            ("endModelYearID".into(), DataType::Int32),
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
                    "fuelSubtypeID".into(),
                    rows.iter()
                        .map(|r| r.fuel_sub_type_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "regClassID".into(),
                    rows.iter().map(|r| r.reg_class_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "beginModelYearID".into(),
                    rows.iter()
                        .map(|r| r.begin_model_year_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "endModelYearID".into(),
                    rows.iter()
                        .map(|r| r.end_model_year_id)
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
        let t = "methaneTHCRatio";
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
        let fuel_sub = get_i32("fuelSubtypeID")?;
        let reg_class = get_i32("regClassID")?;
        let begin_my = get_i32("beginModelYearID")?;
        let end_my = get_i32("endModelYearID")?;
        let ratio = get_f64("CH4THCRatio")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(MethaneThcRatioRow {
                    process_id: process.get(i).ok_or_else(|| null("processID"))?,
                    fuel_sub_type_id: fuel_sub.get(i).ok_or_else(|| null("fuelSubtypeID"))?,
                    reg_class_id: reg_class.get(i).ok_or_else(|| null("regClassID"))?,
                    begin_model_year_id: begin_my.get(i).ok_or_else(|| null("beginModelYearID"))?,
                    end_model_year_id: end_my.get(i).ok_or_else(|| null("endModelYearID"))?,
                    ch4_thc_ratio: ratio.get(i).ok_or_else(|| null("CH4THCRatio"))?,
                })
            })
            .collect()
    }
}

impl TableRow for HcSpeciationRow {
    fn table_name() -> &'static str {
        "HCSpeciation"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("polProcessID".into(), DataType::Int32),
            ("fuelSubtypeID".into(), DataType::Int32),
            ("regClassID".into(), DataType::Int32),
            ("beginModelYearID".into(), DataType::Int32),
            ("endModelYearID".into(), DataType::Int32),
            ("speciationConstant".into(), DataType::Float64),
            ("oxySpeciation".into(), DataType::Float64),
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
                    "fuelSubtypeID".into(),
                    rows.iter()
                        .map(|r| r.fuel_sub_type_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "regClassID".into(),
                    rows.iter().map(|r| r.reg_class_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "beginModelYearID".into(),
                    rows.iter()
                        .map(|r| r.begin_model_year_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "endModelYearID".into(),
                    rows.iter()
                        .map(|r| r.end_model_year_id)
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
                Series::new(
                    "oxySpeciation".into(),
                    rows.iter().map(|r| r.oxy_speciation).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "HCSpeciation";
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
        let fuel_sub = get_i32("fuelSubtypeID")?;
        let reg_class = get_i32("regClassID")?;
        let begin_my = get_i32("beginModelYearID")?;
        let end_my = get_i32("endModelYearID")?;
        let spec_const = get_f64("speciationConstant")?;
        let oxy_spec = get_f64("oxySpeciation")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(HcSpeciationRow {
                    pol_process_id: pol_proc.get(i).ok_or_else(|| null("polProcessID"))?,
                    fuel_sub_type_id: fuel_sub.get(i).ok_or_else(|| null("fuelSubtypeID"))?,
                    reg_class_id: reg_class.get(i).ok_or_else(|| null("regClassID"))?,
                    begin_model_year_id: begin_my.get(i).ok_or_else(|| null("beginModelYearID"))?,
                    end_model_year_id: end_my.get(i).ok_or_else(|| null("endModelYearID"))?,
                    speciation_constant: spec_const
                        .get(i)
                        .ok_or_else(|| null("speciationConstant"))?,
                    oxy_speciation: oxy_spec.get(i).ok_or_else(|| null("oxySpeciation"))?,
                })
            })
            .collect()
    }
}

/// `HCSpeciationCalculator` declares no master-loop subscription of its own;
/// see the [`Calculator::subscriptions`] impl.
static NO_SUBSCRIPTIONS: &[CalculatorSubscription] = &[];

/// Upstream modules — `HCSpeciationCalculator` chains to every calculator that
/// produces the THC it speciates. From the `Chain` directives in
/// `CalculatorInfo.txt` (`Chain HCSpeciationCalculator <inModule>`):
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
/// type carries the calculator's chain metadata/// [`name`](Calculator::name), [`registrations`](Calculator::registrations),
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

    fn execute(&self, ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
        let tables = ctx.tables();

        let speciation = HcSpeciation::build(
            tables.iter_typed::<MethaneThcRatioRow>("methaneTHCRatio")?,
            tables.iter_typed::<HcSpeciationRow>("HCSpeciation")?,
        );

        let fuel_formulations = tables
            .iter_typed::<HcFuelFormulationRow>("FuelFormulation")?
            .into_iter()
            .map(|r| {
                (
                    r.fuel_formulation_id,
                    FuelFormulation {
                        mtbe_volume: r.mtbe_volume,
                        etbe_volume: r.etbe_volume,
                        tame_volume: r.tame_volume,
                        etoh_volume: r.etoh_volume,
                        vol_to_wt_percent_oxy: r.vol_to_wt_percent_oxy,
                    },
                )
            });
        let needed_pol_process_ids = REGISTRATIONS
            .iter()
            .map(|r| i32::from(r.pollutant_id.0) * 100 + i32::from(r.process_id.0));
        let worker_tables = OnroadWorkerTables::new(fuel_formulations, needed_pol_process_ids);

        // `MOVESWorkerOutput` carries no `fuelFormulationID` — MOVES derives one
        // per THC row by joining the county-year fuel supply (`HCFuelSupply`),
        // which expands each row across the formulations of its fuel type and
        // market-share-weights the emission. Synthesize that extract here.
        //
        // NOTE: the `methaneTHCRatio` / `HCSpeciation` lookups below key on
        // `regClassID`, with a *distinct* ratio for each of ~8 reg classes per
        // (process, fuelSubtype, modelYear). Canonical `MOVESWorkerOutput`
        // carries the concrete `regClassID` (it is aggregated away only at the
        // final output step), so each reg class is speciated with its own ratio.
        // The port's BaseRate currently collapses `regClassID` to 0 in the
        // worker output, so these lookups find no match and speciation produces
        // nothing for the onroad chained fixtures. End-to-end emission is
        // therefore blocked on BaseRate preserving `regClassID` in the worker
        // output; the derivation and expansion below are otherwise complete (the
        // unit test exercises them at reg class 0, where the ratio tables match).
        let hc_fuel_supply = synthesize_hc_fuel_supply(ctx)?;

        let thc_rows: Vec<ThcWorkerRow> = tables.iter_typed("MOVESWorkerOutput")?;
        let mut output: Vec<ThcWorkerRow> = Vec::new();
        for row in &thc_rows {
            // The SQL inserts only THC (1) and altTHC (10001) into
            // HCWorkerOutputAll (`where pollutantID in (1, 10001)`).
            if row.pollutant_id != THC_POLLUTANT_ID && row.pollutant_id != ALT_THC_POLLUTANT_ID {
                continue;
            }
            // HCFuelSupply join on (countyID, monthID, fuelTypeID, yearID) —
            // countyID is a run constant, so key on (yearID, monthID, fuelType).
            let Some(supply) =
                hc_fuel_supply.get(&(row.year_id, row.month_id, row.fuel_type_id))
            else {
                continue;
            };
            let emissions: Vec<Emission> = supply
                .iter()
                .map(|&(fuel_sub_type_id, fuel_formulation_id, market_share)| Emission {
                    fuel_sub_type_id,
                    fuel_formulation_id,
                    emission_quant: row.emission_quant * market_share,
                    emission_rate: row.emission_rate * market_share,
                })
                .collect();
            let block = FuelBlock {
                key: FuelBlockKey {
                    pollutant_id: row.pollutant_id,
                    process_id: row.process_id,
                    fuel_type_id: row.fuel_type_id,
                    reg_class_id: row.reg_class_id,
                    model_year_id: row.model_year_id,
                },
                emissions,
            };
            for speciated in speciation.speciate_block(&block, &worker_tables) {
                for em in &speciated.emissions {
                    output.push(ThcWorkerRow {
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

/// `(yearID, monthID, fuelTypeID)` → the fuel-supply formulations of that cell,
/// each `(fuelSubtypeID, fuelFormulationID, marketShare)`.
type HcFuelSupply = HashMap<(i32, i32, i32), Vec<(i32, i32, f64)>>;

/// Synthesize the `HCFuelSupply` extract each THC row joins to.
///
/// MOVES builds it from the county-year fuel supply, joined out to formulation
/// and subtype:
///
/// ```sql
/// select countyID, yearID, monthID, fst.fuelTypeID, fst.fuelSubTypeID,
///        ff.fuelFormulationID, fs.marketShare
/// from year
///   join fuelSupply     fs   on fs.fuelYearID = year.fuelYearID
///   join monthOfAnyYear moay on moay.monthGroupID = fs.monthGroupID
///   join fuelFormulation ff  on ff.fuelFormulationID = fs.fuelFormulationID
///   join fuelSubtype    fst  on fst.fuelSubtypeID = ff.fuelSubtypeID
/// ```
///
/// `HCWorkerOutputAll` then joins `MOVESWorkerOutput` to it
/// `using (countyID, monthID, fuelTypeID, yearID) where pollutantID in (1,
/// 10001)`, expanding each THC row across the cell's formulations and
/// market-share-weighting the emission. `countyID` is a run constant, so the
/// port keys on `(yearID, monthID, fuelTypeID)`. Columns are read via
/// `column_views` and cast, avoiding a `TableRow` impl per source table.
fn synthesize_hc_fuel_supply(ctx: &CalculatorContext) -> Result<HcFuelSupply, Error> {
    let tables = ctx.tables();
    let i32v = |s: &Series| -> Result<Vec<Option<i32>>, Error> {
        Ok(s.cast(&DataType::Int32)
            .map_err(|e| Error::Polars(e.to_string()))?
            .i32()
            .map_err(|e| Error::Polars(e.to_string()))?
            .into_iter()
            .collect())
    };
    let f64v = |s: &Series| -> Result<Vec<Option<f64>>, Error> {
        Ok(s.cast(&DataType::Float64)
            .map_err(|e| Error::Polars(e.to_string()))?
            .f64()
            .map_err(|e| Error::Polars(e.to_string()))?
            .into_iter()
            .collect())
    };

    // fuelYearID → yearID
    let year = tables.column_views("Year", &["yearID", "fuelYearID"])?;
    let year_of_fuel_year: HashMap<i32, i32> = i32v(&year[1])?
        .into_iter()
        .zip(i32v(&year[0])?)
        .filter_map(|(fy, y)| Some((fy?, y?)))
        .collect();

    // monthGroupID → [monthID]
    let moay = tables.column_views("MonthOfAnyYear", &["monthID", "monthGroupID"])?;
    let mut months_of_group: HashMap<i32, Vec<i32>> = HashMap::new();
    for (m, g) in i32v(&moay[0])?.into_iter().zip(i32v(&moay[1])?) {
        if let (Some(m), Some(g)) = (m, g) {
            months_of_group.entry(g).or_default().push(m);
        }
    }

    // fuelFormulationID → fuelSubtypeID
    let ff = tables.column_views("FuelFormulation", &["fuelFormulationID", "fuelSubtypeID"])?;
    let subtype_of_formulation: HashMap<i32, i32> = i32v(&ff[0])?
        .into_iter()
        .zip(i32v(&ff[1])?)
        .filter_map(|(f, s)| Some((f?, s?)))
        .collect();

    // fuelSubtypeID → fuelTypeID
    let fst = tables.column_views("FuelSubtype", &["fuelSubtypeID", "fuelTypeID"])?;
    let fuel_type_of_subtype: HashMap<i32, i32> = i32v(&fst[0])?
        .into_iter()
        .zip(i32v(&fst[1])?)
        .filter_map(|(s, t)| Some((s?, t?)))
        .collect();

    let fs = tables.column_views(
        "FuelSupply",
        &["fuelYearID", "monthGroupID", "fuelFormulationID", "marketShare"],
    )?;
    let (fuel_year, month_group, formulation, share) =
        (i32v(&fs[0])?, i32v(&fs[1])?, i32v(&fs[2])?, f64v(&fs[3])?);

    let mut out: HcFuelSupply = HashMap::new();
    for (((fy, mg), form), ms) in fuel_year
        .into_iter()
        .zip(month_group)
        .zip(formulation)
        .zip(share)
    {
        let (Some(fy), Some(mg), Some(form), Some(ms)) = (fy, mg, form, ms) else {
            continue;
        };
        let Some(&year_id) = year_of_fuel_year.get(&fy) else {
            continue;
        };
        let Some(&subtype) = subtype_of_formulation.get(&form) else {
            continue;
        };
        let Some(&fuel_type) = fuel_type_of_subtype.get(&subtype) else {
            continue;
        };
        let Some(months) = months_of_group.get(&mg) else {
            continue;
        };
        for &month in months {
            out.entry((year_id, month, fuel_type))
                .or_default()
                .push((subtype, form, ms));
        }
    }
    Ok(out)
}

/// Construct the calculator as a boxed trait object — matches the engine's
/// calculator-factory signature so the registry can register it.
#[must_use]
pub fn factory() -> Box<dyn Calculator> {
    Box::new(HcSpeciationCalculator)
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
    fn execute_wires_through_data_plane() {
        use moves_framework::DataFrameStore;
 // Fixture from fixture(): process 1, subtype 20, formulation 100,
 // reg_class 0, model_year 2005, methane ratio 0.25.
 // Expected: methane=2.0, NMHC=6.0, NMOG=4.5, TOG=6.5, VOC=3.0.
        let worker_rows = vec![ThcWorkerRow {
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
            source_type_id: 21,
            reg_class_id: 0,
            fuel_type_id: 1,
            model_year_id: 2005,
            road_type_id: 4,
            fuel_sub_type_id: 20,
            fuel_formulation_id: 100,
            emission_quant: 8.0,
            emission_rate: 4.0,
        }];
        let methane_rows = vec![MethaneThcRatioRow {
            process_id: 1,
            fuel_sub_type_id: 20,
            reg_class_id: 0,
            begin_model_year_id: 2000,
            end_model_year_id: 2010,
            ch4_thc_ratio: 0.25,
        }];
        let hc_rows = vec![
            HcSpeciationRow {
                pol_process_id: NMOG_POLLUTANT_ID * 100 + 1,
                fuel_sub_type_id: 20,
                reg_class_id: 0,
                begin_model_year_id: 2000,
                end_model_year_id: 2010,
                speciation_constant: 0.25,
                oxy_speciation: 0.25,
            },
            HcSpeciationRow {
                pol_process_id: VOC_POLLUTANT_ID * 100 + 1,
                fuel_sub_type_id: 20,
                reg_class_id: 0,
                begin_model_year_id: 2000,
                end_model_year_id: 2010,
                speciation_constant: 0.5,
                oxy_speciation: 0.0,
            },
        ];
        let mut store = moves_framework::InMemoryStore::new();
        store.insert(
            "MOVESWorkerOutput",
            ThcWorkerRow::into_dataframe(worker_rows).unwrap(),
        );
        store.insert(
            "methaneTHCRatio",
            MethaneThcRatioRow::into_dataframe(methane_rows).unwrap(),
        );
        store.insert(
            "HCSpeciation",
            HcSpeciationRow::into_dataframe(hc_rows).unwrap(),
        );
        // `FuelFormulation` carries the oxygenate volumes (read by execute) and
        // `fuelSubtypeID` (read by the HCFuelSupply synthesis), so build it with
        // both rather than via `HcFuelFormulationRow::into_dataframe`.
        let col_i32 = |name: &str, v: i32| Series::new(name.into(), vec![v]).into();
        let col_f64 = |name: &str, v: f64| Series::new(name.into(), vec![v]).into();
        store.insert(
            "FuelFormulation",
            DataFrame::new(
                1,
                vec![
                    col_i32("fuelFormulationID", 100),
                    col_i32("fuelSubtypeID", 20),
                    col_f64("MTBEVolume", 2.0),
                    col_f64("ETBEVolume", 0.0),
                    col_f64("TAMEVolume", 0.0),
                    col_f64("ETOHVolume", 2.0),
                    col_f64("volToWtPercentOxy", 0.5),
                ],
            )
            .unwrap(),
        );
        // HCFuelSupply source tables: a single formulation (100) of fuel type 1,
        // subtype 20, marketShare 1.0 in (year 2020, month 7) — so the THC row's
        // 8.0 expands one-to-one and the speciated values are unchanged.
        store.insert(
            "Year",
            DataFrame::new(
                1,
                vec![col_i32("yearID", 2020), col_i32("fuelYearID", 2020)],
            )
            .unwrap(),
        );
        store.insert(
            "FuelSupply",
            DataFrame::new(
                1,
                vec![
                    col_i32("fuelYearID", 2020),
                    col_i32("monthGroupID", 7),
                    col_i32("fuelFormulationID", 100),
                    col_f64("marketShare", 1.0),
                ],
            )
            .unwrap(),
        );
        store.insert(
            "MonthOfAnyYear",
            DataFrame::new(
                1,
                vec![col_i32("monthID", 7), col_i32("monthGroupID", 7)],
            )
            .unwrap(),
        );
        store.insert(
            "FuelSubtype",
            DataFrame::new(
                1,
                vec![col_i32("fuelSubtypeID", 20), col_i32("fuelTypeID", 1)],
            )
            .unwrap(),
        );
        let ctx = CalculatorContext::with_tables(store);
        let out = HcSpeciationCalculator::new()
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
 // Find each species and check its value.
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
 // factor = 0.25 + 0.25*0.5*4.0 = 0.75; NMOG = 6.0 * 0.75 = 4.5
        assert!((find(NMOG_POLLUTANT_ID) - 4.5).abs() < 1e-9, "NMOG");
 // TOG = NMOG + methane = 4.5 + 2.0 = 6.5
        assert!((find(TOG_POLLUTANT_ID) - 6.5).abs() < 1e-9, "TOG");
 // factor = 0.5; VOC = 6.0 * 0.5 = 3.0
        assert!((find(VOC_POLLUTANT_ID) - 3.0).abs() < 1e-9, "VOC");
    }

    #[test]
    fn calculator_is_object_safe() {
 // The registry stores calculators as Box<dyn Calculator>.
        let calc: Box<dyn Calculator> = Box::new(HcSpeciationCalculator::new());
        assert_eq!(calc.name(), "HCSpeciationCalculator");
        assert_eq!(calc.registrations().len(), 40);
    }
}
