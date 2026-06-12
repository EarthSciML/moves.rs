//! Pre-loaded input bundle for [`run_simulation`](super::run_simulation)
//! the in-memory replacement for NONROAD's input *files*.
//!
//! In the Java↔Fortran bridge this task replaces, MOVES wrote ~30
//! fixed-width input files (`.POP`, `.ALO`, `.GRW`, …) to a worker
//! scratch directory and `nonroad.exe` read them back. The Rust
//! orchestrator instead parses its source data once, in memory, into a
//! [`NonroadInputs`] value — no scratch files, no re-parsing.
//!
//! # What this type carries
//!
//! [`NonroadInputs`] holds the two things the **driver loop** needs:
//!
//! - the population records, pre-grouped by SCC into [`SccGroup`]s//! one group is what `getpop` returns per outer-loop pass
//! (`nonroad.f` label `111`);
//! - the [`RunRegions`] selection tables the inner record loop filters
//! against ([`plan_scc_group`](crate::driver::plan_scc_group)).
//!
//! The deeper per-record reference data — emission-factor, technology,
//! activity, growth, and allocation tables — is consumed not by the
//! driver loop but by the geography routines, behind the
//! [`GeographyExecutor`](super::GeographyExecutor) seam. A production
//! `GeographyExecutor` owns that data; keeping it out of
//! [`NonroadInputs`] keeps the driver-loop contract small and lets the
//! executor evolve independently.

use std::collections::BTreeMap;

use crate::driver::{DriverRecord, RunRegions};
use crate::emissions::exhaust::EmissionUnitCode;
use crate::geography::common::ActivityUnit;
use crate::input::alo::AllocationRecord;
use crate::input::indicator::IndicatorTable;
use crate::input::spillage::SpillageRecord;
use crate::population::retrofit::RetrofitRecord;
use crate::population::{AgeAdjustmentTable, GrowthIndicatorRecord, ScrappageCurve};

/// One SCC group's worth of population records, in file order.
///
/// The Fortran `getpop` routine returns exactly this: all population
/// records sharing one Source Classification Code, ordered as they
/// appeared in the `.POP` input. `nonroad.f`'s outer loop processes
/// the groups one at a time; [`run_simulation`](super::run_simulation)
/// iterates [`NonroadInputs::scc_groups`] in the same way.
#[derive(Debug, Clone, PartialEq)]
pub struct SccGroup {
    /// The 10-character SCC shared by every record in [`records`](Self::records).
    pub scc: String,
    /// The group's population records, in `.POP`-file order. Growth
    /// pairs (a base record immediately followed by its projection
    /// record) must stay adjacent — the inner loop's lookahead
    /// ([`growth_pair`](crate::driver::growth_pair)) depends on it.
    pub records: Vec<DriverRecord>,
}

impl SccGroup {
    /// Bundle `records` under their shared `scc`.
    pub fn new(scc: impl Into<String>, records: Vec<DriverRecord>) -> Self {
        Self {
            scc: scc.into(),
            records,
        }
    }

    /// Number of population records in the group.
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// `true` when the group carries no records. A no-op group: the
    /// driver still classifies its fuel but the record loop is empty.
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

/// The complete pre-loaded input bundle handed to
/// [`run_simulation`](super::run_simulation).
///
/// See the module docs for the rationale behind carrying only the
/// driver-loop inputs (SCC groups + region selection) and leaving the
/// reference tables to the [`GeographyExecutor`](super::GeographyExecutor).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct NonroadInputs {
    /// Population records grouped by SCC — one [`SccGroup`] per outer-
    /// loop pass. Order is preserved into the output; sort upstream if
    /// a particular SCC order is wanted.
    pub scc_groups: Vec<SccGroup>,
    /// The run's state / county selection and the subcounty region
    /// list — Fortran `statcd`/`fipcod`/`reglst`. The inner record
    /// loop filters every record against these.
    pub regions: RunRegions,
}

impl NonroadInputs {
    /// Create an empty input bundle — no SCC groups, no region
    /// selection. A [`run_simulation`](super::run_simulation) over this
    /// produces an empty [`NonroadOutputs`](super::NonroadOutputs) with
    /// a successful completion message.
    pub fn new() -> Self {
        Self::default()
    }

    /// Append an [`SccGroup`] built from `scc` and `records`.
    ///
    /// Returns `&mut Self` so groups can be chained onto a freshly
    /// constructed bundle.
    pub fn push_group(&mut self, scc: impl Into<String>, records: Vec<DriverRecord>) -> &mut Self {
        self.scc_groups.push(SccGroup::new(scc, records));
        self
    }

    /// Total population records across every SCC group.
    pub fn record_count(&self) -> usize {
        self.scc_groups.iter().map(SccGroup::len).sum()
    }

    /// Number of SCC groups in the bundle.
    pub fn group_count(&self) -> usize {
        self.scc_groups.len()
    }

    /// `true` when the bundle has no SCC groups at all.
    pub fn is_empty(&self) -> bool {
        self.scc_groups.is_empty()
    }
}

// =============================================================================
// Reference-data entry types and ReferenceData bundle
// =============================================================================

/// One exhaust-tech-type entry for [`ProductionExecutor`](super::executor::ProductionExecutor) (Fortran `fndtch`).
///
/// Linear-scan key: `scc` + HP range `[hp_min, hp_max]`. The
/// `tech_year` field is not currently used in the lookup — the caller
/// resolves the year via `min(model_year, options.tech_year)` before
/// dispatching, so any single entry covers all years until a finer
/// loader is ported.
#[derive(Debug, Clone, Default)]
pub struct ExhaustTechEntry {
    /// 10-character SCC code.
    pub scc: String,
    /// Lower bound of the HP range (inclusive).
    pub hp_min: f32,
    /// Upper bound of the HP range (inclusive).
    pub hp_max: f32,
    /// Per-tech-slot names (`tectyp(idxtch, 1..n)`).
    pub tech_names: Vec<String>,
    /// Per-tech-slot fractions (`tchfrc(idxtch, 1..n)`). Must be the
    /// same length as `tech_names`.
    pub tech_fractions: Vec<f32>,
    /// BSFC (brake-specific fuel consumption) in lb/HP-hr per tech slot.
    /// Used by `compute_exhaust_factors` to populate the BSFC array for
    /// CO2 and SOx calculations. Must be the same length as `tech_names`.
    pub bsfc: Vec<f32>,
    /// Per-`(pollutant slot, tech slot)` exhaust emission factors, row-
    /// major as `[pollutant_slot * tech_names.len() + tech]` (Fortran
    /// `emsfac` / `emfac`, sourced from NR\*.EMF — here from the MOVES
    /// `nremissionrate` table). The base rate is constant across calendar
    /// years; the model-year/age variation enters through deterioration.
    ///
    /// Empty ⇒ all factors zero, preserving the legacy behaviour where
    /// only the BSFC-derived CO2/SOx pollutants are produced. When
    /// non-empty its length is `MXPOL * tech_names.len()`.
    pub emission_factors: Vec<f32>,
    /// Per-`(pollutant slot, tech slot)` EF unit codes, same layout as
    /// [`emission_factors`](Self::emission_factors). Empty ⇒ every slot
    /// defaults to g/HP-hr.
    pub emission_units: Vec<EmissionUnitCode>,
    /// Per-`(pollutant slot, tech slot)` deterioration A coefficient
    /// (`adetcf`), same layout as [`emission_factors`](Self::emission_factors).
    pub det_a: Vec<f32>,
    /// Per-`(pollutant slot, tech slot)` deterioration B (age-exponent)
    /// coefficient (`bdetcf`), same layout.
    pub det_b: Vec<f32>,
    /// Per-`(pollutant slot, tech slot)` deterioration age cap
    /// (`detcap`), same layout.
    pub det_cap: Vec<f32>,
    /// Per-model-year tech fractions: `model_year → fractions` aligned to
    /// [`tech_names`](Self::tech_names). The base emission rates are model-
    /// year independent, but the tech mix phases cleaner technology in over
    /// model years (`tchfrc` is read at the per-model-year `tchmdyr`).
    /// Empty ⇒ the single [`tech_fractions`](Self::tech_fractions) vector
    /// is used for every model year (legacy behaviour).
    pub tech_fractions_by_year: BTreeMap<i32, Vec<f32>>,
}

impl ExhaustTechEntry {
    /// Tech fractions to use for model year `year`. Resolves
    /// [`tech_fractions_by_year`](Self::tech_fractions_by_year) by exact
    /// match, then the nearest earlier year, then the earliest available;
    /// falls back to the model-year-independent
    /// [`tech_fractions`](Self::tech_fractions) when no per-year data is
    /// loaded.
    pub fn fractions_for_year(&self, year: i32) -> &[f32] {
        if self.tech_fractions_by_year.is_empty() {
            return &self.tech_fractions;
        }
        if let Some(v) = self.tech_fractions_by_year.get(&year) {
            return v;
        }
        if let Some((_, v)) = self.tech_fractions_by_year.range(..=year).next_back() {
            return v;
        }
        if let Some((_, v)) = self.tech_fractions_by_year.iter().next() {
            return v;
        }
        &self.tech_fractions
    }
}

/// One evap-tech-type entry for [`ProductionExecutor`](super::executor::ProductionExecutor) (Fortran `fndevtch`).
///
/// Same key and lookup semantics as [`ExhaustTechEntry`].
#[derive(Debug, Clone, Default)]
pub struct EvapTechEntry {
    /// 10-character SCC code.
    pub scc: String,
    /// Lower bound of the HP range (inclusive).
    pub hp_min: f32,
    /// Upper bound of the HP range (inclusive).
    pub hp_max: f32,
    /// Per-evap-tech-slot names (`evtecnam(idxtch, 1..n)`).
    pub tech_names: Vec<String>,
    /// Per-evap-tech-slot fractions (`evtchfrc(idxtch, 1..n)`).
    pub tech_fractions: Vec<f32>,
    /// Per-`(evap-species slot, tech slot)` evap emission factors, row-
    /// major as `[evap_species_slot * tech_names.len() + tech]` (from the
    /// MOVES `nrevapemissionrate` table). The base rate is constant across
    /// calendar years; age variation enters through deterioration.
    ///
    /// Empty ⇒ all factors zero (no evap emissions computed). When
    /// non-empty its length is `MXPOL * tech_names.len()`.
    pub emission_factors: Vec<f32>,
    /// Per-`(evap-species slot, tech slot)` EF unit codes, same layout as
    /// [`emission_factors`](Self::emission_factors). Empty ⇒ defaults to
    /// `GramsPerHour` for every slot.
    pub unit_codes: Vec<EmissionUnitCode>,
    /// Per-`(evap-species slot, tech slot)` deterioration A coefficient,
    /// same layout as [`emission_factors`](Self::emission_factors).
    pub det_a: Vec<f32>,
    /// Per-`(evap-species slot, tech slot)` deterioration B (age-exponent)
    /// coefficient, same layout.
    pub det_b: Vec<f32>,
    /// Per-`(evap-species slot, tech slot)` deterioration age cap,
    /// same layout.
    pub det_cap: Vec<f32>,
}

/// Growth cross-reference entry for [`ProductionExecutor`](super::executor::ProductionExecutor) (Fortran `fndgxf`).
///
/// Maps `(fips, scc, hp range)` → growth indicator code.
#[derive(Debug, Clone, Default)]
pub struct GrowthXrefEntry {
    /// 5-character county FIPS (`fipin`).
    pub fips: String,
    /// 10-character SCC code (`asccod`).
    pub scc: String,
    /// Lower bound of the HP range (inclusive).
    pub hp_min: f32,
    /// Upper bound of the HP range (inclusive).
    pub hp_max: f32,
    /// Growth indicator code (`indcod`). `None` means no growth-pattern
    /// cross-reference matched this SCC — canonical `fndgxf` returns
    /// `idxgrw ≤ 0`, which the caller treats as a fatal error when growth
    /// is active (prccty.f label 7001).
    pub indicator: Option<String>,
}

/// Activity lookup entry for [`ProductionExecutor`](super::executor::ProductionExecutor) (Fortran `fndact`).
///
/// Key: `(scc, fips)`. The HP is not matched in the linear scan/// the Fortran `fndact` searches by SCC and FIPS only, then returns
/// the first matching activity record.
#[derive(Debug, Clone)]
pub struct ActivityTableEntry {
    /// 10-character SCC code.
    pub scc: String,
    /// 5-character county FIPS, or empty to match any FIPS.
    pub fips: String,
    /// Starts per period (`starts(idxact)`).
    pub starts: f32,
    /// Activity level (`actlev(idxact)`).
    pub activity_level: f32,
    /// Activity-units indicator (`iactun(idxact)`).
    pub activity_unit: ActivityUnit,
    /// Load factor (`faclod(idxact)`).
    pub load_factor: f32,
    /// Age-curve code (`actage(idxact)`).
    pub age_code: String,
}

/// National-to-state allocation entry for [`ProductionExecutor`](super::executor::ProductionExecutor).
///
/// Carries the per-SCC regression coefficients and indicator codes
/// from the `ALLOCATE.XRF` `/ALLOC XREF/` packet (`rdalo.f`).
/// `NationalAdapter::find_allocation` succeeds when an entry for the
/// SCC exists; `NationalAdapter::allocate_to_states` uses the embedded
/// [`AllocationRecord`] together with [`ReferenceData::allocation_indicators`]
/// to compute the canonical `alosta.f` coefficient-weighted ratio.
#[derive(Debug, Clone, Default)]
pub struct NationalAllocationEntry {
    /// 10-character SCC code.
    pub scc: String,
    /// Regression coefficients and indicator codes for this SCC
    /// (from the NR*.ALO `/ALLOC XREF/` packet, parsed by `rdalo.f`).
    pub record: AllocationRecord,
}

/// Monthly and daily temporal profile for one SCC — NR*.TMF (SEASON.DAT) record.
///
/// Canonical Fortran: `mthfac(1..12, idx)` and `dayfac(1..2, idx)` from
/// `nonrdtpl.inc` / `rdseas.f`. In MOVES the equivalent data lives in
/// `nrmonthallocation` (12 rows per SCC, one per month) and
/// `nrdayallocation` (2 rows per SCC, weekday + weekend).
#[derive(Debug, Clone)]
pub struct TemporalProfile {
    /// Monthly fractions, index 0 = January … 11 = December.
    /// Canonical normalization: sum = 1.0.
    pub monthly: [f32; 12],
    /// Day-of-week fractions: index 0 = weekday, 1 = weekend.
    /// Canonical normalization: 5×weekday + 2×weekend = 1.0.
    pub daily: [f32; 2],
}

/// Reference tables loaded once per run by the orchestrator.
///
/// Aggregates every reference table [`ProductionExecutor`](super::executor::ProductionExecutor) needs to
/// evaluate the six NONROAD geography routines. Built once by the
/// orchestrator from the parsed input files and passed by reference to
/// [`ProductionExecutor::new`](super::executor::ProductionExecutor::new).
///
/// # Fortran COMMON-block sources
///
/// Each field name maps to one or more Fortran COMMON blocks or
/// parallel arrays from the NONROAD source. Fields still marked
/// **⚠ NOT YET LOADABLE** have no ported loader; their `Vec<u8>`
/// placeholder signals intent without blocking compilation.
#[derive(Debug, Clone, Default)]
pub struct ReferenceData {
    /// Exhaust tech-type fractions and names — one entry per
    /// `(SCC, HP range)` bucket. Fortran: `TCHFRC`, `TECTYP` from
    /// NR*.EF emission-factor files (`rdtech.f`).
    pub exhaust_tech_entries: Vec<ExhaustTechEntry>,
    /// Evap tech-type fractions and names — same structure as
    /// [`exhaust_tech_entries`](Self::exhaust_tech_entries). Fortran:
    /// `EVTCHFRC`, `EVTECTYP` from NR*.EF files (`rdevtech.f`).
    pub evap_tech_entries: Vec<EvapTechEntry>,
    /// Emission-factor records from NR*.EMF files. Fortran: emission-
    /// factor arrays `EMFAC`, `EMIYR` from `rdemfac.f`.
    /// **⚠ NOT YET LOADABLE.**
    pub emission_factors: Vec<u8>,
    /// Activity lookup entries — one per `(SCC, FIPS)` bucket. Fortran:
    /// `ACTLEV`, `FACLOD`, `IACTUN`, `ACTAGE`, `STARTS` from NR*.ACT
    /// files (`rdact.f`).
    pub activity_entries: Vec<ActivityTableEntry>,
    /// Growth cross-reference entries — one per `(FIPS, SCC, HP range)`.
    /// Fortran: `GXFDAT` table from NR*.GRW indicator files (`rdgrow.f`).
    pub growth_xref_entries: Vec<GrowthXrefEntry>,
    /// Growth indicator records for every indicator code referenced in
    /// [`growth_xref_entries`](Self::growth_xref_entries). Fortran:
    /// growth-factor arrays `GRWFAC`, `GRWFIP` from NR*.GRW files
    /// (`rdgrow.f`).
    pub growth_records: Vec<GrowthIndicatorRecord>,
    /// Scrappage curve (`getscrp`-resolved). Fortran: `SCRPFRC` array
    /// from NR*.POP scrappage data.
    pub scrappage_curve: ScrappageCurve,
    /// Alternate age-adjustment table. Fortran: `AGEADJ` from the
    /// `/AGE ADJUSTMENT/` packet in NR*.ACT files (`rdact.f`). Defaults
    /// to an empty table (DEFAULT curve only).
    pub age_adjustment_table: AgeAdjustmentTable,
    /// Monthly and daily temporal profiles from NR*.TMF (SEASON.DAT /
    /// `nrmonthallocation` + `nrdayallocation`). Keyed by SCC. Missing
    /// SCC falls back to canonical defaults: monthly = [1/12; 12],
    /// daily = [1/7; 2] (`rdseas.f` :215–221 — `defmth`/`defday`).
    pub temporal_profiles: std::collections::BTreeMap<String, TemporalProfile>,
    /// Which months are active for this run — Fortran `lmonth(12)`.
    /// Index 0 = January … 11 = December. Set from the run-spec month
    /// selection (1 month for a MOVES typical-day run; all 12 for annual).
    /// All `false` means "no temporal data loaded; use defaults".
    pub months_selected: [bool; 12],
    /// `true` when weekday activity is selected, `false` for weekend.
    /// Fortran `ldays(IDXWKD)`. MOVES `dayID = 5` → weekday, `2` → weekend.
    pub weekday_selected: bool,
    /// Refueling/spillage-mode records from NR*.SPL files. Fortran:
    /// `MODSPL`, `VOLSPL`, and associated permeation arrays from `rdspil.f`.
    /// Non-empty when a spillage file was loaded; drives `find_refueling`.
    pub spillage_records: Vec<SpillageRecord>,
    /// National-to-state allocation entries keyed by SCC. Fortran:
    /// `ALOSTA` allocation data from NR*.ALO files (`rdalo.f`).
    pub national_allocation: Vec<NationalAllocationEntry>,
    /// County and subcounty allocation cross-reference records from NR*.SCO
    /// files. Fortran: `ascalo`, `coeffs`, `indcod` from `rdalo.f`
    /// (same format as the NR*.ALO national-to-state file). Each entry maps
    /// an SCC to up to [`crate::input::alo::MAX_COEF`] spatial-indicator
    /// codes with regression coefficients for `alocty.f` (county) and
    /// `alosub.f` (subcounty) allocation.
    pub county_allocation_records: Vec<AllocationRecord>,
    /// Spatial-indicator values from NR*.IND files, queried by the county
    /// and subcounty allocation functions (`getind.f`). Built from all
    /// indicator records and used by [`crate::allocation::allocate_county`]
    /// and [`crate::allocation::allocate_subcounty`].
    pub county_allocation_indicators: IndicatorTable,
    /// Spatial-indicator values for national-to-state allocation.
    /// Loaded from the US_*.ALO indicator files. Fortran: the sorted
    /// spatial-indicator scratch file read by `getind.f` at the
    /// national (`00000`) and per-state FIPS levels.
    pub allocation_indicators: IndicatorTable,
    /// Retrofit records from NR*.RFT files. Fortran: `RTRFTDAT` from
    /// `rdrft.f` (`population::retrofit::RetrofitRecord`).
    pub retrofit_records: Vec<RetrofitRecord>,
    /// Fuel oxygen content (weight %) for the gasoline exhaust oxygenate
    /// correction (`emsadj.f` :228–256). `0.0` ⇒ no oxygenate correction.
    pub fuel_oxygen_pct: f32,
    /// `true` when the gasoline supply is reformulated (RFG); RFG fuel skips
    /// the oxygenate / sulfur corrections and takes the RFG-bin path instead.
    pub fuel_rfg: bool,
    /// Ambient temperature (°F) for the exhaust temperature corrections
    /// (`emsadj.f` :167–220). `None` means `zonemonthhour` was not loaded;
    /// `emission_adjustments` returns an error rather than fabricating 75 °F.
    /// Used as the fallback when an SCC has no entry in `ambient_temp_by_scc`.
    pub ambient_temp_f: Option<f32>,
    /// Per-SCC ambient temperature (°F) for the exhaust temperature
    /// corrections, activity-weighted by the equipment's hour-allocation
    /// pattern (`nrhourpatternfinder` → `nrhourallocation`). The temperature
    /// correction is non-linear (`exp`), so the activity-weighted mean (which
    /// favours warm daytime hours for daylight-use equipment) is what the
    /// canonical reproduces — a flat 24-hour mean biases NOx high and CO/THC
    /// low. Empty ⇒ fall back to the scalar `ambient_temp_f`.
    pub ambient_temp_by_scc: std::collections::BTreeMap<String, f32>,
    /// In-use fuel sulfur weight % per fuel slot `[gas-2str, gas-4str,
    /// diesel, LPG, CNG]` — the `.opt` OPTIONS `Gas/Diesel/CNG-LPG sulfur %`
    /// values (`soxful` in `emsadj.f`; the SOx correction is
    /// `soxful/soxbas`). `None` ⇒ neutral correction (`soxful = soxbas`),
    /// preserving the pre-sulfur-wiring behaviour for callers that do not
    /// load fuel-supply data.
    pub fuel_sulfur_pct: Option<[f32; 5]>,
    /// In-use marine-diesel sulfur weight % (`soxdsm`, the rec-marine SCC
    /// override in `emsadj.f` :267). Only meaningful when
    /// `fuel_sulfur_pct` is `Some`.
    pub fuel_sulfur_marine: f32,
    /// Per-tech sulfur alternates keyed by tech name (engTechID string) —
    /// the `.opt` `/PM BASE SULFUR/` packet (`nrsulfuradjustment`, diesel
    /// fuel types 23/24; `sultec`/`sulalt`/`sulcnv` in `clcems.f`). Used by
    /// the SOx EF rewrite and the diesel PM sulfur correction.
    pub sulfur_alternates:
        std::collections::BTreeMap<String, crate::emissions::exhaust::SulfurAlternate>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(region: &str, hp: f32, pop: f32, year: i32) -> DriverRecord {
        DriverRecord {
            region_code: region.to_string(),
            hp_avg: hp,
            population: pop,
            pop_year: year,
            median_life: 0.0,
        }
    }

    #[test]
    fn scc_group_reports_length_and_emptiness() {
        let group = SccGroup::new("2270001010", vec![rec("06037", 25.0, 100.0, 2020)]);
        assert_eq!(group.scc, "2270001010");
        assert_eq!(group.len(), 1);
        assert!(!group.is_empty());

        let empty = SccGroup::new("2265001010", Vec::new());
        assert!(empty.is_empty());
        assert_eq!(empty.len(), 0);
    }

    #[test]
    fn new_inputs_are_empty() {
        let inputs = NonroadInputs::new();
        assert!(inputs.is_empty());
        assert_eq!(inputs.group_count(), 0);
        assert_eq!(inputs.record_count(), 0);
    }

    #[test]
    fn push_group_accumulates_and_chains() {
        let mut inputs = NonroadInputs::new();
        inputs
            .push_group("2270001010", vec![rec("06037", 25.0, 100.0, 2020)])
            .push_group(
                "2265001010",
                vec![
                    rec("06037", 10.0, 50.0, 2020),
                    rec("06038", 10.0, 60.0, 2020),
                ],
            );
        assert_eq!(inputs.group_count(), 2);
        assert_eq!(inputs.record_count(), 3);
        assert!(!inputs.is_empty());
        assert_eq!(inputs.scc_groups[0].scc, "2270001010");
        assert_eq!(inputs.scc_groups[1].records.len(), 2);
    }

    #[test]
    fn record_count_sums_across_groups() {
        let inputs = NonroadInputs {
            scc_groups: vec![
                SccGroup::new("a", vec![rec("06037", 1.0, 1.0, 2020)]),
                SccGroup::new("b", Vec::new()),
                SccGroup::new(
                    "c",
                    vec![rec("06038", 1.0, 1.0, 2020), rec("06039", 1.0, 1.0, 2020)],
                ),
            ],
            regions: RunRegions::default(),
        };
        assert_eq!(inputs.record_count(), 3);
        assert_eq!(inputs.group_count(), 3);
    }
}
