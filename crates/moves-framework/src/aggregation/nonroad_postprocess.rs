//! NONROAD-specific output post-processing — ports the summarization
//! scripts in `database/NonroadProcessingScripts/` (migration plan
//! Task 118).
//!
//! Canonical MOVES ships ~19 SQL scripts that a user runs against a
//! finished NONROAD run's output database to roll the raw
//! `MOVESOutput` / `MOVESActivityOutput` rows up into summary tables:
//! emissions inventories grouped by county / equipment / sector, the
//! equipment population, and emission factors expressed per operating
//! hour, per horsepower-hour, or per vehicle. Each script is a
//! `CREATE TABLE … SELECT … SUM(…) … GROUP BY …` wrapped in index
//! bookkeeping; the meaningful logic is ~300 lines once the
//! `CREATE INDEX` / `information_schema` boilerplate is stripped.
//!
//! This module ports the 18 *summarization* scripts. The 19th,
//! `DecodedNonroadOutput.sql`, is deliberately out of scope: it does
//! not summarize anything (its own header notes it "does not generate
//! a spreadsheet of results, like other Nonroad Post-Processing
//! Scripts") — it only decorates each raw row with name columns joined
//! from `translate_*` reference tables that have not been ported.
//!
//! | Family | Scripts | Function |
//! |---|---|---|
//! | Inventory  | 6 | [`inventory`] |
//! | Population | 1 | [`population_by_sector_and_scc`] |
//! | Emission factors | 11 | [`emission_factors`] |
//!
//! # Why typed records, not Polars
//!
//! The migration plan sketches these scripts becoming Polars
//! expressions. As with the sibling [`output_aggregate`](super::output_aggregate)
//! module — the port of `OutputProcessor.java`'s `GROUP BY` roll-up —
//! the aggregation runs over strongly-typed [`EmissionRecord`] /
//! [`ActivityRecord`] vectors instead: `moves-framework` carries no
//! `polars` dependency, and the concrete `DataFrameStore` data plane
//! (Task 50) has not landed. The group-by / `SUM` mechanics are
//! identical whichever row representation the data plane ultimately
//! delivers; this module's tests pin the reference semantics a future
//! `LazyFrame` port must reproduce.
//!
//! # SQL fidelity notes
//!
//! * **`SUM` and `NULL`.** Metric sums use SQL `SUM` semantics: `NULL`
//!   inputs are skipped, and a group whose every metric input is
//!   `NULL` yields `NULL` (`None`) rather than `0.0`.
//! * **Mass-unit conversion.** The emission-factor scripts multiply
//!   `SUM(emissionQuant)` by a `movesrun.massUnits`→grams factor (the
//!   inline `units` table); [`mass_units_to_grams`] is that table. An
//!   unrecognized unit reproduces the SQL `LEFT JOIN` miss — the
//!   factor is `NULL`, so the converted quantity is `None`. The
//!   inventory scripts do *not* convert: they carry `massUnits`
//!   through as a label column.
//! * **Reference joins.** The equipment-grouped scripts join
//!   `movesoutput.SCC` to the `nrscc` / `nrequipmenttype` /
//!   `nrhprangebin` reference tables. Those are not part of the
//!   unified output schema, so the caller supplies them through
//!   [`NrSccLookup`] (the same dependency-injection shape the
//!   aggregator uses for [`TemporalScalingFactors`](super::TemporalScalingFactors)).
//!   A lookup miss leaves the derived column `None`, matching the
//!   `LEFT JOIN`.
//! * **Determinism.** Output rows are emitted in group-key sort order,
//!   independent of input order.
//!
//! [`EmissionRecord`]: moves_data::EmissionRecord
//! [`ActivityRecord`]: moves_data::ActivityRecord

use std::collections::{BTreeMap, HashMap};

use moves_data::output_schema::{ActivityRecord, EmissionRecord, MovesRunRecord};

// ---------------------------------------------------------------------------
// Activity-type IDs and unit conversion
// ---------------------------------------------------------------------------

/// `movesactivityoutput.activityTypeID` for total source (operating)
/// hours — the operating-hour emission-factor denominator.
const ACTIVITY_SOURCE_HOURS: i16 = 2;
/// `activityTypeID` for equipment population — the per-vehicle
/// emission-factor denominator and the [`population_by_sector_and_scc`]
/// metric.
const ACTIVITY_POPULATION: i16 = 6;
/// `activityTypeID` for average horsepower — one of the three
/// horsepower-hour factors.
const ACTIVITY_AVERAGE_HORSEPOWER: i16 = 9;
/// `activityTypeID` for load factor — one of the three
/// horsepower-hour factors.
const ACTIVITY_LOAD_FACTOR: i16 = 12;

/// Grams per unit of mass for a `movesrun.massUnits` string — the
/// inline `units` table every emission-factor script defines:
///
/// ```sql
/// insert into units values
/// ('ton', 907185, …), ('lb', 453.592, …), ('kg', 1000, …), ('g', 1, …);
/// ```
///
/// The comparison is case-insensitive, matching the MySQL default
/// collation of the script's `massUnits = units.fromUnit` join. An
/// unrecognized unit returns `None` — the SQL `LEFT JOIN` miss, which
/// propagates a `NULL` factor (and therefore a `NULL` converted
/// quantity).
#[must_use]
pub fn mass_units_to_grams(mass_units: &str) -> Option<f64> {
    match mass_units.to_ascii_lowercase().as_str() {
        "ton" => Some(907_185.0),
        "lb" => Some(453.592),
        "kg" => Some(1000.0),
        "g" => Some(1.0),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// NONROAD reference-table lookup
// ---------------------------------------------------------------------------

/// One `nrscc` row: the equipment type and fuel type an SCC code maps
/// to, plus the human-readable SCC description.
#[derive(Debug, Clone, Default, PartialEq)]
struct NrSccEntry {
    nr_equip_type_id: Option<i32>,
    fuel_type_id: Option<i16>,
    description: Option<String>,
}

/// One `nrequipmenttype` row: the sector an equipment type belongs to
/// and its description.
#[derive(Debug, Clone, Default, PartialEq)]
struct NrEquipmentEntry {
    sector_id: Option<i16>,
    description: Option<String>,
}

/// Reference data for the equipment-grouped post-processing scripts.
///
/// The scripts that group by equipment type, sector, or horsepower bin
/// join `movesoutput` / `movesactivityoutput` against the NONROAD
/// reference tables `nrscc` (SCC → equipment type, fuel type,
/// description), `nrequipmenttype` (equipment type → sector,
/// description), and `nrhprangebin` (HP-range-bin ID → bin name);
/// `EmissionFactors_per_hphr_by_SCC_and_ModelYear` additionally joins
/// `enginetech` (engine-tech ID → description).
///
/// These tables are not part of the unified output schema, so the
/// caller builds this lookup from whichever reference source the run
/// loaded and passes it to [`inventory`] / [`emission_factors`].
/// Every accessor returns `None` on a miss, reproducing the scripts'
/// `LEFT JOIN` behaviour. An empty lookup ([`NrSccLookup::default`])
/// is valid: SCC-grouped scripts that need no reference join still
/// work; equipment-grouped scripts collapse their derived columns to
/// `None`.
#[derive(Debug, Clone, Default)]
pub struct NrSccLookup {
    scc: HashMap<String, NrSccEntry>,
    equipment: HashMap<i32, NrEquipmentEntry>,
    hp_bins: HashMap<i16, String>,
    eng_tech: HashMap<i16, String>,
}

impl NrSccLookup {
    /// An empty lookup. Equivalent to [`NrSccLookup::default`].
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register one `nrscc` row: the equipment type, fuel type, and
    /// description an SCC code maps to. Builder-style; chains.
    #[must_use]
    pub fn with_scc(
        mut self,
        scc: impl Into<String>,
        nr_equip_type_id: Option<i32>,
        fuel_type_id: Option<i16>,
        description: Option<String>,
    ) -> Self {
        self.scc.insert(
            scc.into(),
            NrSccEntry {
                nr_equip_type_id,
                fuel_type_id,
                description,
            },
        );
        self
    }

    /// Register one `nrequipmenttype` row: the sector and description
    /// for an equipment-type ID. Builder-style; chains.
    #[must_use]
    pub fn with_equipment(
        mut self,
        nr_equip_type_id: i32,
        sector_id: Option<i16>,
        description: Option<String>,
    ) -> Self {
        self.equipment.insert(
            nr_equip_type_id,
            NrEquipmentEntry {
                sector_id,
                description,
            },
        );
        self
    }

    /// Register one `nrhprangebin` row: the bin name for an HP-range-bin
    /// ID (`movesoutput.hpID`). Builder-style; chains.
    #[must_use]
    pub fn with_hp_bin(mut self, hp_id: i16, bin_name: impl Into<String>) -> Self {
        self.hp_bins.insert(hp_id, bin_name.into());
        self
    }

    /// Register one `enginetech` row: the description for an
    /// engine-tech ID. Builder-style; chains.
    #[must_use]
    pub fn with_eng_tech(mut self, eng_tech_id: i16, description: impl Into<String>) -> Self {
        self.eng_tech.insert(eng_tech_id, description.into());
        self
    }

    /// `nrscc.nrEquipTypeID` for an SCC code.
    fn equip_type_of(&self, scc: Option<&str>) -> Option<i32> {
        self.scc.get(scc?)?.nr_equip_type_id
    }

    /// `nrscc.fuelTypeID` for an SCC code.
    fn fuel_type_of(&self, scc: Option<&str>) -> Option<i16> {
        self.scc.get(scc?)?.fuel_type_id
    }

    /// `nrscc.description` for an SCC code.
    fn scc_description(&self, scc: Option<&str>) -> Option<String> {
        self.scc.get(scc?)?.description.clone()
    }

    /// `nrequipmenttype.sectorID` for an equipment-type ID.
    fn sector_of(&self, equip: Option<i32>) -> Option<i16> {
        self.equipment.get(&equip?)?.sector_id
    }

    /// `nrequipmenttype.description` for an equipment-type ID.
    fn equip_description(&self, equip: Option<i32>) -> Option<String> {
        self.equipment.get(&equip?)?.description.clone()
    }

    /// `nrhprangebin.binName` for an HP-range-bin ID.
    fn hp_bin(&self, hp_id: Option<i16>) -> Option<String> {
        self.hp_bins.get(&hp_id?).cloned()
    }

    /// `enginetech` description for an engine-tech ID.
    fn eng_tech_description(&self, eng_tech_id: Option<i16>) -> Option<String> {
        self.eng_tech.get(&eng_tech_id?).cloned()
    }
}

// ---------------------------------------------------------------------------
// SQL SUM accumulator
// ---------------------------------------------------------------------------

/// A running SQL `SUM`: `NULL` inputs are skipped, and a group that
/// never sees a non-`NULL` input sums to `NULL` (`None`) rather than
/// `0.0`.
#[derive(Debug, Clone, Copy, Default)]
struct SqlSum {
    sum: f64,
    seen: bool,
}

impl SqlSum {
    /// Fold one value into the sum. `None` (SQL `NULL`) is skipped.
    fn add(&mut self, value: Option<f64>) {
        if let Some(v) = value {
            self.sum += v;
            self.seen = true;
        }
    }

    /// The SQL `SUM` result: `None` if no non-`NULL` input was folded.
    fn value(self) -> Option<f64> {
        self.seen.then_some(self.sum)
    }
}

// ---------------------------------------------------------------------------
// Run-metadata resolution
// ---------------------------------------------------------------------------

/// `movesrun` columns the post-processing scripts read: the time-unit
/// and mass-unit labels. Indexed by `MOVESRunID`.
struct RunUnits<'a> {
    by_run: HashMap<i16, &'a MovesRunRecord>,
}

impl<'a> RunUnits<'a> {
    fn new(runs: &'a [MovesRunRecord]) -> Self {
        Self {
            by_run: runs.iter().map(|r| (r.moves_run_id, r)).collect(),
        }
    }

    /// `movesrun.timeUnits` for a run.
    fn time_units(&self, run: i16) -> Option<String> {
        self.by_run.get(&run)?.time_units.clone()
    }

    /// `movesrun.massUnits` for a run.
    fn mass_units(&self, run: i16) -> Option<String> {
        self.by_run.get(&run)?.mass_units.clone()
    }

    /// The mass→grams factor for a run — `NULL` if the run is unknown
    /// or its `massUnits` is unrecognized.
    fn mass_factor(&self, run: i16) -> Option<f64> {
        mass_units_to_grams(&self.mass_units(run)?)
    }
}

// ===========================================================================
// Inventory reports
// ===========================================================================

/// Which emissions-inventory summary to compute — one variant per
/// `Inventory_by_*.sql` script. Each rolls `SUM(emissionQuant)` up to
/// a different set of `GROUP BY` keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum InventoryReport {
    /// `Inventory_by_County_and_Pollutant.sql` — keyed by run, time,
    /// state, county, sector, pollutant, process.
    ByCountyAndPollutant,
    /// `Inventory_by_County_FuelType_Pollutant.sql` — adds fuel type
    /// and fuel sub-type to [`ByCountyAndPollutant`](Self::ByCountyAndPollutant).
    ByCountyFuelTypeAndPollutant,
    /// `Inventory_by_EquipmentType_Pollutant.sql` — keyed by equipment
    /// type (via `nrscc`), its sector, fuel type, fuel sub-type,
    /// pollutant, process.
    ByEquipmentTypeAndPollutant,
    /// `Inventory_by_Equipment_Horsepower_Pollutant.sql` — adds the
    /// HP-range bin to [`ByEquipmentTypeAndPollutant`](Self::ByEquipmentTypeAndPollutant).
    ByEquipmentHorsepowerAndPollutant,
    /// `Inventory_by_Sector_Horsepower_Pollutant.sql` — keyed by
    /// sector (via `nrscc`→`nrequipmenttype`), HP-range bin, fuel type,
    /// fuel sub-type, pollutant, process.
    BySectorHorsepowerAndPollutant,
    /// `Inventory_by_Sector_SCC_Pollutant.sql` — keyed by sector, SCC,
    /// fuel type, fuel sub-type, pollutant, process.
    BySectorSccAndPollutant,
}

impl InventoryReport {
    /// The name of the script (and output table) this variant ports.
    #[must_use]
    pub fn script_name(self) -> &'static str {
        match self {
            Self::ByCountyAndPollutant => "Inventory_by_County_and_Pollutant",
            Self::ByCountyFuelTypeAndPollutant => "Inventory_by_County_FuelType_Pollutant",
            Self::ByEquipmentTypeAndPollutant => "Inventory_by_EquipmentType_Pollutant",
            Self::ByEquipmentHorsepowerAndPollutant => {
                "Inventory_by_Equipment_Horsepower_Pollutant"
            }
            Self::BySectorHorsepowerAndPollutant => "Inventory_by_Sector_Horsepower_Pollutant",
            Self::BySectorSccAndPollutant => "Inventory_by_Sector_SCC_Pollutant",
        }
    }
}

/// One row of an [`inventory`] result table.
///
/// The struct is the union of every `Inventory_by_*` output schema:
/// each [`InventoryReport`] populates the subset of dimension fields
/// it groups by and leaves the rest `None`. `time_units` / `mass_units`
/// are carried through from `movesrun` as labels — the inventory
/// scripts, unlike the emission-factor scripts, do not convert to
/// grams.
#[derive(Debug, Clone, PartialEq)]
pub struct InventoryRow {
    /// `MOVESRunID`.
    pub moves_run_id: i16,
    /// `yearID`.
    pub year_id: Option<i16>,
    /// `monthID`.
    pub month_id: Option<i16>,
    /// `dayID`.
    pub day_id: Option<i16>,
    /// `stateID` — every inventory report keys on it; `None` only when
    /// the source row's `stateID` was `NULL`.
    pub state_id: Option<i16>,
    /// `countyID`.
    pub county_id: Option<i32>,
    /// `sectorID` — from the record for the county/SCC reports, from
    /// `nrequipmenttype` for the equipment/sector reports.
    pub sector_id: Option<i16>,
    /// `nrEquipTypeID` — set only for the equipment-keyed reports.
    pub nr_equip_type_id: Option<i32>,
    /// `nrequipmenttype.description` for [`nr_equip_type_id`](Self::nr_equip_type_id).
    pub equip_description: Option<String>,
    /// `SCC` — set only by [`InventoryReport::BySectorSccAndPollutant`].
    pub scc: Option<String>,
    /// `hpID` — set only for the horsepower-keyed reports.
    pub hp_id: Option<i16>,
    /// `nrhprangebin.binName` for [`hp_id`](Self::hp_id).
    pub hp_bin: Option<String>,
    /// `fuelTypeID`.
    pub fuel_type_id: Option<i16>,
    /// `fuelSubTypeID`.
    pub fuel_sub_type_id: Option<i16>,
    /// `pollutantID`.
    pub pollutant_id: Option<i16>,
    /// `processID`.
    pub process_id: Option<i16>,
    /// `SUM(emissionQuant)` — in the run's native `mass_units`.
    pub emission_quant: Option<f64>,
    /// `movesrun.timeUnits`.
    pub time_units: Option<String>,
    /// `movesrun.massUnits`.
    pub mass_units: Option<String>,
}

/// The `GROUP BY` key for an inventory roll-up — the union of every
/// `Inventory_by_*` key set. Dimensions a given report does not group
/// by are held `None` for every row, so they do not split groups.
/// Field order is the deterministic output sort order.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct InvKey {
    run: i16,
    year: Option<i16>,
    month: Option<i16>,
    day: Option<i16>,
    state: Option<i16>,
    county: Option<i32>,
    sector: Option<i16>,
    nr_equip_type: Option<i32>,
    scc: Option<String>,
    hp_id: Option<i16>,
    fuel_type: Option<i16>,
    fuel_sub_type: Option<i16>,
    pollutant: Option<i16>,
    process: Option<i16>,
}

/// Compute one emissions-inventory summary table.
///
/// Ports the six `Inventory_by_*.sql` scripts: each groups
/// `movesoutput` (the [`EmissionRecord`] batch) by the [`report`]'s key
/// set and sums `emissionQuant`. The equipment- and sector-keyed
/// reports take their equipment/sector/fuel-type dimensions from
/// `lookup`; the county/SCC reports read them straight off the record.
///
/// Rows are returned in group-key sort order.
///
/// [`report`]: InventoryReport
/// [`EmissionRecord`]: moves_data::EmissionRecord
#[must_use]
pub fn inventory(
    report: InventoryReport,
    emissions: &[EmissionRecord],
    runs: &[MovesRunRecord],
    lookup: &NrSccLookup,
) -> Vec<InventoryRow> {
    let run_units = RunUnits::new(runs);
    let mut groups: BTreeMap<InvKey, SqlSum> = BTreeMap::new();
    for rec in emissions {
        let key = inv_key(report, rec, lookup);
        groups.entry(key).or_default().add(rec.emission_quant);
    }
    groups
        .into_iter()
        .map(|(key, sum)| inv_row(&key, sum, &run_units, lookup))
        .collect()
}

/// Build the `GROUP BY` key for one emission record under `report`.
fn inv_key(report: InventoryReport, rec: &EmissionRecord, lookup: &NrSccLookup) -> InvKey {
    let scc = rec.scc.as_deref();
    // Common location dimensions — keyed by every inventory report.
    let mut key = InvKey {
        run: rec.moves_run_id,
        year: rec.year_id,
        month: rec.month_id,
        day: rec.day_id,
        state: rec.state_id,
        county: rec.county_id,
        sector: None,
        nr_equip_type: None,
        scc: None,
        hp_id: None,
        fuel_type: None,
        fuel_sub_type: None,
        pollutant: rec.pollutant_id,
        process: rec.process_id,
    };
    match report {
        InventoryReport::ByCountyAndPollutant => {
            key.sector = rec.sector_id;
        }
        InventoryReport::ByCountyFuelTypeAndPollutant => {
            key.sector = rec.sector_id;
            key.fuel_type = rec.fuel_type_id;
            key.fuel_sub_type = rec.fuel_sub_type_id;
        }
        InventoryReport::ByEquipmentTypeAndPollutant => {
            let equip = lookup.equip_type_of(scc);
            key.nr_equip_type = equip;
            key.sector = lookup.sector_of(equip);
            key.fuel_type = lookup.fuel_type_of(scc);
            key.fuel_sub_type = rec.fuel_sub_type_id;
        }
        InventoryReport::ByEquipmentHorsepowerAndPollutant => {
            let equip = lookup.equip_type_of(scc);
            key.nr_equip_type = equip;
            key.sector = lookup.sector_of(equip);
            key.hp_id = rec.hp_id;
            key.fuel_type = lookup.fuel_type_of(scc);
            key.fuel_sub_type = rec.fuel_sub_type_id;
        }
        InventoryReport::BySectorHorsepowerAndPollutant => {
            key.sector = lookup.sector_of(lookup.equip_type_of(scc));
            key.hp_id = rec.hp_id;
            key.fuel_type = lookup.fuel_type_of(scc);
            key.fuel_sub_type = rec.fuel_sub_type_id;
        }
        InventoryReport::BySectorSccAndPollutant => {
            key.sector = rec.sector_id;
            key.scc = rec.scc.clone();
            key.fuel_type = rec.fuel_type_id;
            key.fuel_sub_type = rec.fuel_sub_type_id;
        }
    }
    key
}

/// Materialize one grouped inventory row.
fn inv_row(
    key: &InvKey,
    sum: SqlSum,
    run_units: &RunUnits<'_>,
    lookup: &NrSccLookup,
) -> InventoryRow {
    InventoryRow {
        moves_run_id: key.run,
        year_id: key.year,
        month_id: key.month,
        day_id: key.day,
        state_id: key.state,
        county_id: key.county,
        sector_id: key.sector,
        nr_equip_type_id: key.nr_equip_type,
        equip_description: lookup.equip_description(key.nr_equip_type),
        scc: key.scc.clone(),
        hp_id: key.hp_id,
        hp_bin: lookup.hp_bin(key.hp_id),
        fuel_type_id: key.fuel_type,
        fuel_sub_type_id: key.fuel_sub_type,
        pollutant_id: key.pollutant,
        process_id: key.process,
        emission_quant: sum.value(),
        time_units: run_units.time_units(key.run),
        mass_units: run_units.mass_units(key.run),
    }
}

// ===========================================================================
// Population report
// ===========================================================================

/// One row of [`population_by_sector_and_scc`] — ports the output
/// schema of `Population_by_Sector_and_SCC.sql`.
#[derive(Debug, Clone, PartialEq)]
pub struct PopulationRow {
    /// `MOVESRunID`.
    pub moves_run_id: i16,
    /// `yearID`.
    pub year_id: Option<i16>,
    /// `monthID`.
    pub month_id: Option<i16>,
    /// `dayID`.
    pub day_id: Option<i16>,
    /// `countyID`.
    pub county_id: Option<i32>,
    /// `fuelTypeID`.
    pub fuel_type_id: Option<i16>,
    /// `sectorID`.
    pub sector_id: Option<i16>,
    /// `SCC`.
    pub scc: Option<String>,
    /// `SUM(activity)` over the equipment-population activity type.
    pub population: Option<f64>,
    /// `movesrun.timeUnits`.
    pub time_units: Option<String>,
    /// `movesrun.massUnits`.
    pub mass_units: Option<String>,
}

/// `GROUP BY` key for `Population_by_Sector_and_SCC.sql`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct PopKey {
    run: i16,
    year: Option<i16>,
    month: Option<i16>,
    day: Option<i16>,
    county: Option<i32>,
    fuel_type: Option<i16>,
    sector: Option<i16>,
    scc: Option<String>,
}

/// Compute the equipment-population summary — ports
/// `Population_by_Sector_and_SCC.sql`.
///
/// Filters `movesactivityoutput` (the [`ActivityRecord`] batch) to the
/// equipment-population activity type (`activityTypeID = 6`) and sums
/// `activity` grouped by run, time, county, fuel type, sector, and SCC.
/// Rows are returned in group-key sort order.
///
/// [`ActivityRecord`]: moves_data::ActivityRecord
#[must_use]
pub fn population_by_sector_and_scc(
    activity: &[ActivityRecord],
    runs: &[MovesRunRecord],
) -> Vec<PopulationRow> {
    let run_units = RunUnits::new(runs);
    let mut groups: BTreeMap<PopKey, SqlSum> = BTreeMap::new();
    for rec in activity {
        if rec.activity_type_id != Some(ACTIVITY_POPULATION) {
            continue;
        }
        let key = PopKey {
            run: rec.moves_run_id,
            year: rec.year_id,
            month: rec.month_id,
            day: rec.day_id,
            county: rec.county_id,
            fuel_type: rec.fuel_type_id,
            sector: rec.sector_id,
            scc: rec.scc.clone(),
        };
        groups.entry(key).or_default().add(rec.activity);
    }
    groups
        .into_iter()
        .map(|(key, sum)| PopulationRow {
            moves_run_id: key.run,
            year_id: key.year,
            month_id: key.month,
            day_id: key.day,
            county_id: key.county,
            fuel_type_id: key.fuel_type,
            sector_id: key.sector,
            scc: key.scc,
            population: sum.value(),
            time_units: run_units.time_units(key.run),
            mass_units: run_units.mass_units(key.run),
        })
        .collect()
}

// ===========================================================================
// Emission-factor reports
// ===========================================================================

/// Which emission-factor table to compute — one variant per
/// `EmissionFactors_*.sql` script.
///
/// The variants cross three denominators (operating hour, horsepower-
/// hour, vehicle) with the grouping the factor is broken out by. Not
/// every combination exists: canonical MOVES ships no
/// `per_Vehicle_*_by_*_and_ModelYear` script.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EmissionFactorReport {
    /// `EmissionFactors_per_OperatingHour_by_SCC.sql`.
    PerOperatingHourByScc,
    /// `EmissionFactors_per_OperatingHour_by_SCC_and_ModelYear.sql`.
    PerOperatingHourBySccAndModelYear,
    /// `EmissionFactors_per_OperatingHour_by_Equipment.sql`.
    PerOperatingHourByEquipment,
    /// `EmissionFactors_per_OperatingHour_by_Equipment_and_Horsepower.sql`.
    PerOperatingHourByEquipmentAndHorsepower,
    /// `EmissionFactors_per_hphr_by_SCC.sql`.
    PerHpHrByScc,
    /// `EmissionFactors_per_hphr_by_SCC_and_ModelYear.sql`.
    PerHpHrBySccAndModelYear,
    /// `EmissionFactors_per_hphr_by_Equipment.sql`.
    PerHpHrByEquipment,
    /// `EmissionFactors_per_hphr_by_Equipment_and_Horsepower.sql`.
    PerHpHrByEquipmentAndHorsepower,
    /// `EmissionFactors_per_Vehicle_by_SCC.sql`.
    PerVehicleByScc,
    /// `EmissionFactors_per_Vehicle_by_Equipment.sql`.
    PerVehicleByEquipment,
    /// `EmissionFactors_per_Vehicle_by_Equipment_and_Horsepower.sql`.
    PerVehicleByEquipmentAndHorsepower,
}

/// The activity quantity an emission factor is expressed per.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Denominator {
    /// `SUM(activity)` over source (operating) hours — `activityTypeID = 2`.
    OperatingHours,
    /// `SUM(avgHorsepower × sourceHours × loadFactor)` — activity types
    /// 9, 2, and 12 multiplied per finest-grain row, then summed.
    HpHours,
    /// `SUM(activity)` over equipment population — `activityTypeID = 6`.
    Vehicles,
}

/// The set of `GROUP BY` dimensions an emission-factor table is broken
/// out by, beyond the always-present run / time / state / county keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EfGrouping {
    /// Keyed by SCC.
    Scc,
    /// Keyed by SCC and model year.
    SccModelYear,
    /// Keyed by SCC, HP bin, model year, and engine tech — the finer
    /// `per_hphr_by_SCC_and_ModelYear` key set.
    SccHorsepowerModelYearEngTech,
    /// Keyed by equipment type and (`nrscc`) fuel type.
    Equipment,
    /// Keyed by equipment type, HP bin, and fuel type.
    EquipmentHorsepower,
}

impl EmissionFactorReport {
    /// The name of the script (and output table) this variant ports.
    #[must_use]
    pub fn script_name(self) -> &'static str {
        match self {
            Self::PerOperatingHourByScc => "EmissionFactors_per_OperatingHour_by_SCC",
            Self::PerOperatingHourBySccAndModelYear => {
                "EmissionFactors_per_OperatingHour_by_SCC_and_ModelYear"
            }
            Self::PerOperatingHourByEquipment => "EmissionFactors_per_OperatingHour_by_Equipment",
            Self::PerOperatingHourByEquipmentAndHorsepower => {
                "EmissionFactors_per_OperatingHour_by_Equipment_and_Horsepower"
            }
            Self::PerHpHrByScc => "EmissionFactors_per_hphr_by_SCC",
            Self::PerHpHrBySccAndModelYear => "EmissionFactors_per_hphr_by_SCC_and_ModelYear",
            Self::PerHpHrByEquipment => "EmissionFactors_per_hphr_by_Equipment",
            Self::PerHpHrByEquipmentAndHorsepower => {
                "EmissionFactors_per_hphr_by_Equipment_and_Horsepower"
            }
            Self::PerVehicleByScc => "EmissionFactors_per_Vehicle_by_SCC",
            Self::PerVehicleByEquipment => "EmissionFactors_per_Vehicle_by_Equipment",
            Self::PerVehicleByEquipmentAndHorsepower => {
                "EmissionFactors_per_Vehicle_by_Equipment_and_Horsepower"
            }
        }
    }

    /// The activity quantity this report's factor is expressed per.
    fn denominator(self) -> Denominator {
        match self {
            Self::PerOperatingHourByScc
            | Self::PerOperatingHourBySccAndModelYear
            | Self::PerOperatingHourByEquipment
            | Self::PerOperatingHourByEquipmentAndHorsepower => Denominator::OperatingHours,
            Self::PerHpHrByScc
            | Self::PerHpHrBySccAndModelYear
            | Self::PerHpHrByEquipment
            | Self::PerHpHrByEquipmentAndHorsepower => Denominator::HpHours,
            Self::PerVehicleByScc
            | Self::PerVehicleByEquipment
            | Self::PerVehicleByEquipmentAndHorsepower => Denominator::Vehicles,
        }
    }

    /// The `GROUP BY` shape this report rolls up to.
    fn grouping(self) -> EfGrouping {
        match self {
            Self::PerOperatingHourByScc | Self::PerHpHrByScc | Self::PerVehicleByScc => {
                EfGrouping::Scc
            }
            Self::PerOperatingHourBySccAndModelYear => EfGrouping::SccModelYear,
            Self::PerHpHrBySccAndModelYear => EfGrouping::SccHorsepowerModelYearEngTech,
            Self::PerOperatingHourByEquipment
            | Self::PerHpHrByEquipment
            | Self::PerVehicleByEquipment => EfGrouping::Equipment,
            Self::PerOperatingHourByEquipmentAndHorsepower
            | Self::PerHpHrByEquipmentAndHorsepower
            | Self::PerVehicleByEquipmentAndHorsepower => EfGrouping::EquipmentHorsepower,
        }
    }

    /// The `emissionRateUnits` label string. The per-vehicle scripts
    /// build it as `concat('g/vehicle per ', @timeUnits)`, where
    /// `@timeUnits` is the first run's `timeUnits`.
    fn rate_units(self, time_units: Option<&str>) -> String {
        match self.denominator() {
            Denominator::OperatingHours => "g/hr".to_string(),
            Denominator::HpHours => "g/hp-hr".to_string(),
            Denominator::Vehicles => {
                format!("g/vehicle per {}", time_units.unwrap_or(""))
            }
        }
    }
}

/// One row of an [`emission_factors`] result table.
///
/// The union of every `EmissionFactors_*` output schema: a report
/// populates the dimension fields its grouping keys by and the
/// decorations its `LEFT JOIN`s supply, leaving the rest `None`.
#[derive(Debug, Clone, PartialEq)]
pub struct EmissionFactorRow {
    /// `MOVESRunID`.
    pub moves_run_id: i16,
    /// `yearID`.
    pub year_id: Option<i16>,
    /// `monthID`.
    pub month_id: Option<i16>,
    /// `dayID`.
    pub day_id: Option<i16>,
    /// `stateID`.
    pub state_id: Option<i16>,
    /// `countyID`.
    pub county_id: Option<i32>,
    /// `SCC` — set for the SCC-grouped reports.
    pub scc: Option<String>,
    /// `nrscc.description` for [`scc`](Self::scc).
    pub scc_description: Option<String>,
    /// `nrEquipTypeID` — set for the equipment-grouped reports.
    pub nr_equip_type_id: Option<i32>,
    /// `nrequipmenttype.description` for [`nr_equip_type_id`](Self::nr_equip_type_id).
    pub equip_description: Option<String>,
    /// `fuelTypeID` — `nrscc.fuelTypeID` of the SCC: a grouping key for
    /// the equipment reports, a decoration for the SCC reports.
    pub fuel_type_id: Option<i16>,
    /// `hpID` — set for the horsepower-keyed reports.
    pub hp_id: Option<i16>,
    /// `nrhprangebin.binName` for [`hp_id`](Self::hp_id).
    pub hp_bin: Option<String>,
    /// `modelYearID` — set for the model-year-keyed reports.
    pub model_year_id: Option<i16>,
    /// `engTechID` — set by `per_hphr_by_SCC_and_ModelYear`.
    pub eng_tech_id: Option<i16>,
    /// `enginetech` description for [`eng_tech_id`](Self::eng_tech_id).
    pub eng_tech_description: Option<String>,
    /// `pollutantID`.
    pub pollutant_id: Option<i16>,
    /// `processID`.
    pub process_id: Option<i16>,
    /// `units.factor × SUM(emissionQuant)` — emissions in grams.
    pub emission_quant: Option<f64>,
    /// The factor's denominator: `SUM` of operating hours,
    /// horsepower-hours, or vehicle population, per the
    /// [`EmissionFactorReport`] variant.
    pub denominator: Option<f64>,
    /// `emissionQuant / denominator`, or `None` when the denominator
    /// is `NULL` or zero.
    pub emission_rate: Option<f64>,
    /// `emissionRateUnits` — `g/hr`, `g/hp-hr`, or `g/vehicle per …`.
    pub emission_rate_units: String,
}

/// The finest-grain dimension tuple shared by `movesoutput` and
/// `movesactivityoutput` rows — the join key of the horsepower-hour
/// pre-pass and the source of every emission-factor `GROUP BY` key.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct RecordDims {
    run: i16,
    year: Option<i16>,
    month: Option<i16>,
    day: Option<i16>,
    state: Option<i16>,
    county: Option<i32>,
    scc: Option<String>,
    model_year: Option<i16>,
    eng_tech: Option<i16>,
    hp_id: Option<i16>,
}

impl RecordDims {
    fn from_emission(rec: &EmissionRecord) -> Self {
        Self {
            run: rec.moves_run_id,
            year: rec.year_id,
            month: rec.month_id,
            day: rec.day_id,
            state: rec.state_id,
            county: rec.county_id,
            scc: rec.scc.clone(),
            model_year: rec.model_year_id,
            eng_tech: rec.eng_tech_id,
            hp_id: rec.hp_id,
        }
    }

    fn from_activity(rec: &ActivityRecord) -> Self {
        Self {
            run: rec.moves_run_id,
            year: rec.year_id,
            month: rec.month_id,
            day: rec.day_id,
            state: rec.state_id,
            county: rec.county_id,
            scc: rec.scc.clone(),
            model_year: rec.model_year_id,
            eng_tech: rec.eng_tech_id,
            hp_id: rec.hp_id,
        }
    }
}

/// The `GROUP BY` key for an emission-factor roll-up. The numerator is
/// grouped by this key plus pollutant and process; the denominator is
/// grouped by this key alone, and the two are inner-joined on it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct EfKey {
    run: i16,
    year: Option<i16>,
    month: Option<i16>,
    day: Option<i16>,
    state: Option<i16>,
    county: Option<i32>,
    scc: Option<String>,
    nr_equip_type: Option<i32>,
    fuel_type: Option<i16>,
    hp_id: Option<i16>,
    model_year: Option<i16>,
    eng_tech: Option<i16>,
}

/// Build the emission-factor `GROUP BY` key for one finest-grain row.
fn ef_key(dims: &RecordDims, grouping: EfGrouping, lookup: &NrSccLookup) -> EfKey {
    let scc = dims.scc.as_deref();
    let mut key = EfKey {
        run: dims.run,
        year: dims.year,
        month: dims.month,
        day: dims.day,
        state: dims.state,
        county: dims.county,
        scc: None,
        nr_equip_type: None,
        fuel_type: None,
        hp_id: None,
        model_year: None,
        eng_tech: None,
    };
    match grouping {
        EfGrouping::Scc => {
            key.scc = dims.scc.clone();
        }
        EfGrouping::SccModelYear => {
            key.scc = dims.scc.clone();
            key.model_year = dims.model_year;
        }
        EfGrouping::SccHorsepowerModelYearEngTech => {
            key.scc = dims.scc.clone();
            key.hp_id = dims.hp_id;
            key.model_year = dims.model_year;
            key.eng_tech = dims.eng_tech;
        }
        EfGrouping::Equipment => {
            key.nr_equip_type = lookup.equip_type_of(scc);
            key.fuel_type = lookup.fuel_type_of(scc);
        }
        EfGrouping::EquipmentHorsepower => {
            key.nr_equip_type = lookup.equip_type_of(scc);
            key.fuel_type = lookup.fuel_type_of(scc);
            key.hp_id = dims.hp_id;
        }
    }
    key
}

/// The finest-grain horsepower-hour pre-pass: ports the
/// `sourceHours` / `horsepower` / `loadfactor` → `hphr` chain.
///
/// `hpHours = avgHorsepower × sourceHours × loadFactor`, computed per
/// `(run, time, state, county, SCC, modelYear, engTech, hpID)` row.
/// The SQL builds `hphr` as `sourceHours INNER JOIN horsepower`, then
/// fills `hpHours` only where a `loadFactor` row also matches — so a
/// finest-grain key contributes a value only when all three activity
/// types are present.
///
/// A key seen more than once for one activity type has its values
/// summed; for the source-hours type this matches the SQL (the join
/// fans out and the later `SUM` re-collapses), and the unified
/// activity schema makes the key unique per type in practice.
fn compute_hp_hours(activity: &[ActivityRecord]) -> BTreeMap<RecordDims, f64> {
    let mut source_hours: BTreeMap<RecordDims, f64> = BTreeMap::new();
    let mut avg_hp: BTreeMap<RecordDims, f64> = BTreeMap::new();
    let mut load_factor: BTreeMap<RecordDims, f64> = BTreeMap::new();
    for rec in activity {
        let bucket = match rec.activity_type_id {
            Some(ACTIVITY_SOURCE_HOURS) => &mut source_hours,
            Some(ACTIVITY_AVERAGE_HORSEPOWER) => &mut avg_hp,
            Some(ACTIVITY_LOAD_FACTOR) => &mut load_factor,
            _ => continue,
        };
        if let Some(value) = rec.activity {
            *bucket.entry(RecordDims::from_activity(rec)).or_default() += value;
        }
    }
    // hphr exists where sourceHours and horsepower both match;
    // hpHours is filled only where loadFactor also matches.
    source_hours
        .into_iter()
        .filter_map(|(dims, hours)| {
            let hp = avg_hp.get(&dims)?;
            let lf = load_factor.get(&dims)?;
            Some((dims, hp * hours * lf))
        })
        .collect()
}

/// Roll the denominator activity up to the report's `GROUP BY` key.
fn ef_denominator(
    report: EmissionFactorReport,
    activity: &[ActivityRecord],
    lookup: &NrSccLookup,
) -> BTreeMap<EfKey, SqlSum> {
    let grouping = report.grouping();
    let mut groups: BTreeMap<EfKey, SqlSum> = BTreeMap::new();
    match report.denominator() {
        Denominator::OperatingHours | Denominator::Vehicles => {
            let want = if report.denominator() == Denominator::OperatingHours {
                ACTIVITY_SOURCE_HOURS
            } else {
                ACTIVITY_POPULATION
            };
            for rec in activity {
                if rec.activity_type_id != Some(want) {
                    continue;
                }
                let key = ef_key(&RecordDims::from_activity(rec), grouping, lookup);
                groups.entry(key).or_default().add(rec.activity);
            }
        }
        Denominator::HpHours => {
            for (dims, hp_hours) in compute_hp_hours(activity) {
                let key = ef_key(&dims, grouping, lookup);
                groups.entry(key).or_default().add(Some(hp_hours));
            }
        }
    }
    groups
}

/// Compute one emission-factor table.
///
/// Ports the eleven `EmissionFactors_*.sql` scripts. The numerator is
/// `units.factor × SUM(emissionQuant)` — the [`EmissionRecord`] batch
/// grouped by the report's key plus pollutant and process, with
/// `emissionQuant` converted from the run's `massUnits` to grams. The
/// denominator is the activity quantity for the report's denominator
/// kind, grouped by the report's key alone. Numerator and denominator
/// are inner-joined on the key — a numerator group with no matching
/// denominator is dropped — and `emissionRate` is
/// `emissionQuant / denominator`, or `None` when the denominator is
/// `NULL` or zero.
///
/// Rows are returned in group-key-then-pollutant-then-process order.
///
/// [`EmissionRecord`]: moves_data::EmissionRecord
#[must_use]
pub fn emission_factors(
    report: EmissionFactorReport,
    emissions: &[EmissionRecord],
    activity: &[ActivityRecord],
    runs: &[MovesRunRecord],
    lookup: &NrSccLookup,
) -> Vec<EmissionFactorRow> {
    let run_units = RunUnits::new(runs);
    let grouping = report.grouping();

    // Numerator: SUM(emissionQuant) grouped by key + pollutant + process.
    let mut numerator: BTreeMap<(EfKey, Option<i16>, Option<i16>), SqlSum> = BTreeMap::new();
    for rec in emissions {
        let key = ef_key(&RecordDims::from_emission(rec), grouping, lookup);
        numerator
            .entry((key, rec.pollutant_id, rec.process_id))
            .or_default()
            .add(rec.emission_quant);
    }

    // Denominator: the activity quantity grouped by key alone.
    let denominator = ef_denominator(report, activity, lookup);

    // `@timeUnits` — the per-vehicle label reads `movesrun.timeUnits`
    // of the first run, matching the script's `select … limit 1`.
    let time_units = runs.first().and_then(|r| r.time_units.clone());
    let rate_units = report.rate_units(time_units.as_deref());

    let mut rows = Vec::new();
    for ((key, pollutant, process), num_sum) in numerator {
        // INNER JOIN temp1 ⋈ temp2: drop a numerator group with no
        // matching denominator group.
        let Some(denom_sum) = denominator.get(&key) else {
            continue;
        };
        let denominator_value = denom_sum.value();
        let factor = run_units.mass_factor(key.run);
        // units.factor × SUM(emissionQuant): NULL if either is NULL.
        let emission_quant = match (factor, num_sum.value()) {
            (Some(f), Some(s)) => Some(f * s),
            _ => None,
        };
        // IF(denominator != 0, emissionQuant / denominator, NULL).
        let emission_rate = match denominator_value {
            Some(d) if d != 0.0 => emission_quant.map(|e| e / d),
            _ => None,
        };
        rows.push(ef_row(
            &key,
            pollutant,
            process,
            emission_quant,
            denominator_value,
            emission_rate,
            rate_units.clone(),
            lookup,
        ));
    }
    rows
}

/// Materialize one grouped emission-factor row.
#[allow(clippy::too_many_arguments)]
fn ef_row(
    key: &EfKey,
    pollutant: Option<i16>,
    process: Option<i16>,
    emission_quant: Option<f64>,
    denominator: Option<f64>,
    emission_rate: Option<f64>,
    emission_rate_units: String,
    lookup: &NrSccLookup,
) -> EmissionFactorRow {
    let scc = key.scc.as_deref();
    EmissionFactorRow {
        moves_run_id: key.run,
        year_id: key.year,
        month_id: key.month,
        day_id: key.day,
        state_id: key.state,
        county_id: key.county,
        scc: key.scc.clone(),
        scc_description: lookup.scc_description(scc),
        nr_equip_type_id: key.nr_equip_type,
        equip_description: lookup.equip_description(key.nr_equip_type),
        // nrscc.fuelTypeID: a grouping key for the equipment reports,
        // a decoration derived from the SCC for the SCC reports.
        fuel_type_id: key.fuel_type.or_else(|| lookup.fuel_type_of(scc)),
        hp_id: key.hp_id,
        hp_bin: lookup.hp_bin(key.hp_id),
        model_year_id: key.model_year,
        eng_tech_id: key.eng_tech,
        eng_tech_description: lookup.eng_tech_description(key.eng_tech),
        pollutant_id: pollutant,
        process_id: process,
        emission_quant,
        denominator,
        emission_rate,
        emission_rate_units,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- record factories ----------------------------------------------

    /// An emission record with all dimensions at fixed defaults; the
    /// caller overrides whatever a test cares about.
    fn emission() -> EmissionRecord {
        EmissionRecord {
            moves_run_id: 1,
            iteration_id: Some(1),
            year_id: Some(2020),
            month_id: Some(7),
            day_id: Some(5),
            hour_id: None,
            state_id: Some(17),
            county_id: Some(17031),
            zone_id: None,
            link_id: None,
            pollutant_id: Some(2),
            process_id: Some(1),
            source_type_id: None,
            reg_class_id: None,
            fuel_type_id: Some(1),
            fuel_sub_type_id: Some(10),
            model_year_id: Some(2018),
            road_type_id: None,
            scc: Some("2270001060".to_string()),
            eng_tech_id: Some(1),
            sector_id: Some(3),
            hp_id: Some(40),
            emission_quant: Some(0.0),
            emission_rate: None,
            run_hash: "h".to_string(),
        }
    }

    /// An activity record with all dimensions at fixed defaults.
    fn activity(activity_type_id: i16, value: Option<f64>) -> ActivityRecord {
        ActivityRecord {
            moves_run_id: 1,
            iteration_id: Some(1),
            year_id: Some(2020),
            month_id: Some(7),
            day_id: Some(5),
            hour_id: None,
            state_id: Some(17),
            county_id: Some(17031),
            zone_id: None,
            link_id: None,
            source_type_id: None,
            reg_class_id: None,
            fuel_type_id: Some(1),
            fuel_sub_type_id: Some(10),
            model_year_id: Some(2018),
            road_type_id: None,
            scc: Some("2270001060".to_string()),
            eng_tech_id: Some(1),
            sector_id: Some(3),
            hp_id: Some(40),
            activity_type_id: Some(activity_type_id),
            activity: value,
            run_hash: "h".to_string(),
        }
    }

    fn run(mass_units: &str, time_units: &str) -> MovesRunRecord {
        MovesRunRecord {
            moves_run_id: 1,
            output_time_period: None,
            time_units: Some(time_units.to_string()),
            distance_units: None,
            mass_units: Some(mass_units.to_string()),
            energy_units: None,
            run_spec_file_name: None,
            run_spec_description: None,
            run_spec_file_date_time: None,
            run_date_time: None,
            scale: None,
            minutes_duration: None,
            default_database_used: None,
            master_version: None,
            master_computer_id: None,
            master_id_number: None,
            domain: None,
            domain_county_id: None,
            domain_county_name: None,
            domain_database_server: None,
            domain_database_name: None,
            expected_done_files: None,
            retrieved_done_files: None,
            models: None,
            run_hash: "h".to_string(),
            calculator_version: "test".to_string(),
        }
    }

    // ---- mass_units_to_grams --------------------------------------------

    #[test]
    fn mass_units_table_matches_the_sql_units_rows() {
        assert_eq!(mass_units_to_grams("ton"), Some(907_185.0));
        assert_eq!(mass_units_to_grams("lb"), Some(453.592));
        assert_eq!(mass_units_to_grams("kg"), Some(1000.0));
        assert_eq!(mass_units_to_grams("g"), Some(1.0));
    }

    #[test]
    fn mass_units_lookup_is_case_insensitive_and_misses_to_none() {
        // MySQL's default collation makes the join case-insensitive.
        assert_eq!(mass_units_to_grams("KG"), Some(1000.0));
        assert_eq!(mass_units_to_grams("Ton"), Some(907_185.0));
        // An unrecognized unit is the LEFT JOIN miss → NULL factor.
        assert_eq!(mass_units_to_grams("stone"), None);
    }

    // ---- SqlSum ----------------------------------------------------------

    #[test]
    fn sql_sum_skips_nulls_and_an_all_null_group_is_null() {
        let mut s = SqlSum::default();
        s.add(None);
        assert_eq!(s.value(), None, "all-NULL group sums to NULL, not 0.0");
        s.add(Some(2.0));
        s.add(None);
        s.add(Some(3.0));
        assert_eq!(s.value(), Some(5.0));
    }

    // ---- inventory -------------------------------------------------------

    #[test]
    fn inventory_by_county_sums_emission_quant_per_group() {
        // Two rows in one (county, sector, pollutant, process) group,
        // one in another pollutant.
        let mut a = emission();
        a.emission_quant = Some(10.0);
        let mut b = emission();
        b.emission_quant = Some(2.5);
        let mut c = emission();
        c.pollutant_id = Some(3);
        c.emission_quant = Some(7.0);

        let rows = inventory(
            InventoryReport::ByCountyAndPollutant,
            &[a, b, c],
            &[run("g", "hours")],
            &NrSccLookup::new(),
        );
        assert_eq!(rows.len(), 2, "one row per distinct pollutant");
        assert_eq!(rows[0].pollutant_id, Some(2));
        assert_eq!(rows[0].emission_quant, Some(12.5));
        assert_eq!(rows[0].county_id, Some(17031));
        assert_eq!(rows[0].sector_id, Some(3));
        // Inventory carries units as labels and does NOT convert.
        assert_eq!(rows[0].mass_units.as_deref(), Some("g"));
        assert_eq!(rows[0].time_units.as_deref(), Some("hours"));
        // SCC / equipment dimensions are absent from this report.
        assert_eq!(rows[0].scc, None);
        assert_eq!(rows[0].nr_equip_type_id, None);
        assert_eq!(rows[1].pollutant_id, Some(3));
        assert_eq!(rows[1].emission_quant, Some(7.0));
    }

    #[test]
    fn inventory_by_county_does_not_convert_tons_to_grams() {
        // The inventory scripts have no `units` join — a ton-unit run
        // still reports the raw summed quantity.
        let mut a = emission();
        a.emission_quant = Some(3.0);
        let rows = inventory(
            InventoryReport::ByCountyAndPollutant,
            &[a],
            &[run("ton", "hours")],
            &NrSccLookup::new(),
        );
        assert_eq!(rows[0].emission_quant, Some(3.0));
        assert_eq!(rows[0].mass_units.as_deref(), Some("ton"));
    }

    #[test]
    fn inventory_by_equipment_type_uses_the_reference_lookup() {
        // SCC 2270001060 → equipment type 7 → sector 11; fuel type 4.
        let lookup = NrSccLookup::new()
            .with_scc(
                "2270001060",
                Some(7),
                Some(4),
                Some("Excavators".to_string()),
            )
            .with_equipment(7, Some(11), Some("Excavators".to_string()));
        let mut a = emission();
        a.emission_quant = Some(8.0);
        let mut b = emission();
        b.emission_quant = Some(1.0);

        let rows = inventory(
            InventoryReport::ByEquipmentTypeAndPollutant,
            &[a, b],
            &[run("g", "hours")],
            &lookup,
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].nr_equip_type_id, Some(7));
        assert_eq!(rows[0].equip_description.as_deref(), Some("Excavators"));
        assert_eq!(rows[0].sector_id, Some(11), "sector from nrequipmenttype");
        assert_eq!(rows[0].fuel_type_id, Some(4), "fuel type from nrscc");
        assert_eq!(rows[0].emission_quant, Some(9.0));
    }

    #[test]
    fn inventory_equipment_lookup_miss_collapses_derived_columns_to_none() {
        // Empty lookup → the SCC→equipment LEFT JOIN misses; the
        // derived columns are NULL and every row collapses into one
        // all-NULL-equipment group.
        let mut a = emission();
        a.emission_quant = Some(4.0);
        let rows = inventory(
            InventoryReport::ByEquipmentTypeAndPollutant,
            &[a],
            &[run("g", "hours")],
            &NrSccLookup::new(),
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].nr_equip_type_id, None);
        assert_eq!(rows[0].sector_id, None);
        assert_eq!(rows[0].equip_description, None);
        assert_eq!(rows[0].emission_quant, Some(4.0));
    }

    #[test]
    fn inventory_by_sector_scc_keys_on_record_columns() {
        let mut a = emission();
        a.scc = Some("ZZZ".to_string());
        a.emission_quant = Some(1.0);
        let mut b = emission();
        b.scc = Some("AAA".to_string());
        b.emission_quant = Some(2.0);

        let rows = inventory(
            InventoryReport::BySectorSccAndPollutant,
            &[a, b],
            &[run("g", "hours")],
            &NrSccLookup::new(),
        );
        assert_eq!(rows.len(), 2);
        // BTreeMap key order: "AAA" sorts before "ZZZ".
        assert_eq!(rows[0].scc.as_deref(), Some("AAA"));
        assert_eq!(rows[0].fuel_type_id, Some(1));
        assert_eq!(rows[0].fuel_sub_type_id, Some(10));
        assert_eq!(rows[1].scc.as_deref(), Some("ZZZ"));
    }

    // ---- population ------------------------------------------------------

    #[test]
    fn population_sums_only_the_population_activity_type() {
        // activityTypeID 6 is population; 2 (source hours) is ignored.
        let pop_a = activity(ACTIVITY_POPULATION, Some(100.0));
        let pop_b = activity(ACTIVITY_POPULATION, Some(40.0));
        let hours = activity(ACTIVITY_SOURCE_HOURS, Some(9999.0));

        let rows = population_by_sector_and_scc(&[pop_a, pop_b, hours], &[run("g", "days")]);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].population, Some(140.0));
        assert_eq!(rows[0].sector_id, Some(3));
        assert_eq!(rows[0].scc.as_deref(), Some("2270001060"));
        assert_eq!(rows[0].time_units.as_deref(), Some("days"));
    }

    #[test]
    fn population_empty_input_yields_no_rows() {
        assert!(population_by_sector_and_scc(&[], &[]).is_empty());
    }

    // ---- emission factors: operating hour -------------------------------

    #[test]
    fn ef_per_operating_hour_by_scc_divides_grams_by_hours() {
        // 5 g of emissions over 2 operating hours → 2.5 g/hr.
        let mut e = emission();
        e.emission_quant = Some(5.0);
        let hours = activity(ACTIVITY_SOURCE_HOURS, Some(2.0));

        let rows = emission_factors(
            EmissionFactorReport::PerOperatingHourByScc,
            &[e],
            &[hours],
            &[run("g", "hours")],
            &NrSccLookup::new(),
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].emission_quant, Some(5.0));
        assert_eq!(rows[0].denominator, Some(2.0));
        assert_eq!(rows[0].emission_rate, Some(2.5));
        assert_eq!(rows[0].emission_rate_units, "g/hr");
        assert_eq!(rows[0].scc.as_deref(), Some("2270001060"));
    }

    #[test]
    fn ef_converts_emission_quant_from_tons_to_grams() {
        // A ton-unit run: emissionQuant is scaled by 907185 before the
        // rate is taken.
        let mut e = emission();
        e.emission_quant = Some(1.0);
        let hours = activity(ACTIVITY_SOURCE_HOURS, Some(1.0));

        let rows = emission_factors(
            EmissionFactorReport::PerOperatingHourByScc,
            &[e],
            &[hours],
            &[run("ton", "hours")],
            &NrSccLookup::new(),
        );
        assert_eq!(rows[0].emission_quant, Some(907_185.0));
        assert_eq!(rows[0].emission_rate, Some(907_185.0));
    }

    #[test]
    fn ef_unknown_mass_units_null_the_converted_quantity() {
        // An unrecognized massUnits is the `units` LEFT JOIN miss:
        // `units.factor` is NULL, so `factor * SUM(...)` is NULL.
        let mut e = emission();
        e.emission_quant = Some(5.0);
        let hours = activity(ACTIVITY_SOURCE_HOURS, Some(2.0));

        let rows = emission_factors(
            EmissionFactorReport::PerOperatingHourByScc,
            &[e],
            &[hours],
            &[run("furlong", "hours")],
            &NrSccLookup::new(),
        );
        assert_eq!(rows[0].emission_quant, None);
        assert_eq!(rows[0].emission_rate, None, "NULL / hours is NULL");
        assert_eq!(rows[0].denominator, Some(2.0));
    }

    #[test]
    fn ef_zero_denominator_yields_a_null_rate() {
        // IF(hours != 0, …, NULL): a zero denominator → NULL rate, but
        // the row is still emitted with its emissionQuant.
        let mut e = emission();
        e.emission_quant = Some(5.0);
        let hours = activity(ACTIVITY_SOURCE_HOURS, Some(0.0));

        let rows = emission_factors(
            EmissionFactorReport::PerOperatingHourByScc,
            &[e],
            &[hours],
            &[run("g", "hours")],
            &NrSccLookup::new(),
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].emission_quant, Some(5.0));
        assert_eq!(rows[0].denominator, Some(0.0));
        assert_eq!(rows[0].emission_rate, None);
    }

    #[test]
    fn ef_numerator_without_a_denominator_group_is_dropped() {
        // INNER JOIN temp1 ⋈ temp2: an emissions group with no
        // matching activity group produces no output row.
        let mut e = emission();
        e.emission_quant = Some(5.0);
        // No activity at all.
        let rows = emission_factors(
            EmissionFactorReport::PerOperatingHourByScc,
            &[e],
            &[],
            &[run("g", "hours")],
            &NrSccLookup::new(),
        );
        assert!(rows.is_empty());
    }

    #[test]
    fn ef_splits_the_numerator_by_pollutant_and_process() {
        // One SCC group, two pollutants → two rows sharing the
        // denominator; process also splits the numerator.
        let mut e1 = emission();
        e1.pollutant_id = Some(2);
        e1.emission_quant = Some(6.0);
        let mut e2 = emission();
        e2.pollutant_id = Some(3);
        e2.emission_quant = Some(9.0);
        let hours = activity(ACTIVITY_SOURCE_HOURS, Some(3.0));

        let rows = emission_factors(
            EmissionFactorReport::PerOperatingHourByScc,
            &[e1, e2],
            &[hours],
            &[run("g", "hours")],
            &NrSccLookup::new(),
        );
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].pollutant_id, Some(2));
        assert_eq!(rows[0].emission_rate, Some(2.0));
        assert_eq!(rows[1].pollutant_id, Some(3));
        assert_eq!(rows[1].emission_rate, Some(3.0));
    }

    // ---- emission factors: horsepower-hour ------------------------------

    #[test]
    fn ef_per_hphr_multiplies_horsepower_hours_and_load_factor() {
        // hpHours = avgHorsepower(50) × sourceHours(4) × loadFactor(0.5)
        //         = 100. 200 g / 100 hp-hr → 2 g/hp-hr.
        let mut e = emission();
        e.emission_quant = Some(200.0);
        let source = activity(ACTIVITY_SOURCE_HOURS, Some(4.0));
        let hp = activity(ACTIVITY_AVERAGE_HORSEPOWER, Some(50.0));
        let lf = activity(ACTIVITY_LOAD_FACTOR, Some(0.5));

        let rows = emission_factors(
            EmissionFactorReport::PerHpHrByScc,
            &[e],
            &[source, hp, lf],
            &[run("g", "hours")],
            &NrSccLookup::new(),
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].denominator, Some(100.0));
        assert_eq!(rows[0].emission_rate, Some(2.0));
        assert_eq!(rows[0].emission_rate_units, "g/hp-hr");
    }

    #[test]
    fn ef_per_hphr_drops_a_finest_grain_key_missing_load_factor() {
        // sourceHours and horsepower match but no loadFactor row →
        // hphr.hpHours stays NULL → no denominator group → INNER JOIN
        // drops the numerator row.
        let mut e = emission();
        e.emission_quant = Some(50.0);
        let source = activity(ACTIVITY_SOURCE_HOURS, Some(4.0));
        let hp = activity(ACTIVITY_AVERAGE_HORSEPOWER, Some(50.0));

        let rows = emission_factors(
            EmissionFactorReport::PerHpHrByScc,
            &[e],
            &[source, hp],
            &[run("g", "hours")],
            &NrSccLookup::new(),
        );
        assert!(rows.is_empty());
    }

    // ---- emission factors: per vehicle ----------------------------------

    #[test]
    fn ef_per_vehicle_divides_by_population_and_labels_with_time_units() {
        // 30 g over a population of 6 → 5 g/vehicle; the label embeds
        // the run's timeUnits.
        let mut e = emission();
        e.emission_quant = Some(30.0);
        let pop = activity(ACTIVITY_POPULATION, Some(6.0));

        let rows = emission_factors(
            EmissionFactorReport::PerVehicleByScc,
            &[e],
            &[pop],
            &[run("g", "days")],
            &NrSccLookup::new(),
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].denominator, Some(6.0));
        assert_eq!(rows[0].emission_rate, Some(5.0));
        assert_eq!(rows[0].emission_rate_units, "g/vehicle per days");
    }

    // ---- emission factors: equipment grouping ---------------------------

    #[test]
    fn ef_by_equipment_groups_two_sccs_into_one_equipment_type() {
        // Two SCCs both map to equipment type 7 / fuel type 4 — their
        // emissions and hours collapse into one equipment group.
        let lookup = NrSccLookup::new()
            .with_scc("A", Some(7), Some(4), Some("Loaders".to_string()))
            .with_scc("B", Some(7), Some(4), Some("Loaders".to_string()))
            .with_equipment(7, Some(11), Some("Loaders".to_string()));
        let mut e1 = emission();
        e1.scc = Some("A".to_string());
        e1.emission_quant = Some(6.0);
        let mut e2 = emission();
        e2.scc = Some("B".to_string());
        e2.emission_quant = Some(4.0);
        let mut h1 = activity(ACTIVITY_SOURCE_HOURS, Some(3.0));
        h1.scc = Some("A".to_string());
        let mut h2 = activity(ACTIVITY_SOURCE_HOURS, Some(2.0));
        h2.scc = Some("B".to_string());

        let rows = emission_factors(
            EmissionFactorReport::PerOperatingHourByEquipment,
            &[e1, e2],
            &[h1, h2],
            &[run("g", "hours")],
            &lookup,
        );
        assert_eq!(rows.len(), 1, "two SCCs → one equipment-type group");
        assert_eq!(rows[0].nr_equip_type_id, Some(7));
        assert_eq!(rows[0].equip_description.as_deref(), Some("Loaders"));
        assert_eq!(rows[0].fuel_type_id, Some(4));
        assert_eq!(rows[0].emission_quant, Some(10.0));
        assert_eq!(rows[0].denominator, Some(5.0));
        assert_eq!(rows[0].emission_rate, Some(2.0));
        assert_eq!(rows[0].scc, None, "equipment reports drop SCC");
    }

    // ---- determinism -----------------------------------------------------

    #[test]
    fn emission_factor_output_order_is_independent_of_input_order() {
        let mk = |scc: &str, q: f64| {
            let mut e = emission();
            e.scc = Some(scc.to_string());
            e.emission_quant = Some(q);
            e
        };
        let mk_h = |scc: &str, h: f64| {
            let mut a = activity(ACTIVITY_SOURCE_HOURS, Some(h));
            a.scc = Some(scc.to_string());
            a
        };
        let emissions = vec![mk("ZZZ", 1.0), mk("AAA", 2.0), mk("MMM", 4.0)];
        let hours = vec![mk_h("ZZZ", 1.0), mk_h("AAA", 1.0), mk_h("MMM", 1.0)];

        let forward = emission_factors(
            EmissionFactorReport::PerOperatingHourByScc,
            &emissions,
            &hours,
            &[run("g", "hours")],
            &NrSccLookup::new(),
        );
        let mut rev_e = emissions.clone();
        rev_e.reverse();
        let mut rev_h = hours.clone();
        rev_h.reverse();
        let reversed = emission_factors(
            EmissionFactorReport::PerOperatingHourByScc,
            &rev_e,
            &rev_h,
            &[run("g", "hours")],
            &NrSccLookup::new(),
        );
        assert_eq!(forward, reversed, "output must not depend on input order");
        let sccs: Vec<_> = forward.iter().filter_map(|r| r.scc.clone()).collect();
        assert_eq!(sccs, vec!["AAA", "MMM", "ZZZ"], "rows are SCC-sorted");
    }

    #[test]
    fn script_names_are_distinct() {
        // Every report variant names a distinct ported script.
        let inv = [
            InventoryReport::ByCountyAndPollutant,
            InventoryReport::ByCountyFuelTypeAndPollutant,
            InventoryReport::ByEquipmentTypeAndPollutant,
            InventoryReport::ByEquipmentHorsepowerAndPollutant,
            InventoryReport::BySectorHorsepowerAndPollutant,
            InventoryReport::BySectorSccAndPollutant,
        ];
        let ef = [
            EmissionFactorReport::PerOperatingHourByScc,
            EmissionFactorReport::PerOperatingHourBySccAndModelYear,
            EmissionFactorReport::PerOperatingHourByEquipment,
            EmissionFactorReport::PerOperatingHourByEquipmentAndHorsepower,
            EmissionFactorReport::PerHpHrByScc,
            EmissionFactorReport::PerHpHrBySccAndModelYear,
            EmissionFactorReport::PerHpHrByEquipment,
            EmissionFactorReport::PerHpHrByEquipmentAndHorsepower,
            EmissionFactorReport::PerVehicleByScc,
            EmissionFactorReport::PerVehicleByEquipment,
            EmissionFactorReport::PerVehicleByEquipmentAndHorsepower,
        ];
        let mut names: Vec<&str> = inv.iter().map(|r| r.script_name()).collect();
        names.extend(ef.iter().map(|r| r.script_name()));
        let count = names.len();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), count, "script names collide");
        assert_eq!(count, 17, "6 inventory + 11 emission-factor scripts");
    }
}
