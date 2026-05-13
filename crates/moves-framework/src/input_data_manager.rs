//! Default-DB-to-execution-DB extraction — port of `InputDataManager.java`.
//!
//! Legacy MOVES reads the pristine default database and writes a per-run
//! filtered copy into `MOVESExecution`. The Java `InputDataManager` is a
//! 4.7k-line class with three parts:
//!
//! 1. A bank of `buildSQLWhereClauseFor*` static methods that, given the
//!    current `ExecutionRunSpec`, emit MariaDB `WHERE` clauses constraining
//!    one dimension (year, month, county, …) to the RunSpec's active
//!    selections.
//! 2. A static table-by-table registry (`tablesAndFilterColumns`) that
//!    maps each default-DB table to the subset of dimensions that table
//!    is allowed to be filtered on. For every row, a column name (or
//!    `null`) records which builder applies.
//! 3. A `merge(source, destination, …)` orchestrator that iterates the
//!    registry, runs the appropriate builders for each table, combines
//!    the clauses with `AND`, and issues filtered `INSERT … SELECT`.
//!
//! The Rust port keeps the same three-part shape but unwinds the MariaDB
//! plumbing:
//!
//! * [`RunSpecFilters`] holds the dimension values projected once from a
//!   parsed [`moves_runspec::RunSpec`]. The Java code calls
//!   `ExecutionRunSpec.theExecutionRunSpec.years` etc.; Rust callers build
//!   a [`RunSpecFilters`] up front and hand it to the builder API by
//!   reference, which keeps the data dependencies explicit and the
//!   builders testable in isolation.
//! * [`WhereClause`] is a structured value (not a SQL string) representing
//!   one filter constraint on a column. Phase 4 (Task 50) lowers it into
//!   a Polars `Expr` for predicate pushdown into Parquet; Phase 2 keeps it
//!   as data so the builders are pure functions that tests can inspect.
//! * [`MergeTableSpec`] mirrors the Java `TableToCopy` inner class: a
//!   table name plus the per-dimension column annotations. The
//!   [`default_tables`] function returns the default registry; Phase 3
//!   calculator additions extend it as new default-DB tables become active.
//! * [`InputDataManager`] is the orchestrator. [`InputDataManager::plan`]
//!   walks the registry and returns a [`MergePlan`] — one
//!   [`TableMergePlan`] per table, with the filters resolved. Phase 4's
//!   data plane consumes the plan against a Parquet root to materialise
//!   [`ExecutionTables`](crate::ExecutionTables).
//!
//! # What ports verbatim, what is stubbed
//!
//! All 17 builder methods covered by `InputDataManagerTest` port directly:
//! Years, FuelYears, ModelYears, Months, Days, Hours, Links, Zones,
//! Counties, States, Regions, Pollutants, Processes, PollutantProcessIDs,
//! RoadTypes, FuelTypes, SourceUseTypes. The dimensions whose Java
//! builders required a live DB connection — `HourDayIDs`, the NonRoad
//! source-type lookup, fuel-subtype derivation — stay TODOs until Task 50
//! lands the Parquet snapshots that hold the lookup tables those queries
//! consulted. The table registry already records the column names so the
//! eventual implementations slot in without a registry rewrite.
//!
//! The merge driver itself (`InputDataManager::plan`) is data-plane-free:
//! it produces the plan but does not yet copy rows. Task 50 wires the
//! plan to Polars `LazyFrame::scan_parquet` + `filter` to do the actual
//! load.

use std::collections::BTreeSet;

use moves_data::PolProcessId;
use moves_runspec::{GeoKind, RunSpec};

// ---------------------------------------------------------------------------
// RunSpec projection — one struct holding every dimension the builders use.
// ---------------------------------------------------------------------------

/// All filter-eligible dimensions projected from a parsed [`RunSpec`].
///
/// The Java code reads each dimension off
/// `ExecutionRunSpec.theExecutionRunSpec.<field>`. Pulling them out into a
/// single value type up front means [`WhereClauseBuilder`] takes
/// pure-data inputs (`&RunSpecFilters`) and tests can construct one by
/// hand without a full RunSpec.
///
/// Each field is `Vec<i64>` regardless of source width (`u16`, `u32`) so
/// the Phase 4 lowering to Polars expressions is uniform — Polars's
/// `Expr::lit` builds an integer literal from `i64` either way.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RunSpecFilters {
    /// `Timespan.year` → `yearID` filter values.
    pub years: Vec<i64>,
    /// `fuelYearID` filter values. Java derives this in `ExecutionRunSpec`
    /// from the years × fuel calendar; until Task 15 lands the projection,
    /// callers pass this in explicitly or leave it empty (in which case
    /// `for_fuel_years` returns `None`).
    pub fuel_years: Vec<i64>,
    /// `Timespan.month` → `monthID` filter values.
    pub months: Vec<i64>,
    /// `Timespan.day` → `dayID` filter values.
    pub days: Vec<i64>,
    /// Hours expanded from `Timespan.begin_hour`..=`Timespan.end_hour`
    /// (1–24, inclusive). Empty if either endpoint is unset.
    pub hours: Vec<i64>,
    /// `linkID` values from the run's execution locations. Empty in
    /// Phase 2 (depends on Task 16's location iterator); the projection
    /// honours whatever the caller supplies.
    pub link_ids: Vec<i64>,
    /// `zoneID` values from the run's execution locations.
    pub zone_ids: Vec<i64>,
    /// `countyID` values from the run's `geographic_selections` filtered
    /// to [`GeoKind::County`]. Project-scale RunSpecs name counties
    /// directly; national-scale runs default to all known counties (left
    /// for the data plane to enumerate).
    pub county_ids: Vec<i64>,
    /// `stateID` values from `geographic_selections` of
    /// [`GeoKind::State`].
    pub state_ids: Vec<i64>,
    /// `regionID` values from the derived region set. Empty in Phase 2.
    pub region_ids: Vec<i64>,
    /// Deduplicated `pollutantID` values pulled from the RunSpec's
    /// pollutant/process associations.
    pub pollutant_ids: Vec<i64>,
    /// Deduplicated `processID` values pulled from the same associations.
    pub process_ids: Vec<i64>,
    /// `polProcessID` composite ids (pollutantID*100 + processID) for
    /// each association in the RunSpec. The Java builder additionally
    /// applies a `118→120` expansion (NonECPM ⇒ NonECNonSO4PM); see
    /// [`WhereClauseBuilder::for_pollutant_process_ids`].
    pub pol_process_ids: Vec<i64>,
    /// `roadTypeID` filter values from the RunSpec's `road_types`.
    pub road_type_ids: Vec<i64>,
    /// `sourceTypeID` filter values pulled from both on- and off-road
    /// vehicle selections.
    pub source_type_ids: Vec<i64>,
    /// `fuelTypeID` filter values pulled from both on- and off-road
    /// vehicle selections.
    pub fuel_type_ids: Vec<i64>,
    /// True when the RunSpec asks for VMT output but no distance pollutant
    /// is selected. The Java code in that case prepends THC (pollutant=1,
    /// process=1, polProcessID=101) to the pollutant / process /
    /// polProcessID clauses so the activity calculator can derive
    /// distance internally.
    pub include_thc_for_vmt: bool,
}

impl RunSpecFilters {
    /// Project a parsed [`RunSpec`] into per-dimension filter values.
    ///
    /// Mirrors the constructor logic in `ExecutionRunSpec.java` (Task 15
    /// will subsume this once the full ExecutionRunSpec lands). Fields
    /// the parsed RunSpec doesn't carry directly (`fuel_years`,
    /// `link_ids`, `zone_ids`, `region_ids`) are left empty — callers
    /// populate them from the execution-side context.
    ///
    /// The `include_thc_for_vmt` flag matches Java's
    /// `runspec.getOutputVMTData() && !runspec.doesHaveDistancePollutantAndProcess()`.
    /// "Distance" maps to pollutantId == 1 (THC) — checking that the
    /// RunSpec mentions any distance pollutant is approximated by
    /// "any pollutantId == 1 present in `pollutant_process_associations`."
    #[must_use]
    pub fn from_runspec(runspec: &RunSpec) -> Self {
        let years = sorted_dedup(runspec.timespan.years.iter().map(|&y| i64::from(y)));
        let months = sorted_dedup(runspec.timespan.months.iter().map(|&m| i64::from(m)));
        let days = sorted_dedup(runspec.timespan.days.iter().map(|&d| i64::from(d)));
        let hours = match (runspec.timespan.begin_hour, runspec.timespan.end_hour) {
            (Some(begin), Some(end)) if begin <= end => (begin..=end).map(i64::from).collect(),
            _ => Vec::new(),
        };

        let mut state_ids: BTreeSet<i64> = BTreeSet::new();
        let mut county_ids: BTreeSet<i64> = BTreeSet::new();
        let mut zone_ids: BTreeSet<i64> = BTreeSet::new();
        let mut link_ids: BTreeSet<i64> = BTreeSet::new();
        for sel in &runspec.geographic_selections {
            match sel.kind {
                GeoKind::State => {
                    state_ids.insert(i64::from(sel.key));
                }
                GeoKind::County => {
                    county_ids.insert(i64::from(sel.key));
                    // County key encodes state in the top digits per FIPS:
                    // stateID = key / 1000. Mirror Java's
                    // `GeographicSelection.County.getStateID`.
                    state_ids.insert(i64::from(sel.key / 1000));
                }
                GeoKind::Zone => {
                    zone_ids.insert(i64::from(sel.key));
                    county_ids.insert(i64::from(sel.key / 10));
                    state_ids.insert(i64::from(sel.key / 10_000));
                }
                GeoKind::Link => {
                    link_ids.insert(i64::from(sel.key));
                }
                GeoKind::Nation => {
                    // Nation-scale selection: do not narrow geography. The
                    // data plane (Task 50) defaults to all states / counties
                    // when these sets are empty.
                }
            }
        }

        let road_type_ids = sorted_dedup(
            runspec
                .road_types
                .iter()
                .map(|rt| i64::from(rt.road_type_id)),
        );

        let source_type_iter = runspec
            .onroad_vehicle_selections
            .iter()
            .map(|s| i64::from(s.source_type_id));
        let source_type_ids = sorted_dedup(source_type_iter);

        let fuel_type_iter = runspec
            .onroad_vehicle_selections
            .iter()
            .map(|s| i64::from(s.fuel_type_id))
            .chain(
                runspec
                    .offroad_vehicle_selections
                    .iter()
                    .map(|s| i64::from(s.fuel_type_id)),
            );
        let fuel_type_ids = sorted_dedup(fuel_type_iter);

        let pollutant_ids = sorted_dedup(
            runspec
                .pollutant_process_associations
                .iter()
                .map(|a| i64::from(a.pollutant_id)),
        );
        let process_ids = sorted_dedup(
            runspec
                .pollutant_process_associations
                .iter()
                .map(|a| i64::from(a.process_id)),
        );
        let pol_process_ids = sorted_dedup(
            runspec
                .pollutant_process_associations
                .iter()
                .map(|a| i64::from(a.pollutant_id) * 100 + i64::from(a.process_id)),
        );

        let has_distance = runspec
            .pollutant_process_associations
            .iter()
            .any(|a| a.pollutant_id == 1);
        let include_thc_for_vmt = runspec.output_vmt_data && !has_distance;

        Self {
            years,
            fuel_years: Vec::new(),
            months,
            days,
            hours,
            link_ids: link_ids.into_iter().collect(),
            zone_ids: zone_ids.into_iter().collect(),
            county_ids: county_ids.into_iter().collect(),
            state_ids: state_ids.into_iter().collect(),
            region_ids: Vec::new(),
            pollutant_ids,
            process_ids,
            pol_process_ids,
            road_type_ids,
            source_type_ids,
            fuel_type_ids,
            include_thc_for_vmt,
        }
    }
}

fn sorted_dedup<I: IntoIterator<Item = i64>>(values: I) -> Vec<i64> {
    let set: BTreeSet<i64> = values.into_iter().collect();
    set.into_iter().collect()
}

// ---------------------------------------------------------------------------
// WhereClause — structured filter constraint, lowered to Polars in Task 50.
// ---------------------------------------------------------------------------

/// One structured filter constraint on a default-DB column.
///
/// In Java this is a SQL string fragment; in Rust it is a typed value
/// because the Phase 4 data plane (Task 50) maps it to a Polars
/// `Expr` for predicate pushdown into Parquet. Phase 2 [`to_sql`] renders
/// to a SQL string so tests can spot-check equivalence with the Java
/// builders.
///
/// [`to_sql`]: WhereClause::to_sql
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WhereClause {
    /// `column IN (values...)`. The mainstream form, produced by every
    /// dimension except model-year and pollutant-process.
    InList {
        /// Default-DB column name the filter applies to.
        column: String,
        /// Distinct values, ascending.
        values: Vec<i64>,
    },
    /// `( (column<=y1 AND column>=y1-40) OR (column<=y2 AND column>=y2-40) … )`.
    ///
    /// Java's `buildSQLWhereClauseForModelYears` justifies the
    /// overlapping-range form on size grounds — explicitly listing every
    /// model year for several calendar years would be larger. The Rust
    /// lowering uses a Polars `OR` chain over the same ranges.
    ModelYearRanges {
        /// Default-DB column name (always a model-year column).
        column: String,
        /// The years from which each range is derived (`year-40 ..= year`).
        years: Vec<i32>,
    },
    /// `(column < 0) OR (column IN (ids...))`.
    ///
    /// Negative `polProcessID` values are "representing" rows that
    /// downstream calculators rely on (per Java comment); they are always
    /// pulled in. The id list is the polProcessIDs implied by the RunSpec's
    /// pollutant/process associations, plus the `118→120` expansion
    /// (`NonECPM → NonECNonSO4PM`).
    PolProcessIds {
        /// Default-DB column name (always `polProcessID`).
        column: String,
        /// Composite ids to include, ascending.
        ids: Vec<i64>,
    },
}

impl WhereClause {
    /// Column the filter applies to.
    #[must_use]
    pub fn column(&self) -> &str {
        match self {
            Self::InList { column, .. }
            | Self::ModelYearRanges { column, .. }
            | Self::PolProcessIds { column, .. } => column,
        }
    }

    /// `true` when the filter has no values — caller should drop it from
    /// the merge plan rather than emit an empty `IN ()` clause.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        match self {
            Self::InList { values, .. } => values.is_empty(),
            Self::ModelYearRanges { years, .. } => years.is_empty(),
            Self::PolProcessIds { ids, .. } => ids.is_empty(),
        }
    }

    /// Render to a SQL string in the same shape the Java builders produce.
    ///
    /// Used by tests to spot-check that the Rust port matches Java byte-
    /// for-byte; not on the runtime hot path. The Phase 4 data plane
    /// lowers a [`WhereClause`] directly to a Polars `Expr` and skips the
    /// SQL representation.
    #[must_use]
    pub fn to_sql(&self) -> String {
        match self {
            Self::InList { column, values } => {
                let body = values
                    .iter()
                    .map(i64::to_string)
                    .collect::<Vec<_>>()
                    .join(",");
                format!("{column} IN ({body})")
            }
            Self::ModelYearRanges { column, years } => {
                let parts = years
                    .iter()
                    .map(|y| format!("({column}<={y} and {column}>={})", y - 40))
                    .collect::<Vec<_>>()
                    .join(" or ");
                format!("({parts})")
            }
            Self::PolProcessIds { column, ids } => {
                if ids.is_empty() {
                    format!("{column} < 0")
                } else {
                    let body = ids.iter().map(i64::to_string).collect::<Vec<_>>().join(",");
                    format!("{column} < 0 OR {column} IN ({body})")
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// WhereClauseBuilder — one entry point per filter dimension.
// ---------------------------------------------------------------------------

/// Bank of dimension-specific predicate builders.
///
/// Each method ports one of the `buildSQLWhereClauseFor*` static methods
/// from `InputDataManager.java`. The return type is `Option<WhereClause>`:
/// `None` if the RunSpec carries no values for that dimension (the Java
/// code returns an empty `Vector` in that case; we elevate the absence
/// to the type system so the merge driver can simply filter out `None`s).
pub struct WhereClauseBuilder;

impl WhereClauseBuilder {
    /// `buildSQLWhereClauseForYears` — `yearID IN (years...)`.
    #[must_use]
    pub fn for_years(filters: &RunSpecFilters, column: &str) -> Option<WhereClause> {
        if filters.years.is_empty() {
            return None;
        }
        Some(WhereClause::InList {
            column: column.to_string(),
            values: filters.years.clone(),
        })
    }

    /// `buildSQLWhereClauseForFuelYears` — `fuelYearID IN (fuel_years...)`.
    #[must_use]
    pub fn for_fuel_years(filters: &RunSpecFilters, column: &str) -> Option<WhereClause> {
        if filters.fuel_years.is_empty() {
            return None;
        }
        Some(WhereClause::InList {
            column: column.to_string(),
            values: filters.fuel_years.clone(),
        })
    }

    /// `buildSQLWhereClauseForModelYears` — overlapping
    /// `(column<=year AND column>=year-40)` ranges per calendar year.
    #[must_use]
    pub fn for_model_years(filters: &RunSpecFilters, column: &str) -> Option<WhereClause> {
        if filters.years.is_empty() {
            return None;
        }
        Some(WhereClause::ModelYearRanges {
            column: column.to_string(),
            years: filters.years.iter().map(|&y| y as i32).collect(),
        })
    }

    /// `buildSQLWhereClauseForMonths` — `monthID IN (months...)`.
    #[must_use]
    pub fn for_months(filters: &RunSpecFilters, column: &str) -> Option<WhereClause> {
        Self::simple_in_list(column, &filters.months)
    }

    /// `buildSQLWhereClauseForDays` — `dayID IN (days...)`.
    #[must_use]
    pub fn for_days(filters: &RunSpecFilters, column: &str) -> Option<WhereClause> {
        Self::simple_in_list(column, &filters.days)
    }

    /// `buildSQLWhereClauseForHours` — `hourID IN (hours...)`.
    #[must_use]
    pub fn for_hours(filters: &RunSpecFilters, column: &str) -> Option<WhereClause> {
        Self::simple_in_list(column, &filters.hours)
    }

    /// `buildSQLWhereClauseForLinks` — `linkID IN (link_ids...)`.
    #[must_use]
    pub fn for_links(filters: &RunSpecFilters, column: &str) -> Option<WhereClause> {
        Self::simple_in_list(column, &filters.link_ids)
    }

    /// `buildSQLWhereClauseForZones` — `zoneID IN (zone_ids...)`.
    #[must_use]
    pub fn for_zones(filters: &RunSpecFilters, column: &str) -> Option<WhereClause> {
        Self::simple_in_list(column, &filters.zone_ids)
    }

    /// `buildSQLWhereClauseForCounties` — `countyID IN (county_ids...)`.
    #[must_use]
    pub fn for_counties(filters: &RunSpecFilters, column: &str) -> Option<WhereClause> {
        Self::simple_in_list(column, &filters.county_ids)
    }

    /// `buildSQLWhereClauseForStates` — `stateID IN (state_ids...)`.
    #[must_use]
    pub fn for_states(filters: &RunSpecFilters, column: &str) -> Option<WhereClause> {
        Self::simple_in_list(column, &filters.state_ids)
    }

    /// `buildSQLWhereClauseForRegions` — `regionID IN (region_ids...)`.
    #[must_use]
    pub fn for_regions(filters: &RunSpecFilters, column: &str) -> Option<WhereClause> {
        Self::simple_in_list(column, &filters.region_ids)
    }

    /// `buildSQLWhereClauseForRoadTypes` — `roadTypeID IN (road_type_ids...)`.
    #[must_use]
    pub fn for_road_types(filters: &RunSpecFilters, column: &str) -> Option<WhereClause> {
        Self::simple_in_list(column, &filters.road_type_ids)
    }

    /// `buildSQLWhereClauseForFuelTypes` — `fuelTypeID IN (fuel_type_ids...)`.
    #[must_use]
    pub fn for_fuel_types(filters: &RunSpecFilters, column: &str) -> Option<WhereClause> {
        Self::simple_in_list(column, &filters.fuel_type_ids)
    }

    /// `buildSQLWhereClauseForSourceUseTypes` — `sourceTypeID IN (source_type_ids...)`.
    #[must_use]
    pub fn for_source_use_types(filters: &RunSpecFilters, column: &str) -> Option<WhereClause> {
        Self::simple_in_list(column, &filters.source_type_ids)
    }

    /// `buildSQLWhereClauseForPollutants` — `pollutantID IN (pollutant_ids...)`.
    ///
    /// When `include_thc_for_vmt` is set, the Java code prepends pollutantID
    /// 1 (THC) to the list. Mirrored here.
    #[must_use]
    pub fn for_pollutants(filters: &RunSpecFilters, column: &str) -> Option<WhereClause> {
        let mut values = filters.pollutant_ids.clone();
        if filters.include_thc_for_vmt {
            push_unique(&mut values, 1);
        }
        if values.is_empty() {
            return None;
        }
        Some(WhereClause::InList {
            column: column.to_string(),
            values,
        })
    }

    /// `buildSQLWhereClauseForProcesses` — `processID IN (process_ids...)`.
    ///
    /// When `include_thc_for_vmt` is set, the Java code prepends processID
    /// 1 (Running Exhaust) to the list. Mirrored here.
    #[must_use]
    pub fn for_processes(filters: &RunSpecFilters, column: &str) -> Option<WhereClause> {
        let mut values = filters.process_ids.clone();
        if filters.include_thc_for_vmt {
            push_unique(&mut values, 1);
        }
        if values.is_empty() {
            return None;
        }
        Some(WhereClause::InList {
            column: column.to_string(),
            values,
        })
    }

    /// `buildSQLWhereClauseForPollutantProcessIDs` —
    /// `(column < 0) OR (column IN (pol_process_ids…))`.
    ///
    /// The Java code performs three composite operations:
    ///
    /// 1. Always includes the negative-id "representing" rows
    ///    (`column < 0`).
    /// 2. Prepends polProcessID 101 (THC × Running Exhaust) when
    ///    `output_vmt_data` is on without a distance pollutant.
    /// 3. For each polProcessID whose pollutant is 118 (NonECPM), also
    ///    includes the 120×100 + process variant (NonECNonSO4PM × same
    ///    process).
    #[must_use]
    pub fn for_pollutant_process_ids(
        filters: &RunSpecFilters,
        column: &str,
    ) -> Option<WhereClause> {
        let mut ids: BTreeSet<i64> = filters.pol_process_ids.iter().copied().collect();

        if filters.include_thc_for_vmt {
            ids.insert(101);
        }

        let mut expanded = Vec::new();
        for &id in &ids {
            expanded.push(id);
            if PolProcessId(id as u32).pollutant_id().0 == 118 {
                let process_part = id % 100;
                expanded.push(120 * 100 + process_part);
            }
        }
        let ids: Vec<i64> = expanded
            .into_iter()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();

        // Always emit the clause — even with an empty `ids` list the
        // `< 0` half ensures representing rows come through. The Java
        // returns an empty Vector only when nothing was added at all,
        // which only happens when the RunSpec has zero associations and
        // `include_thc_for_vmt` is false.
        if ids.is_empty() {
            return None;
        }
        Some(WhereClause::PolProcessIds {
            column: column.to_string(),
            ids,
        })
    }

    fn simple_in_list(column: &str, values: &[i64]) -> Option<WhereClause> {
        if values.is_empty() {
            None
        } else {
            Some(WhereClause::InList {
                column: column.to_string(),
                values: values.to_vec(),
            })
        }
    }
}

fn push_unique(values: &mut Vec<i64>, candidate: i64) {
    if !values.contains(&candidate) {
        values.insert(0, candidate);
    }
}

// ---------------------------------------------------------------------------
// MergeTableSpec — per-table column annotations.
// ---------------------------------------------------------------------------

/// Per-table column annotations driving filter application.
///
/// Each field, when `Some`, records the name of the column in the source
/// table that the corresponding dimension's WHERE clause applies to.
/// `None` means the table is not filtered on that dimension — either the
/// table has no such column or filtering would discard records the
/// downstream calculators still need (see the comments in
/// `InputDataManager.java`'s `tablesAndFilterColumns` array for the
/// per-table reasoning).
///
/// Mirrors the inner `TableToCopy` class in Java line-for-line, less the
/// nonroad-specific fields (`sectorID`, `equipmentTypeID`) which the
/// `NRTableToCopy` companion uses.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct MergeTableSpec {
    /// Default-DB table name (case-sensitive).
    pub table_name: &'static str,
    /// Column to filter against [`WhereClauseBuilder::for_years`].
    pub year_column: Option<&'static str>,
    /// Column to filter against [`WhereClauseBuilder::for_months`].
    pub month_column: Option<&'static str>,
    /// Column to filter against [`WhereClauseBuilder::for_days`].
    pub day_column: Option<&'static str>,
    /// Column to filter against [`WhereClauseBuilder::for_hours`].
    pub hour_column: Option<&'static str>,
    /// Column to filter against [`WhereClauseBuilder::for_links`].
    pub link_column: Option<&'static str>,
    /// Column to filter against [`WhereClauseBuilder::for_zones`].
    pub zone_column: Option<&'static str>,
    /// Column to filter against [`WhereClauseBuilder::for_counties`].
    pub county_column: Option<&'static str>,
    /// Column to filter against [`WhereClauseBuilder::for_states`].
    pub state_column: Option<&'static str>,
    /// Column to filter against [`WhereClauseBuilder::for_regions`].
    pub region_column: Option<&'static str>,
    /// Column to filter against [`WhereClauseBuilder::for_pollutants`].
    pub pollutant_column: Option<&'static str>,
    /// Column to filter against [`WhereClauseBuilder::for_processes`].
    pub process_column: Option<&'static str>,
    /// Column to filter against [`WhereClauseBuilder::for_road_types`].
    pub road_type_column: Option<&'static str>,
    /// Column to filter against [`WhereClauseBuilder::for_pollutant_process_ids`].
    pub pol_process_id_column: Option<&'static str>,
    /// Column to filter against [`WhereClauseBuilder::for_source_use_types`].
    pub source_use_type_column: Option<&'static str>,
    /// Column to filter against [`WhereClauseBuilder::for_fuel_types`].
    pub fuel_type_column: Option<&'static str>,
    /// Column to filter against [`WhereClauseBuilder::for_fuel_years`].
    pub fuel_year_column: Option<&'static str>,
    /// Column to filter against [`WhereClauseBuilder::for_model_years`].
    pub model_year_column: Option<&'static str>,
}

impl MergeTableSpec {
    /// Construct a spec with no filter columns (the table is copied wholesale).
    #[must_use]
    pub const fn new(table_name: &'static str) -> Self {
        Self {
            table_name,
            year_column: None,
            month_column: None,
            day_column: None,
            hour_column: None,
            link_column: None,
            zone_column: None,
            county_column: None,
            state_column: None,
            region_column: None,
            pollutant_column: None,
            process_column: None,
            road_type_column: None,
            pol_process_id_column: Option::None,
            source_use_type_column: None,
            fuel_type_column: None,
            fuel_year_column: None,
            model_year_column: None,
        }
    }

    /// Annotate the year-filter column.
    #[must_use]
    pub const fn year(mut self, c: &'static str) -> Self {
        self.year_column = Some(c);
        self
    }

    /// Annotate the month-filter column.
    #[must_use]
    pub const fn month(mut self, c: &'static str) -> Self {
        self.month_column = Some(c);
        self
    }

    /// Annotate the day-filter column.
    #[must_use]
    pub const fn day(mut self, c: &'static str) -> Self {
        self.day_column = Some(c);
        self
    }

    /// Annotate the hour-filter column.
    #[must_use]
    pub const fn hour(mut self, c: &'static str) -> Self {
        self.hour_column = Some(c);
        self
    }

    /// Annotate the link-filter column.
    #[must_use]
    pub const fn link(mut self, c: &'static str) -> Self {
        self.link_column = Some(c);
        self
    }

    /// Annotate the zone-filter column.
    #[must_use]
    pub const fn zone(mut self, c: &'static str) -> Self {
        self.zone_column = Some(c);
        self
    }

    /// Annotate the county-filter column.
    #[must_use]
    pub const fn county(mut self, c: &'static str) -> Self {
        self.county_column = Some(c);
        self
    }

    /// Annotate the state-filter column.
    #[must_use]
    pub const fn state(mut self, c: &'static str) -> Self {
        self.state_column = Some(c);
        self
    }

    /// Annotate the region-filter column.
    #[must_use]
    pub const fn region(mut self, c: &'static str) -> Self {
        self.region_column = Some(c);
        self
    }

    /// Annotate the pollutant-filter column.
    #[must_use]
    pub const fn pollutant(mut self, c: &'static str) -> Self {
        self.pollutant_column = Some(c);
        self
    }

    /// Annotate the process-filter column.
    #[must_use]
    pub const fn process(mut self, c: &'static str) -> Self {
        self.process_column = Some(c);
        self
    }

    /// Annotate the road-type-filter column.
    #[must_use]
    pub const fn road_type(mut self, c: &'static str) -> Self {
        self.road_type_column = Some(c);
        self
    }

    /// Annotate the polProcessID-filter column.
    #[must_use]
    pub const fn pol_process_id(mut self, c: &'static str) -> Self {
        self.pol_process_id_column = Some(c);
        self
    }

    /// Annotate the source-use-type-filter column.
    #[must_use]
    pub const fn source_use_type(mut self, c: &'static str) -> Self {
        self.source_use_type_column = Some(c);
        self
    }

    /// Annotate the fuel-type-filter column.
    #[must_use]
    pub const fn fuel_type(mut self, c: &'static str) -> Self {
        self.fuel_type_column = Some(c);
        self
    }

    /// Annotate the fuel-year-filter column.
    #[must_use]
    pub const fn fuel_year(mut self, c: &'static str) -> Self {
        self.fuel_year_column = Some(c);
        self
    }

    /// Annotate the model-year-filter column.
    #[must_use]
    pub const fn model_year(mut self, c: &'static str) -> Self {
        self.model_year_column = Some(c);
        self
    }
}

// ---------------------------------------------------------------------------
// MergePlan — output of the orchestrator.
// ---------------------------------------------------------------------------

/// The set of filter clauses to apply to one default-DB table.
///
/// The `clauses` are combined with `AND` at load time. An empty `clauses`
/// vector means "load the whole table" — used for reference tables like
/// `AgeCategory`, `AvgSpeedBin`, etc.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableMergePlan {
    /// Source table to load.
    pub table_name: &'static str,
    /// Filter clauses to AND together.
    pub clauses: Vec<WhereClause>,
}

/// Complete plan describing how to materialise the per-run execution DB.
///
/// One entry per [`MergeTableSpec`] in the registry; the data plane
/// consumes the plan against a Parquet root to produce
/// [`ExecutionTables`](crate::ExecutionTables) (Task 50).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct MergePlan {
    /// One entry per table, in registry order.
    pub tables: Vec<TableMergePlan>,
}

impl MergePlan {
    /// Number of tables in the plan.
    #[must_use]
    pub fn len(&self) -> usize {
        self.tables.len()
    }

    /// `true` when the plan has no tables (degenerate / test-only state).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }

    /// Look up one table's plan by name. Case-sensitive (the registry uses
    /// the same casing as the default-DB schema).
    #[must_use]
    pub fn find(&self, table_name: &str) -> Option<&TableMergePlan> {
        self.tables.iter().find(|t| t.table_name == table_name)
    }
}

// ---------------------------------------------------------------------------
// InputDataManager — orchestrator.
// ---------------------------------------------------------------------------

/// The Rust port of `InputDataManager.java`.
///
/// A unit struct with associated functions only — the Java class had no
/// instance state worth preserving (the few instance constructors existed
/// solely for inner-class scoping). [`plan`](Self::plan) is the entry
/// point; Phase 4 (Task 50) adds a sibling `execute` that consumes a
/// [`MergePlan`] against a Parquet root.
#[derive(Debug, Clone, Copy, Default)]
pub struct InputDataManager;

impl InputDataManager {
    /// Build a [`MergePlan`] over the given table registry.
    ///
    /// Walks each [`MergeTableSpec`] and, for every `*_column: Some(name)`
    /// annotation, calls the matching [`WhereClauseBuilder`] entry point.
    /// Empty clauses (no filter values in the RunSpec) are skipped so the
    /// resulting plan only carries actionable predicates.
    #[must_use]
    pub fn plan(filters: &RunSpecFilters, tables: &[MergeTableSpec]) -> MergePlan {
        let mut plans = Vec::with_capacity(tables.len());
        for spec in tables {
            let mut clauses = Vec::new();
            if let Some(c) = spec
                .year_column
                .and_then(|n| WhereClauseBuilder::for_years(filters, n))
            {
                clauses.push(c);
            }
            if let Some(c) = spec
                .month_column
                .and_then(|n| WhereClauseBuilder::for_months(filters, n))
            {
                clauses.push(c);
            }
            if let Some(c) = spec
                .day_column
                .and_then(|n| WhereClauseBuilder::for_days(filters, n))
            {
                clauses.push(c);
            }
            if let Some(c) = spec
                .hour_column
                .and_then(|n| WhereClauseBuilder::for_hours(filters, n))
            {
                clauses.push(c);
            }
            if let Some(c) = spec
                .link_column
                .and_then(|n| WhereClauseBuilder::for_links(filters, n))
            {
                clauses.push(c);
            }
            if let Some(c) = spec
                .zone_column
                .and_then(|n| WhereClauseBuilder::for_zones(filters, n))
            {
                clauses.push(c);
            }
            if let Some(c) = spec
                .county_column
                .and_then(|n| WhereClauseBuilder::for_counties(filters, n))
            {
                clauses.push(c);
            }
            if let Some(c) = spec
                .state_column
                .and_then(|n| WhereClauseBuilder::for_states(filters, n))
            {
                clauses.push(c);
            }
            if let Some(c) = spec
                .region_column
                .and_then(|n| WhereClauseBuilder::for_regions(filters, n))
            {
                clauses.push(c);
            }
            if let Some(c) = spec
                .pollutant_column
                .and_then(|n| WhereClauseBuilder::for_pollutants(filters, n))
            {
                clauses.push(c);
            }
            if let Some(c) = spec
                .process_column
                .and_then(|n| WhereClauseBuilder::for_processes(filters, n))
            {
                clauses.push(c);
            }
            if let Some(c) = spec
                .road_type_column
                .and_then(|n| WhereClauseBuilder::for_road_types(filters, n))
            {
                clauses.push(c);
            }
            if let Some(c) = spec
                .pol_process_id_column
                .and_then(|n| WhereClauseBuilder::for_pollutant_process_ids(filters, n))
            {
                clauses.push(c);
            }
            if let Some(c) = spec
                .source_use_type_column
                .and_then(|n| WhereClauseBuilder::for_source_use_types(filters, n))
            {
                clauses.push(c);
            }
            if let Some(c) = spec
                .fuel_type_column
                .and_then(|n| WhereClauseBuilder::for_fuel_types(filters, n))
            {
                clauses.push(c);
            }
            if let Some(c) = spec
                .fuel_year_column
                .and_then(|n| WhereClauseBuilder::for_fuel_years(filters, n))
            {
                clauses.push(c);
            }
            if let Some(c) = spec
                .model_year_column
                .and_then(|n| WhereClauseBuilder::for_model_years(filters, n))
            {
                clauses.push(c);
            }
            plans.push(TableMergePlan {
                table_name: spec.table_name,
                clauses,
            });
        }
        MergePlan { tables: plans }
    }
}

// ---------------------------------------------------------------------------
// Default table registry — port of tablesAndFilterColumns.
// ---------------------------------------------------------------------------

/// Default registry of merge-eligible tables.
///
/// Direct port of the `tablesAndFilterColumns` array in
/// `InputDataManager.merge(Connection, …)`. Each entry records the
/// dimensions the named table can be filtered on; conditional Java
/// entries (e.g. `includeFuelSupply ? new TableToCopy(...) : null`) are
/// represented as unconditional entries here — the caller drops them by
/// pruning the registry, not by reading boolean flags inside the
/// registry itself. The Java comments justifying each `null` annotation
/// (e.g. "AvgSpeedDistribution cannot filter by roadTypeID because
/// TotalActivityGenerator…") are preserved as `//` comments on the
/// matching entry.
///
/// Naming follows the Java source verbatim — the table names match the
/// MOVES default-DB casing so a Parquet snapshot keyed by the same string
/// can be located with one lookup.
#[must_use]
pub fn default_tables() -> Vec<MergeTableSpec> {
    vec![
        MergeTableSpec::new("AgeCategory"),
        MergeTableSpec::new("AgeGroup"),
        // ATBaseEmissions also carries a `monthGroupID` column in Java's
        // registry; Task 50 wires the month-group filter alongside the
        // rest of the DB-derived dimensions.
        MergeTableSpec::new("ATBaseEmissions").pol_process_id("polProcessID"),
        MergeTableSpec::new("ATRatio")
            .pol_process_id("polProcessID")
            .fuel_type("fuelTypeID"),
        MergeTableSpec::new("ATRatioGas2")
            .pol_process_id("polProcessID")
            .source_use_type("sourceTypeID"),
        MergeTableSpec::new("ATRatioNonGas")
            .pol_process_id("polProcessID")
            .source_use_type("sourceTypeID"),
        MergeTableSpec::new("AverageTankGasoline")
            .zone("zoneID")
            .fuel_type("fuelTypeID")
            .fuel_year("fuelYearID"),
        MergeTableSpec::new("AverageTankTemperature")
            .month("monthID")
            .zone("zoneID"),
        MergeTableSpec::new("AvgSpeedBin"),
        // AvgSpeedDistribution cannot filter by roadTypeID or hourDayID
        // because TotalActivityGenerator will not be able to calculate
        // SourceHours properly.
        MergeTableSpec::new("AvgSpeedDistribution").source_use_type("sourceTypeID"),
        MergeTableSpec::new("AVFT").source_use_type("sourceTypeID"),
        MergeTableSpec::new("BaseFuel").fuel_type("fuelTypeID"),
        MergeTableSpec::new("ColdSoakInitialHourFraction")
            .month("monthID")
            .zone("zoneID")
            .source_use_type("sourceTypeID"),
        MergeTableSpec::new("ColdSoakTankTemperature")
            .month("monthID")
            .zone("zoneID")
            .hour("hourID"),
        MergeTableSpec::new("ComplexModelParameterName"),
        MergeTableSpec::new("ComplexModelParameters").pol_process_id("polProcessID"),
        MergeTableSpec::new("County")
            .county("countyID")
            .state("stateID"),
        MergeTableSpec::new("countyType"),
        MergeTableSpec::new("CountyYear")
            .year("yearID")
            .county("countyID"),
        // CrankcaseEmissionRatio can't be filtered by polProcessID because
        // PM needs NonECNonSO4PM which isn't shown on the GUI.
        MergeTableSpec::new("CrankcaseEmissionRatio")
            .source_use_type("sourceTypeID")
            .fuel_type("fuelTypeID"),
        MergeTableSpec::new("criteriaRatio")
            .pol_process_id("polProcessID")
            .source_use_type("sourceTypeID")
            .fuel_type("fuelTypeID"),
        MergeTableSpec::new("CumTVVCoeffs").pol_process_id("polProcessID"),
        MergeTableSpec::new("DataSource"),
        MergeTableSpec::new("DayOfAnyWeek").day("dayID"),
        // DayVMTFraction cannot filter by roadTypeID or TotalActivityGenerator
        // will not be able to calculate SourceHours properly.
        MergeTableSpec::new("DayVMTFraction")
            .month("monthID")
            .day("dayID")
            .source_use_type("sourceTypeID"),
        MergeTableSpec::new("dioxinemissionrate")
            .pol_process_id("polProcessID")
            .fuel_type("fuelTypeID"),
        MergeTableSpec::new("DriveSchedule"),
        MergeTableSpec::new("DriveScheduleAssoc")
            .road_type("roadTypeID")
            .source_use_type("sourceTypeID"),
        MergeTableSpec::new("DriveScheduleSecond"),
        MergeTableSpec::new("driveScheduleSecondLink"),
        MergeTableSpec::new("e10FuelProperties")
            .fuel_year("fuelYearID")
            .region("fuelRegionID"),
        MergeTableSpec::new("EmissionProcess").process("processID"),
        MergeTableSpec::new("EmissionRate").pol_process_id("polProcessID"),
        MergeTableSpec::new("EmissionRateByAge").pol_process_id("polProcessID"),
        MergeTableSpec::new("EmissionRateAdjustment")
            .pol_process_id("polProcessID")
            .source_use_type("sourceTypeID")
            .fuel_type("fuelTypeID"),
        MergeTableSpec::new("EngineSize"),
        MergeTableSpec::new("EngineTech"),
        MergeTableSpec::new("ETOHBin"),
        MergeTableSpec::new("evapTemperatureAdjustment").process("processID"),
        MergeTableSpec::new("evapRVPTemperatureAdjustment")
            .process("processID")
            .fuel_type("fuelTypeID"),
        MergeTableSpec::new("evefficiency")
            .pol_process_id("polProcessID")
            .source_use_type("sourceTypeID"),
        MergeTableSpec::new("FleetAvgAdjustment").pol_process_id("polProcessID"),
        MergeTableSpec::new("FuelEngTechAssoc"),
        MergeTableSpec::new("FuelFormulation"),
        MergeTableSpec::new("FuelModelName"),
        MergeTableSpec::new("FuelModelWtFactor"),
        MergeTableSpec::new("FuelModelYearGroup"),
        MergeTableSpec::new("FuelParameterName"),
        MergeTableSpec::new("FuelSubtype").fuel_type("fuelTypeID"),
        MergeTableSpec::new("FuelSupply")
            .fuel_year("fuelYearID")
            .region("fuelRegionID"),
        MergeTableSpec::new("FuelSupplyYear").fuel_year("fuelYearID"),
        MergeTableSpec::new("FuelType").fuel_type("fuelTypeID"),
        MergeTableSpec::new("fuelUsageFraction")
            .county("countyID")
            .fuel_type("sourceBinFuelTypeID")
            .fuel_year("fuelYearID"),
        MergeTableSpec::new("fuelWizardFactors"),
        MergeTableSpec::new("FullACAdjustment")
            .pol_process_id("polProcessID")
            .source_use_type("sourceTypeID"),
        MergeTableSpec::new("generalFuelRatio")
            .pollutant("pollutantID")
            .process("processID")
            .pol_process_id("polProcessID")
            .fuel_type("fuelTypeID"),
        MergeTableSpec::new("generalFuelRatioExpression")
            .pol_process_id("polProcessID")
            .fuel_type("fuelTypeID"),
        MergeTableSpec::new("GreetManfAndDisposal").pollutant("pollutantID"),
        // Contains "base year" / bounding values; filtered only on pollutantID.
        MergeTableSpec::new("GreetWellToPump").pollutant("pollutantID"),
        MergeTableSpec::new("Grid"),
        MergeTableSpec::new("GridZoneAssoc").zone("zoneID"),
        MergeTableSpec::new("HCPermeationCoeff").pol_process_id("polProcessID"),
        MergeTableSpec::new("HCSpeciation").pol_process_id("polProcessID"),
        // hotellingActivityDistribution uses wildcards for zoneID, so it
        // cannot be filtered by zone.
        MergeTableSpec::new("hotellingActivityDistribution"),
        MergeTableSpec::new("hotellingAgeFraction").zone("zoneID"),
        MergeTableSpec::new("hotellingHours")
            .year("yearID")
            .month("monthID")
            .zone("zoneID")
            .source_use_type("sourceTypeID"),
        MergeTableSpec::new("hotellingCalendarYear").year("yearID"),
        MergeTableSpec::new("hotellingHourFraction")
            .zone("zoneID")
            .day("dayID")
            .hour("hourID"),
        MergeTableSpec::new("hotellingMonthAdjust")
            .month("monthID")
            .zone("zoneID"),
        MergeTableSpec::new("hotellingHoursPerDay")
            .year("yearID")
            .zone("zoneID")
            .day("dayID"),
        // HourDay cannot be filtered by hourID because TotalActivityGenerator
        // requires all hours.
        MergeTableSpec::new("HourDay").day("dayID"),
        // HourOfAnyDay cannot be filtered by hourID because
        // TotalActivityGenerator requires all hours.
        MergeTableSpec::new("HourOfAnyDay"),
        // HourVMTFraction cannot filter by roadTypeID or hourID.
        MergeTableSpec::new("HourVMTFraction")
            .day("dayID")
            .source_use_type("sourceTypeID"),
        MergeTableSpec::new("HPMSVtype"),
        MergeTableSpec::new("HPMSVtypeDay")
            .year("yearID")
            .month("monthID")
            .day("dayID"),
        // Contains "base year" / bounding values; cannot be filtered.
        MergeTableSpec::new("HPMSVtypeYear"),
        MergeTableSpec::new("IMCoverage")
            .year("yearID")
            .county("countyID")
            .state("stateID")
            .pol_process_id("polProcessID")
            .source_use_type("sourceTypeID")
            .fuel_type("fuelTypeID"),
        MergeTableSpec::new("idleDayAdjust")
            .day("dayID")
            .source_use_type("sourceTypeID"),
        MergeTableSpec::new("idleModelYearGrouping").source_use_type("sourceTypeID"),
        MergeTableSpec::new("idleMonthAdjust")
            .month("monthID")
            .source_use_type("sourceTypeID"),
        MergeTableSpec::new("idleRegion"),
        MergeTableSpec::new("IMFactor")
            .pol_process_id("polProcessID")
            .source_use_type("sourceTypeID")
            .fuel_type("fuelTypeID"),
        MergeTableSpec::new("IMInspectFreq"),
        MergeTableSpec::new("IMModelYearGroup"),
        MergeTableSpec::new("IMTestStandards"),
        MergeTableSpec::new("StartsOpModeDistribution")
            .day("dayID")
            .hour("hourID")
            .source_use_type("sourceTypeID"),
        MergeTableSpec::new("integratedSpeciesSet"),
        MergeTableSpec::new("integratedSpeciesSetName"),
        MergeTableSpec::new("Link")
            .zone("zoneID")
            .county("countyID")
            .road_type("roadTypeID"),
        MergeTableSpec::new("LinkAverageSpeed"),
        MergeTableSpec::new("LinkHourVMTFraction")
            .month("monthID")
            .day("dayID")
            .hour("hourID")
            .source_use_type("sourceTypeID"),
        MergeTableSpec::new("linkSourceTypeHour").source_use_type("sourceTypeID"),
        MergeTableSpec::new("M6SulfurCoeff").pollutant("pollutantID"),
        MergeTableSpec::new("MeanFuelParameters")
            .pol_process_id("polProcessID")
            .fuel_type("fuelTypeID"),
        MergeTableSpec::new("mechanismName"),
        MergeTableSpec::new("metalemissionrate")
            .pol_process_id("polProcessID")
            .source_use_type("sourceTypeID")
            .fuel_type("fuelTypeID"),
        MergeTableSpec::new("methaneTHCRatio").process("processID"),
        MergeTableSpec::new("minorhapratio")
            .pol_process_id("polProcessID")
            .fuel_type("fuelTypeID"),
        MergeTableSpec::new("ModelYear"),
        MergeTableSpec::new("modelYearCutPoints"),
        MergeTableSpec::new("ModelYearGroup"),
        MergeTableSpec::new("ModelYearMapping"),
        MergeTableSpec::new("MonthGroupHour").hour("hourID"),
        MergeTableSpec::new("MonthGroupOfAnyYear"),
        // AggregationSQLGenerator needs MonthOfAnyYear to have all 12 months.
        MergeTableSpec::new("MonthofAnyYear"),
        MergeTableSpec::new("MonthVMTFraction")
            .month("monthID")
            .source_use_type("sourceTypeID"),
        MergeTableSpec::new("NONO2Ratio")
            .pol_process_id("polProcessID")
            .source_use_type("sourceTypeID")
            .fuel_type("fuelTypeID"),
        MergeTableSpec::new("NOxHumidityAdjust").fuel_type("fuelTypeID"),
        MergeTableSpec::new("offNetworkLink").source_use_type("sourceTypeID"),
        MergeTableSpec::new("OMDGPolProcessRepresented").pol_process_id("polProcessID"),
        MergeTableSpec::new("onRoadRetrofit")
            .pollutant("pollutantID")
            .process("processID")
            .source_use_type("sourceTypeID")
            .fuel_type("fuelTypeID"),
        MergeTableSpec::new("OperatingMode"),
        MergeTableSpec::new("OpModeDistribution")
            .pol_process_id("polProcessID")
            .source_use_type("sourceTypeID"),
        MergeTableSpec::new("OpModePolProcAssoc").pol_process_id("polProcessID"),
        MergeTableSpec::new("OxyThreshName"),
        MergeTableSpec::new("pahGasRatio")
            .pol_process_id("polProcessID")
            .fuel_type("fuelTypeID"),
        MergeTableSpec::new("pahParticleRatio")
            .pol_process_id("polProcessID")
            .fuel_type("fuelTypeID"),
        MergeTableSpec::new("PM10EmissionRatio")
            .pol_process_id("polProcessID")
            .source_use_type("sourceTypeID")
            .fuel_type("fuelTypeID"),
        MergeTableSpec::new("PMSpeciation")
            .pollutant("outputPollutantID")
            .process("processID")
            .source_use_type("sourceTypeID")
            .fuel_type("fuelTypeID"),
        MergeTableSpec::new("PollutantDisplayGroup"),
        MergeTableSpec::new("Pollutant").pollutant("pollutantID"),
        MergeTableSpec::new("PollutantProcessAssoc")
            .pollutant("pollutantID")
            .process("processID")
            .pol_process_id("polProcessID"),
        MergeTableSpec::new("PollutantProcessModelYear").pol_process_id("polProcessID"),
        MergeTableSpec::new("RefuelingControlTechnology")
            .process("processID")
            .source_use_type("sourceTypeID"),
        MergeTableSpec::new("RefuelingFactors").fuel_type("fuelTypeID"),
        MergeTableSpec::new("RegulatoryClass"),
        MergeTableSpec::new("region").region("regionID"),
        MergeTableSpec::new("regionCode"),
        MergeTableSpec::new("regionCounty")
            .fuel_year("fuelYearID")
            .county("countyID"),
        MergeTableSpec::new("RetrofitInputAssociations"),
        // RoadType cannot filter by roadTypeID or TotalActivityGenerator
        // will not be able to calculate SourceHours properly.
        MergeTableSpec::new("RoadType"),
        // RoadTypeDistribution cannot filter by roadTypeID for the same reason.
        MergeTableSpec::new("RoadTypeDistribution").source_use_type("sourceTypeID"),
        MergeTableSpec::new("SampleVehicleDay")
            .day("dayID")
            .source_use_type("sourceTypeID"),
        MergeTableSpec::new("SampleVehicleSoaking")
            .day("dayID")
            .source_use_type("sourceTypeID"),
        MergeTableSpec::new("SampleVehicleSoakingDay")
            .day("dayID")
            .source_use_type("sourceTypeID"),
        MergeTableSpec::new("SampleVehicleSoakingDayUsed")
            .day("dayID")
            .source_use_type("sourceTypeID"),
        MergeTableSpec::new("SampleVehicleSoakingDayBasis").day("dayID"),
        MergeTableSpec::new("SampleVehicleSoakingDayBasisUsed").day("dayID"),
        MergeTableSpec::new("SampleVehicleTrip").day("dayID"),
        // Do not filter SampleVehiclePopulation by fuel type or source type.
        MergeTableSpec::new("SampleVehiclePopulation").model_year("modelYearID"),
        MergeTableSpec::new("SCC"),
        MergeTableSpec::new("Sector"),
        MergeTableSpec::new("SHO")
            .year("yearID")
            .month("monthID")
            .source_use_type("sourceTypeID"),
        // AVFT needs all fractions so it can move vehicles between fuel
        // types; not filtered by fuelTypeID.
        MergeTableSpec::new("SizeWeightFraction"),
        MergeTableSpec::new("SoakActivityFraction")
            .month("monthID")
            .zone("zoneID"),
        MergeTableSpec::new("SourceBin").fuel_type("fuelTypeID"),
        MergeTableSpec::new("SourceBinDistribution").pol_process_id("polProcessID"),
        MergeTableSpec::new("SourceHours")
            .year("yearID")
            .month("monthID")
            .source_use_type("sourceTypeID"),
        MergeTableSpec::new("SourceTypeAge"),
        // Used in base year calculations; cannot be filtered.
        MergeTableSpec::new("SourceTypeAgeDistribution"),
        MergeTableSpec::new("SourceTypeDayVMT")
            .year("yearID")
            .month("monthID")
            .day("dayID")
            .source_use_type("sourceTypeID"),
        // SourceTypeHour cannot be filtered by hour because hotelling
        // shaping requires all hours of a day.
        MergeTableSpec::new("SourceTypeHour").source_use_type("sourceTypeID"),
        // Used in AVFTControlStrategy calculations; do not filter by sourceTypeID.
        MergeTableSpec::new("SourceTypeModelYear").model_year("modelYearID"),
        MergeTableSpec::new("SourceTypeModelYearGroup").source_use_type("sourceTypeID"),
        MergeTableSpec::new("SourceTypePolProcess")
            .pol_process_id("polProcessID")
            .source_use_type("sourceTypeID"),
        MergeTableSpec::new("SourceTypeTechAdjustment")
            .process("processID")
            .source_use_type("sourceTypeID"),
        // SourceTypeYear used in base year calculations; cannot be filtered.
        MergeTableSpec::new("SourceTypeYear"),
        MergeTableSpec::new("SourceTypeYearVMT")
            .year("yearID")
            .source_use_type("sourceTypeID"),
        MergeTableSpec::new("SourceUseType"),
        MergeTableSpec::new("sourceUseTypePhysics").source_use_type("sourceTypeID"),
        MergeTableSpec::new("Starts")
            .year("yearID")
            .month("monthID")
            .zone("zoneID"),
        MergeTableSpec::new("startsAgeAdjustment").source_use_type("sourceTypeID"),
        MergeTableSpec::new("startsPerDay")
            .day("dayID")
            .source_use_type("sourceTypeID"),
        MergeTableSpec::new("startsPerDayPerVehicle")
            .day("dayID")
            .source_use_type("sourceTypeID"),
        MergeTableSpec::new("startsHourFraction")
            .day("dayID")
            .hour("hourID")
            .source_use_type("sourceTypeID"),
        MergeTableSpec::new("startsMonthAdjust")
            .month("monthID")
            .source_use_type("sourceTypeID"),
        MergeTableSpec::new("StartsPerVehicle").source_use_type("sourceTypeID"),
        MergeTableSpec::new("State").state("stateID"),
        MergeTableSpec::new("SulfateEmissionRate")
            .pol_process_id("polProcessID")
            .fuel_type("fuelTypeID"),
        MergeTableSpec::new("SulfateFractions")
            .process("processID")
            .source_use_type("sourceTypeID")
            .fuel_type("fuelTypeID"),
        MergeTableSpec::new("SulfurBase"),
        MergeTableSpec::new("sulfurCapAmount").fuel_type("fuelTypeID"),
        MergeTableSpec::new("SulfurModelCoeff").process("processID"),
        MergeTableSpec::new("SulfurModelName"),
        MergeTableSpec::new("TankTemperatureGroup"),
        MergeTableSpec::new("TankTemperatureRise"),
        MergeTableSpec::new("TankVaporGenCoeffs"),
        MergeTableSpec::new("TemperatureAdjustment")
            .pol_process_id("polProcessID")
            .fuel_type("fuelTypeID"),
        MergeTableSpec::new("TemperatureProfileID")
            .month("monthID")
            .zone("zoneID"),
        MergeTableSpec::new("TOGSpeciationProfileName"),
        MergeTableSpec::new("totalIdleFraction")
            .month("monthID")
            .day("dayID")
            .source_use_type("sourceTypeID"),
        MergeTableSpec::new("StartTempAdjustment")
            .pol_process_id("polProcessID")
            .fuel_type("fuelTypeID"),
        MergeTableSpec::new("WeightClass"),
        // Contains "base year" / bounding values; cannot be filtered.
        MergeTableSpec::new("Year"),
        MergeTableSpec::new("Zone")
            .zone("zoneID")
            .county("countyID"),
        MergeTableSpec::new("ZoneMonthHour")
            .month("monthID")
            .zone("zoneID"),
        // ZoneRoadType cannot filter by roadTypeID for activity-calculator reasons.
        MergeTableSpec::new("ZoneRoadType").zone("zoneID"),
    ]
}

// ---------------------------------------------------------------------------
// Tests — covers the 26 asserts in InputDataManagerTest plus structural
// checks for the registry and the projection.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use moves_runspec::{
        GeographicSelection, OnroadVehicleSelection, PollutantProcessAssociation, RoadType,
        RunSpec, Timespan,
    };

    fn sample_filters() -> RunSpecFilters {
        RunSpecFilters {
            years: vec![2020],
            fuel_years: vec![2020],
            months: vec![7],
            days: vec![5, 2],
            hours: vec![6, 7, 8, 9, 10],
            link_ids: vec![],
            zone_ids: vec![],
            county_ids: vec![40001],
            state_ids: vec![40],
            region_ids: vec![100000000],
            pollutant_ids: vec![2, 3],
            process_ids: vec![1, 2],
            pol_process_ids: vec![201, 202, 301, 302],
            road_type_ids: vec![2, 3, 4, 5],
            source_type_ids: vec![21, 31, 32],
            fuel_type_ids: vec![1, 2, 5],
            include_thc_for_vmt: false,
        }
    }

    // ---------------- Java InputDataManagerTest 11 builders ----------------

    #[test]
    fn for_years_returns_in_list_with_year() {
        let f = sample_filters();
        let clause = WhereClauseBuilder::for_years(&f, "year").expect("non-empty");
        assert_eq!(clause.column(), "year");
        assert!(!clause.is_empty());
        assert_eq!(clause.to_sql(), "year IN (2020)");
    }

    #[test]
    fn for_months_returns_in_list_with_month() {
        let f = sample_filters();
        let clause = WhereClauseBuilder::for_months(&f, "month").expect("non-empty");
        assert!(!clause.is_empty());
        assert_eq!(clause.to_sql(), "month IN (7)");
    }

    #[test]
    fn for_counties_returns_in_list_with_county() {
        let f = sample_filters();
        let clause = WhereClauseBuilder::for_counties(&f, "COUNTY").expect("non-empty");
        assert!(!clause.is_empty());
        assert_eq!(clause.to_sql(), "COUNTY IN (40001)");
    }

    #[test]
    fn for_pollutants_returns_in_list_with_column_qualifier() {
        let f = sample_filters();
        let clause =
            WhereClauseBuilder::for_pollutants(&f, "POLLUTANT.PollutantID").expect("non-empty");
        assert!(!clause.is_empty());
        // Values come back deduped+sorted, but the Pollutant builder uses
        // the input order, so ordering follows the filters slice.
        assert_eq!(clause.to_sql(), "POLLUTANT.PollutantID IN (2,3)");
    }

    #[test]
    fn for_processes_returns_in_list() {
        let f = sample_filters();
        let clause = WhereClauseBuilder::for_processes(&f, "Process.ProcessID").expect("non-empty");
        assert!(!clause.is_empty());
        assert_eq!(clause.to_sql(), "Process.ProcessID IN (1,2)");
    }

    #[test]
    fn for_pollutant_process_ids_emits_negative_or_in_list() {
        let f = sample_filters();
        let clause =
            WhereClauseBuilder::for_pollutant_process_ids(&f, "PolProcessID").expect("non-empty");
        assert!(!clause.is_empty());
        // < 0 prefix is always present per Java; the explicit ids follow.
        let sql = clause.to_sql();
        assert!(sql.starts_with("PolProcessID < 0 OR PolProcessID IN ("));
        assert!(sql.contains("201"));
        assert!(sql.contains("302"));
    }

    #[test]
    fn for_days_returns_in_list() {
        let f = sample_filters();
        let clause = WhereClauseBuilder::for_days(&f, "day").expect("non-empty");
        assert!(!clause.is_empty());
    }

    #[test]
    fn for_hours_returns_in_list() {
        let f = sample_filters();
        let clause = WhereClauseBuilder::for_hours(&f, "hour").expect("non-empty");
        assert!(!clause.is_empty());
    }

    #[test]
    fn for_road_types_returns_in_list() {
        let f = sample_filters();
        let clause = WhereClauseBuilder::for_road_types(&f, "RoadTypeID").expect("non-empty");
        assert!(!clause.is_empty());
        assert_eq!(clause.to_sql(), "RoadTypeID IN (2,3,4,5)");
    }

    #[test]
    fn for_fuel_types_returns_in_list() {
        let f = sample_filters();
        let clause = WhereClauseBuilder::for_fuel_types(&f, "FuelTypeID").expect("non-empty");
        assert!(!clause.is_empty());
        assert_eq!(clause.to_sql(), "FuelTypeID IN (1,2,5)");
    }

    #[test]
    fn for_source_use_types_returns_in_list() {
        let f = sample_filters();
        let clause =
            WhereClauseBuilder::for_source_use_types(&f, "SourceTypeID").expect("non-empty");
        assert!(!clause.is_empty());
        assert_eq!(clause.to_sql(), "SourceTypeID IN (21,31,32)");
    }

    // ------------------ merge() test — the 12th method ------------------

    #[test]
    fn plan_populates_dayofweek_filter_for_days() {
        // testMerge in Java verifies that after merge(), DayOfAnyWeek has rows.
        // The Rust analogue: a merge plan over the default registry should
        // contain a DayOfAnyWeek entry with a day-column filter populated.
        let f = sample_filters();
        let plan = InputDataManager::plan(&f, &default_tables());
        let entry = plan.find("DayOfAnyWeek").expect("DayOfAnyWeek in plan");
        assert!(!entry.clauses.is_empty());
        assert_eq!(entry.clauses[0].column(), "dayID");
    }

    // ----------------- Additional structural / regression tests -----------------

    #[test]
    fn empty_filters_drop_builders_to_none() {
        let f = RunSpecFilters::default();
        assert!(WhereClauseBuilder::for_years(&f, "yearID").is_none());
        assert!(WhereClauseBuilder::for_months(&f, "monthID").is_none());
        assert!(WhereClauseBuilder::for_road_types(&f, "roadTypeID").is_none());
        assert!(WhereClauseBuilder::for_pollutants(&f, "pollutantID").is_none());
        assert!(WhereClauseBuilder::for_pollutant_process_ids(&f, "polProcessID").is_none());
    }

    #[test]
    fn model_years_render_overlapping_ranges() {
        let f = RunSpecFilters {
            years: vec![2010, 2012],
            ..Default::default()
        };
        let clause = WhereClauseBuilder::for_model_years(&f, "modelYearID").expect("non-empty");
        let sql = clause.to_sql();
        assert!(sql.contains("(modelYearID<=2010 and modelYearID>=1970)"));
        assert!(sql.contains("(modelYearID<=2012 and modelYearID>=1972)"));
        assert!(sql.contains(" or "));
    }

    #[test]
    fn thc_prefix_added_when_outputting_vmt_without_distance() {
        let f = RunSpecFilters {
            pollutant_ids: vec![2, 3],
            process_ids: vec![1, 2],
            pol_process_ids: vec![201, 301, 202, 302],
            include_thc_for_vmt: true,
            ..Default::default()
        };
        let pol = WhereClauseBuilder::for_pollutants(&f, "pollutantID").expect("non-empty");
        // THC (id 1) prepended.
        assert_eq!(pol.to_sql(), "pollutantID IN (1,2,3)");
        let proc = WhereClauseBuilder::for_processes(&f, "processID").expect("non-empty");
        // process 1 is already present, so push_unique is a no-op.
        assert_eq!(proc.to_sql(), "processID IN (1,2)");
        let pp =
            WhereClauseBuilder::for_pollutant_process_ids(&f, "polProcessID").expect("non-empty");
        let sql = pp.to_sql();
        // 101 = THC × Running Exhaust is included.
        assert!(sql.contains("101"));
    }

    #[test]
    fn pol_process_ids_expand_118_to_120() {
        let f = RunSpecFilters {
            pol_process_ids: vec![11801], // pollutant 118 (NonECPM), process 1
            ..Default::default()
        };
        let clause =
            WhereClauseBuilder::for_pollutant_process_ids(&f, "polProcessID").expect("non-empty");
        let sql = clause.to_sql();
        // Source id present.
        assert!(sql.contains("11801"));
        // Expansion id (120*100 + 1) present.
        assert!(sql.contains("12001"));
    }

    // ------------------------ RunSpecFilters projection ------------------------

    #[test]
    fn from_runspec_extracts_timespan_dimensions() {
        let spec = RunSpec {
            timespan: Timespan {
                years: vec![2020, 2020], // duplicate filtered out
                months: vec![6, 7, 7],
                days: vec![5],
                begin_hour: Some(6),
                end_hour: Some(10),
                aggregate_by: None,
            },
            ..Default::default()
        };
        let filters = RunSpecFilters::from_runspec(&spec);
        assert_eq!(filters.years, vec![2020]);
        assert_eq!(filters.months, vec![6, 7]);
        assert_eq!(filters.days, vec![5]);
        assert_eq!(filters.hours, vec![6, 7, 8, 9, 10]);
    }

    #[test]
    fn from_runspec_extracts_geography_state_county_zone() {
        let spec = RunSpec {
            geographic_selections: vec![
                GeographicSelection {
                    kind: GeoKind::County,
                    key: 40_001,
                    description: "Adair, OK".into(),
                },
                GeographicSelection {
                    kind: GeoKind::State,
                    key: 6,
                    description: "California".into(),
                },
            ],
            ..Default::default()
        };
        let filters = RunSpecFilters::from_runspec(&spec);
        assert_eq!(filters.county_ids, vec![40_001]);
        // County 40001 implies state 40; explicit state 6 also captured.
        assert!(filters.state_ids.contains(&6));
        assert!(filters.state_ids.contains(&40));
    }

    #[test]
    fn from_runspec_extracts_vehicle_and_road_types() {
        let spec = RunSpec {
            onroad_vehicle_selections: vec![
                OnroadVehicleSelection {
                    fuel_type_id: 1,
                    fuel_type_name: "Gasoline".into(),
                    source_type_id: 21,
                    source_type_name: "Passenger Car".into(),
                },
                OnroadVehicleSelection {
                    fuel_type_id: 2,
                    fuel_type_name: "Diesel".into(),
                    source_type_id: 31,
                    source_type_name: "Passenger Truck".into(),
                },
            ],
            road_types: vec![RoadType {
                road_type_id: 4,
                road_type_name: "Urban Restricted".into(),
                model_combination: None,
            }],
            ..Default::default()
        };
        let filters = RunSpecFilters::from_runspec(&spec);
        assert_eq!(filters.fuel_type_ids, vec![1, 2]);
        assert_eq!(filters.source_type_ids, vec![21, 31]);
        assert_eq!(filters.road_type_ids, vec![4]);
    }

    #[test]
    fn from_runspec_extracts_pollutant_process_pairs() {
        let spec = RunSpec {
            pollutant_process_associations: vec![
                PollutantProcessAssociation {
                    pollutant_id: 2,
                    pollutant_name: "CO".into(),
                    process_id: 1,
                    process_name: "Running Exhaust".into(),
                },
                PollutantProcessAssociation {
                    pollutant_id: 3,
                    pollutant_name: "NOx".into(),
                    process_id: 1,
                    process_name: "Running Exhaust".into(),
                },
            ],
            ..Default::default()
        };
        let filters = RunSpecFilters::from_runspec(&spec);
        assert_eq!(filters.pollutant_ids, vec![2, 3]);
        assert_eq!(filters.process_ids, vec![1]);
        assert_eq!(filters.pol_process_ids, vec![201, 301]);
    }

    #[test]
    fn from_runspec_vmt_flag_set_only_when_distance_missing() {
        let mut spec = RunSpec {
            output_vmt_data: true,
            pollutant_process_associations: vec![PollutantProcessAssociation {
                pollutant_id: 2,
                pollutant_name: "CO".into(),
                process_id: 1,
                process_name: "Running Exhaust".into(),
            }],
            ..Default::default()
        };
        let filters = RunSpecFilters::from_runspec(&spec);
        assert!(filters.include_thc_for_vmt);

        // Now add a distance pollutant (id == 1) and verify the flag clears.
        spec.pollutant_process_associations
            .push(PollutantProcessAssociation {
                pollutant_id: 1,
                pollutant_name: "Total Gaseous Hydrocarbons".into(),
                process_id: 1,
                process_name: "Running Exhaust".into(),
            });
        let filters = RunSpecFilters::from_runspec(&spec);
        assert!(!filters.include_thc_for_vmt);
    }

    // ------------------------ MergePlan / registry shape ------------------------

    #[test]
    fn default_tables_registry_is_substantial() {
        // The Java registry has ~150 entries (counting conditional ones).
        // Sanity check that our port carries a comparable count — we
        // commit to >=120 to leave room for the conditional Java entries
        // we represent unconditionally and the few NonRoad tables that
        // live in a separate (NotYetPorted) registry.
        let tables = default_tables();
        assert!(
            tables.len() >= 120,
            "expected >=120 tables, got {}",
            tables.len()
        );
    }

    #[test]
    fn default_tables_registry_names_are_unique() {
        let tables = default_tables();
        let mut seen = std::collections::HashSet::new();
        for t in &tables {
            assert!(
                seen.insert(t.table_name),
                "duplicate table name in registry: {}",
                t.table_name
            );
        }
    }

    #[test]
    fn plan_for_sample_filters_has_clauses_on_filterable_tables() {
        let f = sample_filters();
        let plan = InputDataManager::plan(&f, &default_tables());
        // Pick a few tables with known dimensions and verify they have clauses.
        let day_of_week = plan.find("DayOfAnyWeek").unwrap();
        assert_eq!(day_of_week.clauses.len(), 1);
        let county_year = plan.find("CountyYear").unwrap();
        assert!(county_year.clauses.iter().any(|c| c.column() == "yearID"));
        let imc = plan.find("IMCoverage").unwrap();
        assert!(imc.clauses.iter().any(|c| c.column() == "yearID"));
        assert!(imc.clauses.iter().any(|c| c.column() == "stateID"));
    }

    #[test]
    fn plan_for_unfilterable_table_has_no_clauses() {
        // AgeCategory has no filter columns — its plan entry should be
        // empty regardless of the RunSpec.
        let f = sample_filters();
        let plan = InputDataManager::plan(&f, &default_tables());
        let age = plan.find("AgeCategory").unwrap();
        assert!(age.clauses.is_empty());
    }

    #[test]
    fn plan_skips_clauses_when_filter_is_empty() {
        // RunSpec with months only: tables filtered on year, day, etc.
        // should have *no* corresponding clause for those dimensions —
        // only month clauses make it through.
        let f = RunSpecFilters {
            months: vec![6],
            ..Default::default()
        };
        let plan = InputDataManager::plan(&f, &default_tables());
        let month_vmt = plan.find("MonthVMTFraction").unwrap();
        assert_eq!(month_vmt.clauses.len(), 1);
        assert_eq!(month_vmt.clauses[0].column(), "monthID");
    }

    #[test]
    fn input_data_manager_is_default_constructible() {
        let _m = InputDataManager;
    }
}
