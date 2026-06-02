//! Port of `AverageSpeedOperatingModeDistributionGenerator.java`
//! (`gov.epa.otaq.moves.master.implementation.ghg`) — the generator that
//! builds tire-wear `RatesOpModeDistribution` records from average-speed
//! information.
//!
//! the average-speed-binned variant of
//! the drive-schedule-derived `OperatingModeDistributionGenerator`
//!It runs when the RunSpec supplies `AvgSpeedDistribution`
//! inputs rather than link drive schedules.
//!
//! # What this generator produces
//!
//! Tire wear is the one running-emission process whose operating mode
//! depends only on average speed — there is no VSP / drive-cycle physics.
//! This generator therefore builds the `RatesOpModeDistribution`
//! operating-mode-fraction rows for the **Tirewear** process (process id
//! 10, `polProcessID` 11710 — "Primary PM2.5 - Tirewear Particulate").
//! Although the Java class comment also names Running Exhaust, the live
//! code only ever handles Tirewear: `executeLoop` logs
//! `"AvgSpeedOMDG called for unknown process"` and returns for any other
//! process, and the chain DAG
//! (`characterization/calculator-chains/calculator-dag.json`) records a
//! single subscription — Tirewear, `PROCESS` granularity, `GENERATOR`
//! priority.
//!
//! # Scope of this port — the live (`DO_RATES_FIRST`) paths
//!
//! `CompilationFlags.DO_RATES_FIRST` is `true` in the pinned EPA source, so
//! `executeLoop` dispatches to exactly two computations, by RunSpec domain:
//!
//! * **Non-Project domain** → `calculateRatesFirstOpModeFractions`, ported
//! as [`rates_first_op_mode_fractions`].
//! * **Project domain** → `calculateTireProjectOpModeFractions` (with the
//! speed→op-mode `CASE` clause built by `buildOpModeClause`), ported as
//! [`project_op_mode_fractions`] / [`assign_tirewear_op_mode`].
//!
//! Two branches are **dead code** in the pinned source and are deliberately
//! not ported:
//!
//! * `calculateOpModeFractions` — the non-rates-first `OpModeDistribution`
//! path; unreachable because `DO_RATES_FIRST` is `true`. (It is the only
//! path that reads `avgSpeedDistribution`, so that table does not appear
//! in [`Generator::input_tables`].)
//! * the `USE_2010B_TIREWEAR_RATE_METHOD` branch inside
//! `calculateRatesFirstOpModeFractions` — the flag is `false`, and the
//! EPA comment notes the method "is known to make incorrect rates".
//!
//! # The live algorithm
//!
//! Tire-wear operating-mode "distributions" are degenerate: every emitted
//! row carries `opModeFraction = 1`. A vehicle in a given speed context is
//! assigned to exactly one tire-wear operating mode.
//!
//! * **Non-Project** ([`rates_first_op_mode_fractions`]) — every speed bin
//! already carries its tire-wear operating mode in
//! `avgSpeedBin.opModeIDTirewear`. The generator cross-joins the speed
//! bins with the RunSpec's selected source types, road types and
//! hour/days and emits one `opModeFraction = 1` row each, carrying the
//! bin's `avgBinSpeed`.
//! * **Project** ([`project_op_mode_fractions`]) — a project link has a
//! single `linkAvgSpeed`, so the generator assigns the whole link one
//! operating mode via the [`assign_tirewear_op_mode`] `CASE` clause and
//! cross-joins it with the selected source types and hour/days
//! (`avgSpeedBinID = 0`, `avgBinSpeed = linkAvgSpeed`).
//!
//! Both Java statements are `INSERT IGNORE`; the private `insert_ignore`
//! helper reproduces the primary-key de-duplication and additionally sorts
//! the output for a deterministic, testable row order. Primary key, from
//! `database/CreateExecutionRates.sql`: `(sourceTypeID, polProcessID,
//! roadTypeID, hourDayID, opModeID, avgSpeedBinID)`.
//!
//! # Numeric fidelity
//!
//! `avgSpeedBin.avgBinSpeed`, `link.linkAvgSpeed` and
//! `operatingMode.speedLower` / `speedUpper` are all `FLOAT` (32-bit)
//! columns; this port carries them — and the `RatesOpModeDistribution`
//! output speed column — as `f64`. Widening `f32 → f64` is lossless, so the
//! speed-range comparisons in [`assign_tirewear_op_mode`] reproduce the
//! MySQL `CASE` clause exactly: MySQL itself promotes the `FLOAT` column
//! and the decimal literals to `DOUBLE` before comparing. The only stored
//! non-key value — `opModeFraction = 1` — is exact in both widths.
//!
//! # Data plane
//!
//! [`Generator::execute`] receives a [`CalculatorContext`] whose
//! `ExecutionTables` / `ScratchNamespace` are Phase-2 placeholders until
//! the `DataFrameStore` lands (), so `execute` cannot
//! yet read the input tables nor write `RatesOpModeDistribution`. The
//! numerically faithful algorithm is fully ported and unit-tested in the
//! free functions [`rates_first_op_mode_fractions`],
//! [`project_op_mode_fractions`] and [`assign_tirewear_op_mode`]; once the
//! data plane exists, `execute` projects the input views from
//! `ctx.tables()`, dispatches on RunSpec domain, and writes the rows to the
//! scratch namespace.

use std::cmp::Ordering;
use std::collections::HashSet;

use moves_calculator_info::{Granularity, Priority};
use moves_data::{
    PolProcessId, PollutantId, PollutantProcessAssociation, ProcessId, RoadTypeId, SourceTypeId,
};
use moves_framework::{
    CalculatorContext, CalculatorOutput, CalculatorSubscription, DataFrameStoreTyped, Error,
    Generator, TableRow,
};
use polars::prelude::{DataFrame, DataType, NamedFrom, PolarsResult, Schema, Series};

/// Tirewear — process id 10. The only process this generator handles;
/// `executeLoop` errors out for any other process.
const TIREWEAR_PROCESS: ProcessId = ProcessId(10);

/// `polProcessID` 11710 — pollutant 117 ("Primary PM2.5 - Tirewear
/// Particulate") × process 10 (Tirewear). The Java hard-codes
/// `polProcessIDs = "11710"` for the tire-wear process.
const TIREWEAR_POL_PROCESS: PolProcessId = PolProcessId(11710);

/// Speed (mph) below which a link is assigned the idle tire-wear operating
/// mode — the `when linkAvgSpeed < 0.1` clause of `buildOpModeClause`.
/// `0.1` is the `f64` nearest one-tenth, matching MySQL's promotion of the
/// `0.1` literal to `DOUBLE` for comparison against the `FLOAT` column.
const IDLE_SPEED_THRESHOLD: f64 = 0.1;

/// Idle tire-wear operating mode — `buildOpModeClause`'s
/// `when linkAvgSpeed < 0.1 then 400`.
const IDLE_TIREWEAR_OP_MODE: i16 = 400;

/// Operating mode for a link speed that matches no tire-wear speed range/// the `else -1` of the assignment `CASE`.
const UNASSIGNED_OP_MODE: i16 = -1;

/// Operating mode assigned when a speed bin's nullable `opModeIDTirewear`
/// is `NULL`. The Java `INSERT IGNORE ... SELECT` writes the `NULL` into
/// the `NOT NULL` `opModeID` column; MySQL `INSERT IGNORE` downgrades the
/// resulting `ER_BAD_NULL_ERROR` to a warning and stores the column's
/// implicit numeric default, `0`. (The MOVES default DB assigns every
/// speed bin a tire-wear operating mode, so this is a schema-permitted
/// edge case rather than an observed one.)
const NULL_OP_MODE_DEFAULT: i16 = 0;

/// `avgSpeedBinID` recorded for project-domain rows — the Java
/// `0 as avgSpeedBinID`: a project link is not speed-binned.
const PROJECT_AVG_SPEED_BIN_ID: i16 = 0;

/// Lowest operating-mode id `buildOpModeClause` scans for tire-wear speed
/// ranges (`opModeID >= 401`).
const TIREWEAR_OP_MODE_MIN: i16 = 401;

/// Highest operating-mode id `buildOpModeClause` scans for tire-wear speed
/// ranges (`opModeID <= 499`).
const TIREWEAR_OP_MODE_MAX: i16 = 499;

/// The case-insensitive `opModeName` prefix `buildOpModeClause` filters on
/// (`opModeName LIKE 'tirewear%'`).
const TIREWEAR_NAME_PREFIX: &str = "tirewear";

/// One `RatesOpModeDistribution` row produced by this generator.
///
/// Models the eight columns the Java `INSERT IGNORE` statements populate,
/// in `INSERT` column order. The execution-database table also has
/// `opModeFractionCV` and `avgSpeedFraction`; this generator never sets
/// them, so they take their schema defaults and are not modeled here.
///
/// `avgBinSpeed` is the `FLOAT` `avgSpeedBin.avgBinSpeed` (non-Project) or
/// `link.linkAvgSpeed` (Project); it is held as `f64` for consistency with
/// the rest of the port — see the module's *Numeric fidelity* section.
/// `opModeFraction` is always `1.0`, exact in both `f32` and `f64`.
///
/// Primary key (the `INSERT IGNORE` de-duplication key, from
/// `database/CreateExecutionRates.sql`): `(sourceTypeID, polProcessID,
/// roadTypeID, hourDayID, opModeID, avgSpeedBinID)`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RatesOpModeDistributionRow {
 /// `sourceTypeID` — a RunSpec-selected source type.
    pub source_type_id: SourceTypeId,
 /// `roadTypeID` — a RunSpec-selected road type (non-Project) or the
 /// project link's road type (Project).
    pub road_type_id: RoadTypeId,
 /// `avgSpeedBinID` — the speed bin (non-Project) or `0` (Project).
    pub avg_speed_bin_id: i16,
 /// `hourDayID` — a RunSpec-selected hour/day combination.
    pub hour_day_id: i16,
 /// `polProcessID` — always `TIREWEAR_POL_PROCESS` (11710).
    pub pol_process_id: PolProcessId,
 /// `opModeID` — the tire-wear operating mode this fraction applies to.
    pub op_mode_id: i16,
 /// `opModeFraction` — always `1.0`: tire-wear op-mode distributions are
 /// degenerate (one operating mode per speed context).
    pub op_mode_fraction: f64,
 /// `avgBinSpeed` — the bin's average speed (non-Project) or the link's
 /// average speed (Project).
    pub avg_bin_speed: f64,
}

/// Primary-key tuple of `RatesOpModeDistribution` — the columns the Java
/// `INSERT IGNORE` statements de-duplicate on, in primary-key order.
type RowKey = (SourceTypeId, PolProcessId, RoadTypeId, i16, i16, i16);

impl RatesOpModeDistributionRow {
 /// The primary-key projection used both to de-duplicate `INSERT IGNORE`
 /// collisions and to give the output a deterministic order.
    fn key(&self) -> RowKey {
        (
            self.source_type_id,
            self.pol_process_id,
            self.road_type_id,
            self.hour_day_id,
            self.op_mode_id,
            self.avg_speed_bin_id,
        )
    }
}

/// Apply MySQL `INSERT IGNORE` semantics to a candidate row list: keep the
/// first row for each primary key, drop later collisions, and return the
/// result in deterministic primary-key order.
fn insert_ignore(rows: Vec<RatesOpModeDistributionRow>) -> Vec<RatesOpModeDistributionRow> {
    let mut seen: HashSet<RowKey> = HashSet::with_capacity(rows.len());
    let mut out: Vec<RatesOpModeDistributionRow> =
        rows.into_iter().filter(|r| seen.insert(r.key())).collect();
    out.sort_unstable_by_key(RatesOpModeDistributionRow::key);
    out
}

/// A projected `avgSpeedBin` row — the speed bins and their pre-assigned
/// tire-wear operating modes.
///
/// Only the three columns the non-Project path reads are modeled; the
/// table also has `avgSpeedBinDesc` and `opModeIDRunning`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AvgSpeedBin {
 /// `avgSpeedBinID`.
    pub avg_speed_bin_id: i16,
 /// `avgBinSpeed` — the bin's representative average speed (`FLOAT`).
    pub avg_bin_speed: f64,
 /// `opModeIDTirewear` — the tire-wear operating mode for this bin. The
 /// column is `NULL`-able; a `None` is written into the `NOT NULL`
 /// `opModeID` column as `NULL_OP_MODE_DEFAULT` by `INSERT IGNORE`.
    pub op_mode_id_tirewear: Option<i16>,
}

/// A projected `operatingMode` row — the columns `buildOpModeClause` reads
/// to build the project-domain speed→op-mode `CASE` clause.
///
/// The table has more columns (`VSPLower`/`VSPUpper`, brake rates, soak
/// times); only the four the tire-wear `CASE` clause needs are modeled.
#[derive(Debug, Clone, PartialEq)]
pub struct OperatingMode {
 /// `opModeID`.
    pub op_mode_id: i16,
 /// `opModeName` — filtered with `LIKE 'tirewear%'` (case-insensitive).
    pub op_mode_name: String,
 /// `speedLower` — inclusive lower speed bound (`FLOAT`, `NULL`-able);
 /// `None` means the range is open below.
    pub speed_lower: Option<f64>,
 /// `speedUpper` — exclusive upper speed bound (`FLOAT`, `NULL`-able);
 /// `None` means the range is open above.
    pub speed_upper: Option<f64>,
}

/// A projected `link` row — a single project-domain link.
///
/// `calculateTireProjectOpModeFractions` processes one link per
/// `executeLoop`, reading only its road type and average speed.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Link {
 /// `roadTypeID` — copied straight onto every emitted row.
    pub road_type_id: RoadTypeId,
 /// `linkAvgSpeed` — the link's average speed (`FLOAT`); drives the
 /// op-mode assignment and is copied to `avgBinSpeed`.
    pub link_avg_speed: f64,
}

/// Projected input tables for [`rates_first_op_mode_fractions`] — the
/// non-Project (`PROCESS`-granularity) rates-first path.
///
/// Each field is the Rust analogue of one table the Java `SELECT`
/// references. Once the data plane lands, [`Generator::execute`]
/// builds this view from `ctx.tables()`.
#[derive(Debug, Clone, Copy)]
pub struct RatesFirstInputs<'a> {
 /// `avgSpeedBin` — every speed bin and its tire-wear operating mode.
    pub avg_speed_bins: &'a [AvgSpeedBin],
 /// `runSpecSourceType.sourceTypeID` — the RunSpec's source types.
    pub run_spec_source_type: &'a [SourceTypeId],
 /// `runSpecRoadType.roadTypeID` — the RunSpec's road types.
    pub run_spec_road_type: &'a [RoadTypeId],
 /// `runSpecHourDay.hourDayID` — the RunSpec's hour/day combinations.
    pub run_spec_hour_day: &'a [i16],
 /// `pollutantProcessAssoc` — the modeled `(pollutant, process)` pairs.
 /// The Java `where polProcessID in (11710)` filter means this path
 /// emits nothing unless the tire-wear pol-process is modeled.
    pub pollutant_process_assoc: &'a [PollutantProcessAssociation],
}

/// Projected input tables for [`project_op_mode_fractions`] — the
/// Project-domain (`LINK`-granularity) path.
#[derive(Debug, Clone, Copy)]
pub struct ProjectInputs<'a> {
 /// The single `link` being processed this `executeLoop`.
    pub link: Link,
 /// `runSpecSourceType.sourceTypeID` — the RunSpec's source types.
    pub run_spec_source_type: &'a [SourceTypeId],
 /// `runSpecHourDay.hourDayID` — the RunSpec's hour/day combinations.
    pub run_spec_hour_day: &'a [i16],
 /// `operatingMode` — the rows `buildOpModeClause` scans for tire-wear
 /// speed ranges.
    pub operating_modes: &'a [OperatingMode],
}

/// Whether `name` satisfies the Java `opModeName LIKE 'tirewear%'` filter.
///
/// The MOVES default-DB collation is `utf8mb4_unicode_ci`, so `LIKE` is
/// case-insensitive; this matches the `TIREWEAR_NAME_PREFIX` ignoring
/// ASCII case. `str::get` keeps the prefix slice on a char boundary.
fn is_tirewear_name(name: &str) -> bool {
    name.get(..TIREWEAR_NAME_PREFIX.len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(TIREWEAR_NAME_PREFIX))
}

/// Order two optional speeds with MySQL `ORDER BY ... ASC` semantics:
/// `NULL` (`None`) sorts before any value.
fn cmp_optional_speed(a: Option<f64>, b: Option<f64>) -> Ordering {
    match (a, b) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(x), Some(y)) => x.total_cmp(&y),
    }
}

/// Assign a tire-wear operating mode to a link average speed — the port of
/// `buildOpModeClause` plus the `CASE ... else -1 end` that consumes it.
///
/// The Java builds, and MySQL evaluates top-to-bottom, this `CASE`:
///
/// * `when linkAvgSpeed < 0.1 then 400` — the idle clause, checked first;
/// * one clause per tire-wear operating mode (id `401..=499`, name
/// `LIKE 'tirewear%'`), ordered by `speedLower`, of the form
/// `when (speedLower <= linkAvgSpeed and linkAvgSpeed < speedUpper)`;
/// * `else -1`.
///
/// A SQL `CASE` returns its first matching `WHEN`, so this returns the
/// first tire-wear operating mode (in `speedLower` order) whose half-open
/// `[speedLower, speedUpper)` range contains `link_avg_speed`. Either bound
/// may be `NULL`/`None`, leaving that side of the range open. Returns
/// `IDLE_TIREWEAR_OP_MODE` for speeds below `IDLE_SPEED_THRESHOLD` and
/// `UNASSIGNED_OP_MODE` (`-1`) when no range matches.
#[must_use]
pub fn assign_tirewear_op_mode(link_avg_speed: f64, operating_modes: &[OperatingMode]) -> i16 {
 // First CASE clause: `when linkAvgSpeed < 0.1 then 400`.
    if link_avg_speed < IDLE_SPEED_THRESHOLD {
        return IDLE_TIREWEAR_OP_MODE;
    }
 // The remaining clauses, in `ORDER BY speedLower` order. Op-mode id is
 // a stable tie-breaker so equal-`speedLower` rows resolve the same way
 // every run (MySQL leaves that order unspecified).
    let mut tirewear: Vec<&OperatingMode> = operating_modes
        .iter()
        .filter(|om| {
            (TIREWEAR_OP_MODE_MIN..=TIREWEAR_OP_MODE_MAX).contains(&om.op_mode_id)
                && is_tirewear_name(&om.op_mode_name)
        })
        .collect();
    tirewear.sort_by(|a, b| {
        cmp_optional_speed(a.speed_lower, b.speed_lower).then(a.op_mode_id.cmp(&b.op_mode_id))
    });
    for om in tirewear {
        let above_lower = om.speed_lower.map_or(true, |lower| lower <= link_avg_speed);
        let below_upper = om.speed_upper.map_or(true, |upper| link_avg_speed < upper);
        if above_lower && below_upper {
            return om.op_mode_id;
        }
    }
 // `else -1`.
    UNASSIGNED_OP_MODE
}

/// Port of `calculateRatesFirstOpModeFractions` (standard branch) — the
/// non-Project, `DO_RATES_FIRST` path.
///
/// The Java step-100 statement is one `INSERT IGNORE` whose `SELECT`
/// cross-joins `avgSpeedBin`, `runSpecSourceType`, `runSpecRoadType`,
/// `runSpecHourDay` and `pollutantProcessAssoc` (filtered to
/// `polProcessID IN (11710)`), emitting one `opModeFraction = 1` row per
/// `(bin, source type, road type, hour/day)` with the bin's
/// `opModeIDTirewear` and `avgBinSpeed`.
///
/// The `pollutantProcessAssoc` join is a gate: if the run does not model
/// the tire-wear pol-process the cross join is empty and the result is
/// empty. A speed bin whose `opModeIDTirewear` is `None` contributes a row
/// with `op_mode_id` `NULL_OP_MODE_DEFAULT` — see that constant.
///
/// (The dead `USE_2010B_TIREWEAR_RATE_METHOD` branch — the
/// `avgSpeedDistribution`-weighted variant — is not ported; see the
/// module docs.)
#[must_use]
pub fn rates_first_op_mode_fractions(
    inputs: &RatesFirstInputs<'_>,
) -> Vec<RatesOpModeDistributionRow> {
 // `from ... pollutantProcessAssoc ppa where polProcessID in (11710)`:
 // an empty filtered join emits nothing.
    let tirewear_modeled = inputs
        .pollutant_process_assoc
        .iter()
        .any(|ppa| ppa.polproc_id() == TIREWEAR_POL_PROCESS);
    if !tirewear_modeled {
        return Vec::new();
    }

    let capacity = inputs.avg_speed_bins.len()
 * inputs.run_spec_source_type.len()
 * inputs.run_spec_road_type.len()
 * inputs.run_spec_hour_day.len();
    let mut rows: Vec<RatesOpModeDistributionRow> = Vec::with_capacity(capacity);
    for bin in inputs.avg_speed_bins {
        let op_mode_id = bin.op_mode_id_tirewear.unwrap_or(NULL_OP_MODE_DEFAULT);
        for &source_type_id in inputs.run_spec_source_type {
            for &road_type_id in inputs.run_spec_road_type {
                for &hour_day_id in inputs.run_spec_hour_day {
                    rows.push(RatesOpModeDistributionRow {
                        source_type_id,
                        road_type_id,
                        avg_speed_bin_id: bin.avg_speed_bin_id,
                        hour_day_id,
                        pol_process_id: TIREWEAR_POL_PROCESS,
                        op_mode_id,
                        op_mode_fraction: 1.0,
                        avg_bin_speed: bin.avg_bin_speed,
                    });
                }
            }
        }
    }
    insert_ignore(rows)
}

/// Port of `calculateTireProjectOpModeFractions` (the `DO_RATES_FIRST`
/// branch) — the Project-domain path, for one link.
///
/// The Java method runs once per project link and issues two statements:
///
/// * **step 010** `delete from RatesOpModeDistribution where
/// polProcessID = 11710` — clears every prior tire-wear row;
/// * **step 020** an `INSERT IGNORE` whose `SELECT` cross-joins the single
/// `link` with `RunSpecSourceType` and `RunSpecHourDay`, assigning the
/// link one operating mode via the [`assign_tirewear_op_mode`] `CASE`
/// clause (`avgSpeedBinID = 0`, `opModeFraction = 1`,
/// `avgBinSpeed = linkAvgSpeed`).
///
/// Because step 010 wipes the table each link iteration,
/// `RatesOpModeDistribution` holds only the *current* link's tire-wear
/// distribution — consumed by that link's rate calculators before the next
/// link overwrites it. This function returns one link's complete row set;
/// the `execute` writes it as the full (replacing) table content.
#[must_use]
pub fn project_op_mode_fractions(inputs: &ProjectInputs<'_>) -> Vec<RatesOpModeDistributionRow> {
    let op_mode_id = assign_tirewear_op_mode(inputs.link.link_avg_speed, inputs.operating_modes);

    let capacity = inputs.run_spec_source_type.len() * inputs.run_spec_hour_day.len();
    let mut rows: Vec<RatesOpModeDistributionRow> = Vec::with_capacity(capacity);
    for &source_type_id in inputs.run_spec_source_type {
        for &hour_day_id in inputs.run_spec_hour_day {
            rows.push(RatesOpModeDistributionRow {
                source_type_id,
                road_type_id: inputs.link.road_type_id,
                avg_speed_bin_id: PROJECT_AVG_SPEED_BIN_ID,
                hour_day_id,
                pol_process_id: TIREWEAR_POL_PROCESS,
                op_mode_id,
                op_mode_fraction: 1.0,
                avg_bin_speed: inputs.link.link_avg_speed,
            });
        }
    }
    insert_ignore(rows)
}

// ============================================================================
// Data-plane wiring
// ============================================================================

/// Build a [`Error::RowExtraction`] for a missing/bad cell in an input table.
fn row_err(table: &'static str, row: usize, column: &'static str, msg: String) -> Error {
    Error::RowExtraction {
        table: table.into(),
        row,
        column: column.into(),
        message: msg,
    }
}

// ---- TableRow for input tables ----

/// A row projected from `avgSpeedBin` for this generator's use.
impl TableRow for AvgSpeedBin {
    fn table_name() -> &'static str {
        "avgSpeedBin"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("avgSpeedBinID".into(), DataType::Int32),
            ("avgBinSpeed".into(), DataType::Float64),
            ("opModeIDTirewear".into(), DataType::Int32),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "avgSpeedBinID".into(),
                    rows.iter()
                        .map(|r| r.avg_speed_bin_id as i32)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "avgBinSpeed".into(),
                    rows.iter().map(|r| r.avg_bin_speed).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "opModeIDTirewear".into(),
                    rows.iter()
                        .map(|r| r.op_mode_id_tirewear.map(|v| v as i32))
                        .collect::<Vec<Option<i32>>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "avgSpeedBin";
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
        let avg_speed_bin_id = get_i32("avgSpeedBinID")?;
        let avg_bin_speed = get_f64("avgBinSpeed")?;
        let op_mode_id_tirewear = get_i32("opModeIDTirewear")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(AvgSpeedBin {
                    avg_speed_bin_id: avg_speed_bin_id
                        .get(i)
                        .ok_or_else(|| null("avgSpeedBinID"))?
                        as i16,
                    avg_bin_speed: avg_bin_speed.get(i).ok_or_else(|| null("avgBinSpeed"))?,
                    op_mode_id_tirewear: op_mode_id_tirewear.get(i).map(|v| v as i16),
                })
            })
            .collect()
    }
}

/// A row projected from `operatingMode` for this generator's use.
impl TableRow for OperatingMode {
    fn table_name() -> &'static str {
        "operatingMode"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("opModeID".into(), DataType::Int32),
            ("opModeName".into(), DataType::String),
            ("speedLower".into(), DataType::Float64),
            ("speedUpper".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "opModeID".into(),
                    rows.iter()
                        .map(|r| r.op_mode_id as i32)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "opModeName".into(),
                    rows.iter()
                        .map(|r| r.op_mode_name.clone())
                        .collect::<Vec<String>>(),
                )
                .into(),
                Series::new(
                    "speedLower".into(),
                    rows.iter()
                        .map(|r| r.speed_lower)
                        .collect::<Vec<Option<f64>>>(),
                )
                .into(),
                Series::new(
                    "speedUpper".into(),
                    rows.iter()
                        .map(|r| r.speed_upper)
                        .collect::<Vec<Option<f64>>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "operatingMode";
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
        let op_mode_id = get_i32("opModeID")?;
        let op_mode_name_col = df
            .column("opModeName")
            .map_err(|e| row_err(t, 0, "opModeName", e.to_string()))?
            .str()
            .map_err(|e| row_err(t, 0, "opModeName", e.to_string()))?;
        let speed_lower = get_f64("speedLower")?;
        let speed_upper = get_f64("speedUpper")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(OperatingMode {
                    op_mode_id: op_mode_id.get(i).ok_or_else(|| null("opModeID"))? as i16,
                    op_mode_name: op_mode_name_col
                        .get(i)
                        .ok_or_else(|| null("opModeName"))?
                        .to_owned(),
                    speed_lower: speed_lower.get(i),
                    speed_upper: speed_upper.get(i),
                })
            })
            .collect()
    }
}

/// A row projected from `link` for this generator's use.
impl TableRow for Link {
    fn table_name() -> &'static str {
        "link"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("roadTypeID".into(), DataType::Int32),
            ("linkAvgSpeed".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "roadTypeID".into(),
                    rows.iter()
                        .map(|r| r.road_type_id.0 as i32)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "linkAvgSpeed".into(),
                    rows.iter().map(|r| r.link_avg_speed).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "link";
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
        let road_type_id = get_i32("roadTypeID")?;
        let link_avg_speed = get_f64("linkAvgSpeed")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(Link {
                    road_type_id: RoadTypeId(
                        road_type_id.get(i).ok_or_else(|| null("roadTypeID"))? as u16,
                    ),
                    link_avg_speed: link_avg_speed.get(i).ok_or_else(|| null("linkAvgSpeed"))?,
                })
            })
            .collect()
    }
}

/// A wrapper row for `runSpecSourceType` — one source type selected by the
/// RunSpec.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RunSpecSourceTypeRow {
 /// `sourceTypeID`.
    pub source_type_id: i32,
}

impl TableRow for RunSpecSourceTypeRow {
    fn table_name() -> &'static str {
        "runSpecSourceType"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([("sourceTypeID".into(), DataType::Int32)])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![Series::new(
                "sourceTypeID".into(),
                rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
            )
            .into()],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "runSpecSourceType";
        let source_type_id = df
            .column("sourceTypeID")
            .map_err(|e| row_err(t, 0, "sourceTypeID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "sourceTypeID", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(RunSpecSourceTypeRow {
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                })
            })
            .collect()
    }
}

/// A wrapper row for `runSpecRoadType` — one road type selected by the RunSpec.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RunSpecRoadTypeRow {
 /// `roadTypeID`.
    pub road_type_id: i32,
}

impl TableRow for RunSpecRoadTypeRow {
    fn table_name() -> &'static str {
        "runSpecRoadType"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([("roadTypeID".into(), DataType::Int32)])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![Series::new(
                "roadTypeID".into(),
                rows.iter().map(|r| r.road_type_id).collect::<Vec<i32>>(),
            )
            .into()],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "runSpecRoadType";
        let road_type_id = df
            .column("roadTypeID")
            .map_err(|e| row_err(t, 0, "roadTypeID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "roadTypeID", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(RunSpecRoadTypeRow {
                    road_type_id: road_type_id.get(i).ok_or_else(|| null("roadTypeID"))?,
                })
            })
            .collect()
    }
}

/// A wrapper row for `runSpecHourDay` — one hour/day combination selected by
/// the RunSpec.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RunSpecHourDayRow {
 /// `hourDayID`.
    pub hour_day_id: i32,
}

impl TableRow for RunSpecHourDayRow {
    fn table_name() -> &'static str {
        "runSpecHourDay"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([("hourDayID".into(), DataType::Int32)])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![Series::new(
                "hourDayID".into(),
                rows.iter().map(|r| r.hour_day_id).collect::<Vec<i32>>(),
            )
            .into()],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "runSpecHourDay";
        let hour_day_id = df
            .column("hourDayID")
            .map_err(|e| row_err(t, 0, "hourDayID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "hourDayID", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(RunSpecHourDayRow {
                    hour_day_id: hour_day_id.get(i).ok_or_else(|| null("hourDayID"))?,
                })
            })
            .collect()
    }
}

/// A wrapper row for `pollutantProcessAssoc` — projects `(pollutantID,
/// processID)` from the `PollutantProcessAssoc` default-DB table.
///
/// The foreign type [`PollutantProcessAssociation`] cannot implement
/// [`TableRow`] directly (it lives in `moves_data`), so this local wrapper
/// carries the same semantics with the columns the rates-first path needs.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AvgSpeedPollutantProcessAssocRow {
 /// `pollutantID`.
    pub pollutant_id: i32,
 /// `processID`.
    pub process_id: i32,
}

impl TableRow for AvgSpeedPollutantProcessAssocRow {
    fn table_name() -> &'static str {
        "pollutantProcessAssoc"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("pollutantID".into(), DataType::Int32),
            ("processID".into(), DataType::Int32),
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
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "pollutantProcessAssoc";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let pollutant_id = get_i32("pollutantID")?;
        let process_id = get_i32("processID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(AvgSpeedPollutantProcessAssocRow {
                    pollutant_id: pollutant_id.get(i).ok_or_else(|| null("pollutantID"))?,
                    process_id: process_id.get(i).ok_or_else(|| null("processID"))?,
                })
            })
            .collect()
    }
}

// ---- TableRow for output table ----

impl TableRow for RatesOpModeDistributionRow {
    fn table_name() -> &'static str {
        "RatesOpModeDistribution"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("sourceTypeID".into(), DataType::Int32),
            ("roadTypeID".into(), DataType::Int32),
            ("avgSpeedBinID".into(), DataType::Int32),
            ("hourDayID".into(), DataType::Int32),
            ("polProcessID".into(), DataType::Int32),
            ("opModeID".into(), DataType::Int32),
            ("opModeFraction".into(), DataType::Float64),
            ("avgBinSpeed".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter()
                        .map(|r| r.source_type_id.0 as i32)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "roadTypeID".into(),
                    rows.iter()
                        .map(|r| r.road_type_id.0 as i32)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "avgSpeedBinID".into(),
                    rows.iter()
                        .map(|r| r.avg_speed_bin_id as i32)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "hourDayID".into(),
                    rows.iter()
                        .map(|r| r.hour_day_id as i32)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "polProcessID".into(),
                    rows.iter()
                        .map(|r| r.pol_process_id.0 as i32)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "opModeID".into(),
                    rows.iter()
                        .map(|r| r.op_mode_id as i32)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "opModeFraction".into(),
                    rows.iter()
                        .map(|r| r.op_mode_fraction)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "avgBinSpeed".into(),
                    rows.iter().map(|r| r.avg_bin_speed).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "RatesOpModeDistribution";
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
        let source_type_id = get_i32("sourceTypeID")?;
        let road_type_id = get_i32("roadTypeID")?;
        let avg_speed_bin_id = get_i32("avgSpeedBinID")?;
        let hour_day_id = get_i32("hourDayID")?;
        let pol_process_id = get_i32("polProcessID")?;
        let op_mode_id = get_i32("opModeID")?;
        let op_mode_fraction = get_f64("opModeFraction")?;
        let avg_bin_speed = get_f64("avgBinSpeed")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(RatesOpModeDistributionRow {
                    source_type_id: SourceTypeId(
                        source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))? as u16,
                    ),
                    road_type_id: RoadTypeId(
                        road_type_id.get(i).ok_or_else(|| null("roadTypeID"))? as u16,
                    ),
                    avg_speed_bin_id: avg_speed_bin_id
                        .get(i)
                        .ok_or_else(|| null("avgSpeedBinID"))?
                        as i16,
                    hour_day_id: hour_day_id.get(i).ok_or_else(|| null("hourDayID"))? as i16,
                    pol_process_id: PolProcessId(
                        pol_process_id.get(i).ok_or_else(|| null("polProcessID"))? as u32,
                    ),
                    op_mode_id: op_mode_id.get(i).ok_or_else(|| null("opModeID"))? as i16,
                    op_mode_fraction: op_mode_fraction
                        .get(i)
                        .ok_or_else(|| null("opModeFraction"))?,
 // This generator always populates avgBinSpeed in its
 // `insert ignore ... avgBinSpeed` statements (Java
 // AverageSpeedOperatingModeDistributionGenerator.java lines
 // 171-177, 283-286, 309-312) from the speed bin / link speed,
 // and CreateExecutionRates.sql declares the column `FLOAT NULL`
 // with no DEFAULT. A NULL read-back is a genuine data gap that
 // downstream rate aggregation divides by
 // (baserategenerator/aggregate.rs), so error loudly like every
 // sibling column rather than fabricate a 0.0 mph speed.
                    avg_bin_speed: avg_bin_speed.get(i).ok_or_else(|| null("avgBinSpeed"))?,
                })
            })
            .collect()
    }
}

/// `RatesOpModeDistribution` generator for average-speed-driven tire-wear
/// operating-mode distributions.
///
/// Ports `AverageSpeedOperatingModeDistributionGenerator.java`; see the
/// module documentation for the scope of the port.
#[derive(Debug, Clone)]
pub struct AverageSpeedOperatingModeDistributionGenerator {
 /// The single master-loop subscription, built once in [`Self::new`].
    subscriptions: [CalculatorSubscription; 1],
}

impl AverageSpeedOperatingModeDistributionGenerator {
 /// Chain-DAG name — matches the Java class name.
    pub const NAME: &'static str = "AverageSpeedOperatingModeDistributionGenerator";

 /// Construct the generator with its master-loop subscription.
 ///
 /// The chain DAG (`calculator-dag.json`) records one subscription:
 /// Tirewear (process 10), `PROCESS` granularity, `GENERATOR` priority.
 /// The Java `subscribeToMe` instead subscribes at `LINK` granularity in
 /// the Project domain — a runtime RunSpec decision the registry /
 /// engine applies — so the static metadata follows the DAG's
 /// (non-Project) `PROCESS` subscription.
    #[must_use]
    pub fn new() -> Self {
        let priority =
            Priority::parse("GENERATOR").expect("\"GENERATOR\" is a valid MasterLoop priority");
        Self {
            subscriptions: [CalculatorSubscription::new(
                TIREWEAR_PROCESS,
                Granularity::Process,
                priority,
            )],
        }
    }
}

impl Default for AverageSpeedOperatingModeDistributionGenerator {
    fn default() -> Self {
        Self::new()
    }
}

/// Default-DB / execution-DB tables the live paths read. `avgSpeedBin`,
/// `operatingMode` and `pollutantProcessAssoc` are default-DB tables;
/// `link` and the `runSpec*` tables are execution-DB tables populated from
/// the RunSpec. `avgSpeedDistribution` is intentionally absent — only the
/// dead `calculateOpModeFractions` path reads it.
static INPUT_TABLES: &[&str] = &[
    "avgSpeedBin",
    "link",
    "operatingMode",
    "pollutantProcessAssoc",
    "runSpecHourDay",
    "runSpecRoadType",
    "runSpecSourceType",
];

/// Scratch-namespace table this generator writes.
static OUTPUT_TABLES: &[&str] = &["RatesOpModeDistribution"];

impl Generator for AverageSpeedOperatingModeDistributionGenerator {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn subscriptions(&self) -> &[CalculatorSubscription] {
        &self.subscriptions
    }

 // `upstream` keeps the trait default (empty): the chain DAG records no
 // `depends_on` edges — this generator's inputs are default-DB and
 // RunSpec tables, not other generators' output.

    fn input_tables(&self) -> &[&'static str] {
        INPUT_TABLES
    }

    fn output_tables(&self) -> &[&'static str] {
        OUTPUT_TABLES
    }

 /// Run the generator for the current master-loop iteration.
 ///
 /// Dispatches on the RunSpec domain derived from `ctx.position()`:
 ///
 /// * **Project domain** (`link_id` is `Some`) → reads `link`,
 /// `runSpecSourceType`, `runSpecHourDay` and `operatingMode` from
 /// `ctx.tables()`, calls [`project_op_mode_fractions`].
 /// * **Non-Project domain** (`link_id` is `None`) → reads `avgSpeedBin`,
 /// `runSpecSourceType`, `runSpecRoadType`, `runSpecHourDay` and
 /// `pollutantProcessAssoc` from `ctx.tables()`, calls
 /// [`rates_first_op_mode_fractions`].
 ///
 /// The result is written to `ctx.scratch()` under `"RatesOpModeDistribution"`.
    fn execute(&self, ctx: &mut CalculatorContext) -> Result<CalculatorOutput, Error> {
        let rows = if ctx.position().location.link_id.is_some() {
 // Project domain — one link, assign tirewear op-mode from link speed.
            let link_rows: Vec<Link> = ctx.tables().iter_typed("link")?;
            let source_type_rows: Vec<RunSpecSourceTypeRow> =
                ctx.tables().iter_typed("runSpecSourceType")?;
            let hour_day_rows: Vec<RunSpecHourDayRow> =
                ctx.tables().iter_typed("runSpecHourDay")?;
            let operating_mode_rows: Vec<OperatingMode> =
                ctx.tables().iter_typed("operatingMode")?;

 // The project link table contains only the single current link.
 // If it is empty, produce no rows (defensive).
            let Some(&link) = link_rows.first() else {
                return crate::wiring::write_scratch_table(
                    ctx,
                    OUTPUT_TABLES[0],
                    Vec::<RatesOpModeDistributionRow>::new(),
                );
            };

            let run_spec_source_type: Vec<SourceTypeId> = source_type_rows
                .iter()
                .map(|r| SourceTypeId(r.source_type_id as u16))
                .collect();
            let run_spec_hour_day: Vec<i16> =
                hour_day_rows.iter().map(|r| r.hour_day_id as i16).collect();

            let inputs = ProjectInputs {
                link,
                run_spec_source_type: &run_spec_source_type,
                run_spec_hour_day: &run_spec_hour_day,
                operating_modes: &operating_mode_rows,
            };
            project_op_mode_fractions(&inputs)
        } else {
 // Non-Project (rates-first) domain — cross-join all speed bins.
            let avg_speed_bin_rows: Vec<AvgSpeedBin> = ctx.tables().iter_typed("avgSpeedBin")?;
            let source_type_rows: Vec<RunSpecSourceTypeRow> =
                ctx.tables().iter_typed("runSpecSourceType")?;
            let road_type_rows: Vec<RunSpecRoadTypeRow> =
                ctx.tables().iter_typed("runSpecRoadType")?;
            let hour_day_rows: Vec<RunSpecHourDayRow> =
                ctx.tables().iter_typed("runSpecHourDay")?;
            let ppa_rows: Vec<AvgSpeedPollutantProcessAssocRow> =
                ctx.tables().iter_typed("pollutantProcessAssoc")?;

            let run_spec_source_type: Vec<SourceTypeId> = source_type_rows
                .iter()
                .map(|r| SourceTypeId(r.source_type_id as u16))
                .collect();
            let run_spec_road_type: Vec<RoadTypeId> = road_type_rows
                .iter()
                .map(|r| RoadTypeId(r.road_type_id as u16))
                .collect();
            let run_spec_hour_day: Vec<i16> =
                hour_day_rows.iter().map(|r| r.hour_day_id as i16).collect();
 // Convert wrapper rows back to PollutantProcessAssociation.
            let pollutant_process_assoc: Vec<PollutantProcessAssociation> = ppa_rows
                .iter()
                .filter_map(|r| {
                    PollutantProcessAssociation::find_by_ids(
                        PollutantId(r.pollutant_id as u16),
                        ProcessId(r.process_id as u16),
                    )
                })
                .collect();

            let inputs = RatesFirstInputs {
                avg_speed_bins: &avg_speed_bin_rows,
                run_spec_source_type: &run_spec_source_type,
                run_spec_road_type: &run_spec_road_type,
                run_spec_hour_day: &run_spec_hour_day,
                pollutant_process_assoc: &pollutant_process_assoc,
            };
            rates_first_op_mode_fractions(&inputs)
        };

        crate::wiring::write_scratch_table(ctx, OUTPUT_TABLES[0], rows)
    }
}

/// Generator factory — returns a boxed instance for registration with the
/// `CalculatorRegistry`.
#[must_use]
pub fn factory() -> Box<dyn Generator> {
    Box::new(AverageSpeedOperatingModeDistributionGenerator::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use moves_data::PollutantId;

 /// `pollutantProcessAssoc` row for the tire-wear pol-process (11710).
    fn tirewear_ppa() -> PollutantProcessAssociation {
        PollutantProcessAssociation {
            pollutant_id: PollutantId(117),
            process_id: ProcessId(10),
        }
    }

 /// `pollutantProcessAssoc` row helper for an arbitrary pair.
    fn ppa(pollutant: u16, process: u16) -> PollutantProcessAssociation {
        PollutantProcessAssociation {
            pollutant_id: PollutantId(pollutant),
            process_id: ProcessId(process),
        }
    }

 /// `avgSpeedBin` row helper.
    fn bin(id: i16, speed: f64, op_mode: Option<i16>) -> AvgSpeedBin {
        AvgSpeedBin {
            avg_speed_bin_id: id,
            avg_bin_speed: speed,
            op_mode_id_tirewear: op_mode,
        }
    }

 /// `operatingMode` row helper.
    fn om(id: i16, name: &str, lower: Option<f64>, upper: Option<f64>) -> OperatingMode {
        OperatingMode {
            op_mode_id: id,
            op_mode_name: name.to_owned(),
            speed_lower: lower,
            speed_upper: upper,
        }
    }

 /// A realistic contiguous tire-wear speed-bin set: idle handled by the
 /// `< 0.1` clause, then `[0.1, 2.5)`, `[2.5, 7.5)`, `[7.5, ∞)`.
    fn tirewear_modes() -> Vec<OperatingMode> {
        vec![
            om(401, "Tirewear 0-2.5 mph", Some(0.1), Some(2.5)),
            om(402, "Tirewear 2.5-7.5 mph", Some(2.5), Some(7.5)),
            om(403, "Tirewear 7.5+ mph", Some(7.5), None),
        ]
    }

 // ── assign_tirewear_op_mode ─────────────────────────────────────────

    #[test]
    fn idle_speed_assigns_op_mode_400() {
        let modes = tirewear_modes();
        assert_eq!(assign_tirewear_op_mode(0.0, &modes), 400);
        assert_eq!(assign_tirewear_op_mode(0.05, &modes), 400);
    }

    #[test]
    fn idle_threshold_is_exclusive_at_0_1() {
 // 0.1 is NOT < 0.1, so it falls through to the speed ranges.
        let modes = tirewear_modes();
        assert_eq!(assign_tirewear_op_mode(0.1, &modes), 401);
 // Just below the threshold is still idle.
        assert_eq!(assign_tirewear_op_mode(0.099, &modes), 400);
    }

    #[test]
    fn speed_in_middle_range_selects_that_mode() {
        let modes = tirewear_modes();
        assert_eq!(assign_tirewear_op_mode(1.0, &modes), 401);
        assert_eq!(assign_tirewear_op_mode(5.0, &modes), 402);
        assert_eq!(assign_tirewear_op_mode(50.0, &modes), 403);
    }

    #[test]
    fn lower_bound_is_inclusive_upper_bound_is_exclusive() {
        let modes = tirewear_modes();
 // 2.5 is excluded from [0.1, 2.5) and included in [2.5, 7.5).
        assert_eq!(assign_tirewear_op_mode(2.5, &modes), 402);
 // 7.5 is excluded from [2.5, 7.5) and included in [7.5, ∞).
        assert_eq!(assign_tirewear_op_mode(7.5, &modes), 403);
    }

    #[test]
    fn unmatched_speed_returns_minus_one() {
 // Ranges cover only [0.1, 5.0); a speed >= 0.1 with no covering
 // range yields the CASE `else -1`.
        let modes = vec![om(401, "Tirewear low", Some(0.1), Some(5.0))];
        assert_eq!(assign_tirewear_op_mode(10.0, &modes), -1);
 // And with no tire-wear modes at all.
        assert_eq!(assign_tirewear_op_mode(10.0, &[]), -1);
    }

    #[test]
    fn open_bounds_match_unbounded_side() {
 // Open below: matches any speed >= 0.1 up to the upper bound.
        let open_low = vec![om(401, "Tirewear open low", None, Some(5.0))];
        assert_eq!(assign_tirewear_op_mode(0.1, &open_low), 401);
        assert_eq!(assign_tirewear_op_mode(4.9, &open_low), 401);
        assert_eq!(assign_tirewear_op_mode(5.0, &open_low), -1);
 // Open above: matches any speed at or above the lower bound.
        let open_high = vec![om(402, "Tirewear open high", Some(5.0), None)];
        assert_eq!(assign_tirewear_op_mode(5.0, &open_high), 402);
        assert_eq!(assign_tirewear_op_mode(1.0e6, &open_high), 402);
        assert_eq!(assign_tirewear_op_mode(4.9, &open_high), -1);
    }

    #[test]
    fn op_modes_outside_401_499_are_ignored() {
 // 400 and 500 are out of the scanned band even with a matching
 // name; only 401's range applies.
        let modes = vec![
            om(400, "Tirewear idle", None, Some(100.0)),
            om(500, "Tirewear over", None, Some(100.0)),
            om(401, "Tirewear real", Some(0.1), Some(100.0)),
        ];
        assert_eq!(assign_tirewear_op_mode(10.0, &modes), 401);
    }

    #[test]
    fn non_tirewear_named_modes_are_ignored() {
 // An id in 401..=499 whose name is not `tirewear%` is excluded by
 // the `LIKE` filter, leaving the speed unmatched.
        let modes = vec![om(450, "Running exhaust bin", Some(0.1), Some(100.0))];
        assert_eq!(assign_tirewear_op_mode(10.0, &modes), -1);
    }

    #[test]
    fn name_filter_is_case_insensitive() {
 // `utf8mb4_unicode_ci` makes `LIKE 'tirewear%'` case-insensitive.
        let modes = vec![om(401, "TIREWEAR CRUISE", Some(0.1), Some(100.0))];
        assert_eq!(assign_tirewear_op_mode(10.0, &modes), 401);
    }

    #[test]
    fn ranges_resolve_regardless_of_input_order() {
 // Same bins as `tirewear_modes` but shuffled: `ORDER BY speedLower`
 // makes the result independent of input order.
        let shuffled = vec![
            om(403, "Tirewear 7.5+ mph", Some(7.5), None),
            om(401, "Tirewear 0-2.5 mph", Some(0.1), Some(2.5)),
            om(402, "Tirewear 2.5-7.5 mph", Some(2.5), Some(7.5)),
        ];
        assert_eq!(assign_tirewear_op_mode(1.0, &shuffled), 401);
        assert_eq!(assign_tirewear_op_mode(5.0, &shuffled), 402);
        assert_eq!(assign_tirewear_op_mode(9.0, &shuffled), 403);
    }

    #[test]
    fn idle_clause_wins_over_a_range_covering_low_speed() {
 // A range that also covers sub-0.1 speeds must not pre-empt op-mode
 // 400 — the idle clause is the first CASE `WHEN`.
        let modes = vec![om(401, "Tirewear open low", None, Some(100.0))];
        assert_eq!(assign_tirewear_op_mode(0.05, &modes), 400);
        assert_eq!(assign_tirewear_op_mode(0.5, &modes), 401);
    }

 // ── rates_first_op_mode_fractions ───────────────────────────────────

    #[test]
    fn rates_first_cross_joins_all_dimensions() {
        let bins = [bin(1, 2.0, Some(401)), bin(2, 8.0, Some(402))];
        let assoc = [tirewear_ppa()];
        let inputs = RatesFirstInputs {
            avg_speed_bins: &bins,
            run_spec_source_type: &[SourceTypeId(21), SourceTypeId(31)],
            run_spec_road_type: &[RoadTypeId(2), RoadTypeId(4)],
            run_spec_hour_day: &[51, 52],
            pollutant_process_assoc: &assoc,
        };
        let rows = rates_first_op_mode_fractions(&inputs);
 // 2 bins × 2 source types × 2 road types × 2 hour/days = 16 rows.
        assert_eq!(rows.len(), 16);
        for r in &rows {
            assert_eq!(r.pol_process_id, TIREWEAR_POL_PROCESS);
            assert_eq!(r.op_mode_fraction, 1.0);
        }
 // Each row carries its bin's op mode and avg speed.
        let bin1 = rows.iter().find(|r| r.avg_speed_bin_id == 1).unwrap();
        assert_eq!(bin1.op_mode_id, 401);
        assert_eq!(bin1.avg_bin_speed, 2.0);
        let bin2 = rows.iter().find(|r| r.avg_speed_bin_id == 2).unwrap();
        assert_eq!(bin2.op_mode_id, 402);
        assert_eq!(bin2.avg_bin_speed, 8.0);
    }

    #[test]
    fn rates_first_empty_when_tirewear_not_modeled() {
 // pollutantProcessAssoc lacks 11710: the filtered join is empty.
        let bins = [bin(1, 2.0, Some(401))];
        let assoc = [ppa(2, 1), ppa(31, 90)];
        let inputs = RatesFirstInputs {
            avg_speed_bins: &bins,
            run_spec_source_type: &[SourceTypeId(21)],
            run_spec_road_type: &[RoadTypeId(2)],
            run_spec_hour_day: &[51],
            pollutant_process_assoc: &assoc,
        };
        assert!(rates_first_op_mode_fractions(&inputs).is_empty());
    }

    #[test]
    fn rates_first_null_op_mode_tirewear_becomes_zero() {
 // A NULL `opModeIDTirewear` is coerced to 0 by `INSERT IGNORE`.
        let bins = [bin(1, 2.0, None)];
        let assoc = [tirewear_ppa()];
        let inputs = RatesFirstInputs {
            avg_speed_bins: &bins,
            run_spec_source_type: &[SourceTypeId(21)],
            run_spec_road_type: &[RoadTypeId(2)],
            run_spec_hour_day: &[51],
            pollutant_process_assoc: &assoc,
        };
        let rows = rates_first_op_mode_fractions(&inputs);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].op_mode_id, 0);
    }

    #[test]
    fn rates_first_empty_inputs_produce_no_rows() {
 // Tire-wear modeled, but no bins / no RunSpec dimensions.
        let assoc = [tirewear_ppa()];
        let inputs = RatesFirstInputs {
            avg_speed_bins: &[],
            run_spec_source_type: &[SourceTypeId(21)],
            run_spec_road_type: &[RoadTypeId(2)],
            run_spec_hour_day: &[51],
            pollutant_process_assoc: &assoc,
        };
        assert!(rates_first_op_mode_fractions(&inputs).is_empty());
    }

    #[test]
    fn rates_first_dedups_duplicate_bins_and_sorts() {
 // Two identical bins collide on the primary key; `INSERT IGNORE`
 // keeps one. Output is primary-key sorted.
        let bins = [bin(1, 2.0, Some(401)), bin(1, 2.0, Some(401))];
        let assoc = [tirewear_ppa()];
        let inputs = RatesFirstInputs {
            avg_speed_bins: &bins,
            run_spec_source_type: &[SourceTypeId(31), SourceTypeId(21)],
            run_spec_road_type: &[RoadTypeId(2)],
            run_spec_hour_day: &[52, 51],
            pollutant_process_assoc: &assoc,
        };
        let rows = rates_first_op_mode_fractions(&inputs);
 // 1 distinct bin × 2 source types × 1 road type × 2 hour/days = 4.
        assert_eq!(rows.len(), 4);
        let keys: Vec<RowKey> = rows.iter().map(RatesOpModeDistributionRow::key).collect();
        let mut sorted = keys.clone();
        sorted.sort_unstable();
        assert_eq!(keys, sorted);
    }

 // ── project_op_mode_fractions ───────────────────────────────────────

    #[test]
    fn project_cross_joins_source_types_and_hour_days() {
        let modes = tirewear_modes();
        let inputs = ProjectInputs {
            link: Link {
                road_type_id: RoadTypeId(5),
                link_avg_speed: 5.0,
            },
            run_spec_source_type: &[SourceTypeId(21), SourceTypeId(31)],
            run_spec_hour_day: &[51, 52, 53],
            operating_modes: &modes,
        };
        let rows = project_op_mode_fractions(&inputs);
 // 2 source types × 3 hour/days = 6 rows.
        assert_eq!(rows.len(), 6);
        for r in &rows {
            assert_eq!(r.pol_process_id, TIREWEAR_POL_PROCESS);
            assert_eq!(r.road_type_id, RoadTypeId(5));
            assert_eq!(r.avg_speed_bin_id, 0);
            assert_eq!(r.avg_bin_speed, 5.0);
            assert_eq!(r.op_mode_fraction, 1.0);
 // 5.0 mph falls in the [2.5, 7.5) tire-wear range.
            assert_eq!(r.op_mode_id, 402);
        }
    }

    #[test]
    fn project_idle_link_gets_op_mode_400() {
        let modes = tirewear_modes();
        let inputs = ProjectInputs {
            link: Link {
                road_type_id: RoadTypeId(5),
                link_avg_speed: 0.0,
            },
            run_spec_source_type: &[SourceTypeId(21)],
            run_spec_hour_day: &[51],
            operating_modes: &modes,
        };
        let rows = project_op_mode_fractions(&inputs);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].op_mode_id, 400);
    }

    #[test]
    fn project_unmatched_link_speed_gets_op_mode_minus_one() {
 // No operating modes: every non-idle speed is unassigned.
        let inputs = ProjectInputs {
            link: Link {
                road_type_id: RoadTypeId(5),
                link_avg_speed: 30.0,
            },
            run_spec_source_type: &[SourceTypeId(21)],
            run_spec_hour_day: &[51],
            operating_modes: &[],
        };
        let rows = project_op_mode_fractions(&inputs);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].op_mode_id, -1);
    }

    #[test]
    fn project_output_is_primary_key_sorted() {
        let modes = tirewear_modes();
        let inputs = ProjectInputs {
            link: Link {
                road_type_id: RoadTypeId(5),
                link_avg_speed: 5.0,
            },
            run_spec_source_type: &[SourceTypeId(31), SourceTypeId(21)],
            run_spec_hour_day: &[53, 51],
            operating_modes: &modes,
        };
        let rows = project_op_mode_fractions(&inputs);
        let keys: Vec<RowKey> = rows.iter().map(RatesOpModeDistributionRow::key).collect();
        let mut sorted = keys.clone();
        sorted.sort_unstable();
        assert_eq!(keys, sorted);
    }

    #[test]
    fn project_empty_run_spec_produces_no_rows() {
        let modes = tirewear_modes();
        let inputs = ProjectInputs {
            link: Link {
                road_type_id: RoadTypeId(5),
                link_avg_speed: 5.0,
            },
            run_spec_source_type: &[],
            run_spec_hour_day: &[51],
            operating_modes: &modes,
        };
        assert!(project_op_mode_fractions(&inputs).is_empty());
    }

 // ── generator metadata / trait ──────────────────────────────────────

    #[test]
    fn generator_metadata_matches_chain_dag() {
        let gen = AverageSpeedOperatingModeDistributionGenerator::new();
        assert_eq!(gen.name(), "AverageSpeedOperatingModeDistributionGenerator");
        assert_eq!(gen.output_tables(), &["RatesOpModeDistribution"]);
 // No upstream generators (chain DAG `depends_on` is empty).
        assert!(gen.upstream().is_empty());
        assert!(gen.input_tables().contains(&"avgSpeedBin"));
        assert!(gen.input_tables().contains(&"operatingMode"));
 // The dead `calculateOpModeFractions` path's table is not listed.
        assert!(!gen.input_tables().contains(&"avgSpeedDistribution"));

        let subs = gen.subscriptions();
        assert_eq!(subs.len(), 1);
        assert_eq!(subs[0].process_id, ProcessId(10));
        assert_eq!(subs[0].granularity, Granularity::Process);
        assert_eq!(subs[0].priority.display(), "GENERATOR");
    }

 // ── execute() integration tests ─────────────────────────────────────

 /// Build an `InMemoryStore` populated with the rates-first (non-Project)
 /// input tables required by `execute`.
    fn rates_first_store() -> moves_framework::InMemoryStore {
        use moves_framework::{DataFrameStore, InMemoryStore};

        let mut store = InMemoryStore::new();

 // avgSpeedBin: two bins — bin 1 at 2.0 mph (op mode 401), bin 2 at
 // 8.0 mph (op mode 402).
        store.insert(
            "avgSpeedBin",
            AvgSpeedBin::into_dataframe(vec![
                AvgSpeedBin {
                    avg_speed_bin_id: 1,
                    avg_bin_speed: 2.0,
                    op_mode_id_tirewear: Some(401),
                },
                AvgSpeedBin {
                    avg_speed_bin_id: 2,
                    avg_bin_speed: 8.0,
                    op_mode_id_tirewear: Some(402),
                },
            ])
            .unwrap(),
        );
 // runSpecSourceType: one source type.
        store.insert(
            "runSpecSourceType",
            RunSpecSourceTypeRow::into_dataframe(vec![RunSpecSourceTypeRow { source_type_id: 21 }])
                .unwrap(),
        );
 // runSpecRoadType: one road type.
        store.insert(
            "runSpecRoadType",
            RunSpecRoadTypeRow::into_dataframe(vec![RunSpecRoadTypeRow { road_type_id: 2 }])
                .unwrap(),
        );
 // runSpecHourDay: one hour/day.
        store.insert(
            "runSpecHourDay",
            RunSpecHourDayRow::into_dataframe(vec![RunSpecHourDayRow { hour_day_id: 51 }]).unwrap(),
        );
 // pollutantProcessAssoc: tire-wear pol-process (pollutant 117, process 10).
        store.insert(
            "pollutantProcessAssoc",
            AvgSpeedPollutantProcessAssocRow::into_dataframe(vec![
                AvgSpeedPollutantProcessAssocRow {
                    pollutant_id: 117,
                    process_id: 10,
                },
            ])
            .unwrap(),
        );
        store
    }

    #[test]
    fn execute_rates_first_writes_expected_rows_to_scratch() {
        use moves_framework::{DataFrameStoreTyped, IterationPosition};

        let store = rates_first_store();
 // Non-project position: no link_id.
        let pos = IterationPosition::default();
        let mut ctx = CalculatorContext::with_position_and_tables(pos, store);

        let gen = AverageSpeedOperatingModeDistributionGenerator::new();
        let out = gen.execute(&mut ctx).expect("execute ok");
 // Generator writes to scratch, not the main output DataFrame.
        assert!(out.dataframe().is_none());

        let rows: Vec<RatesOpModeDistributionRow> = ctx
            .scratch()
            .store
            .iter_typed("RatesOpModeDistribution")
            .expect("RatesOpModeDistribution in scratch");
 // 2 bins × 1 source type × 1 road type × 1 hour/day = 2 rows.
        assert_eq!(rows.len(), 2);

 // All rows carry the tirewear pol-process and fraction = 1.
        for r in &rows {
            assert_eq!(r.pol_process_id, TIREWEAR_POL_PROCESS);
            assert_eq!(r.op_mode_fraction, 1.0);
        }

 // Each row is keyed by its bin's speed and op mode.
        let row1 = rows.iter().find(|r| r.avg_speed_bin_id == 1).unwrap();
        assert_eq!(row1.op_mode_id, 401);
        assert_eq!(row1.avg_bin_speed, 2.0);
        assert_eq!(row1.source_type_id, SourceTypeId(21));
        assert_eq!(row1.road_type_id, RoadTypeId(2));
        assert_eq!(row1.hour_day_id, 51);

        let row2 = rows.iter().find(|r| r.avg_speed_bin_id == 2).unwrap();
        assert_eq!(row2.op_mode_id, 402);
        assert_eq!(row2.avg_bin_speed, 8.0);
    }

    #[test]
    fn execute_rates_first_empty_when_tirewear_not_in_ppa() {
        use moves_framework::{DataFrameStore, DataFrameStoreTyped, IterationPosition};

        let mut store = rates_first_store();
 // Replace pollutantProcessAssoc with one that has no tire-wear entry.
        store.insert(
            "pollutantProcessAssoc",
            AvgSpeedPollutantProcessAssocRow::into_dataframe(vec![
                AvgSpeedPollutantProcessAssocRow {
                    pollutant_id: 2,
                    process_id: 1,
                },
            ])
            .unwrap(),
        );

        let pos = IterationPosition::default();
        let mut ctx = CalculatorContext::with_position_and_tables(pos, store);

        let gen = AverageSpeedOperatingModeDistributionGenerator::new();
        gen.execute(&mut ctx).expect("execute ok");

        let rows: Vec<RatesOpModeDistributionRow> = ctx
            .scratch()
            .store
            .iter_typed("RatesOpModeDistribution")
            .expect("RatesOpModeDistribution in scratch");
        assert!(rows.is_empty(), "no tire-wear rows when ppa lacks 11710");
    }

    #[test]
    fn execute_project_writes_expected_rows_to_scratch() {
        use moves_framework::ExecutionLocation;
        use moves_framework::{
            DataFrameStore, DataFrameStoreTyped, InMemoryStore, IterationPosition,
        };

        let mut store = InMemoryStore::new();

 // link: 5.0 mph on road type 5.
        store.insert(
            "link",
            Link::into_dataframe(vec![Link {
                road_type_id: RoadTypeId(5),
                link_avg_speed: 5.0,
            }])
            .unwrap(),
        );
 // runSpecSourceType: two source types.
        store.insert(
            "runSpecSourceType",
            RunSpecSourceTypeRow::into_dataframe(vec![
                RunSpecSourceTypeRow { source_type_id: 21 },
                RunSpecSourceTypeRow { source_type_id: 31 },
            ])
            .unwrap(),
        );
 // runSpecHourDay: one hour/day.
        store.insert(
            "runSpecHourDay",
            RunSpecHourDayRow::into_dataframe(vec![RunSpecHourDayRow { hour_day_id: 51 }]).unwrap(),
        );
 // operatingMode: realistic tirewear modes.
        store.insert(
            "operatingMode",
            OperatingMode::into_dataframe(vec![
                OperatingMode {
                    op_mode_id: 401,
                    op_mode_name: "Tirewear 0-2.5 mph".to_owned(),
                    speed_lower: Some(0.1),
                    speed_upper: Some(2.5),
                },
                OperatingMode {
                    op_mode_id: 402,
                    op_mode_name: "Tirewear 2.5-7.5 mph".to_owned(),
                    speed_lower: Some(2.5),
                    speed_upper: Some(7.5),
                },
                OperatingMode {
                    op_mode_id: 403,
                    op_mode_name: "Tirewear 7.5+ mph".to_owned(),
                    speed_lower: Some(7.5),
                    speed_upper: None,
                },
            ])
            .unwrap(),
        );

 // Project domain position: link_id is Some.
        let pos = IterationPosition {
            iteration: 0,
            process_id: None,
            location: ExecutionLocation::link(40, 40_001, 400_011, 1),
            time: moves_framework::ExecutionTime::none(),
        };
        let mut ctx = CalculatorContext::with_position_and_tables(pos, store);

        let gen = AverageSpeedOperatingModeDistributionGenerator::new();
        let out = gen.execute(&mut ctx).expect("execute ok");
        assert!(out.dataframe().is_none());

        let rows: Vec<RatesOpModeDistributionRow> = ctx
            .scratch()
            .store
            .iter_typed("RatesOpModeDistribution")
            .expect("RatesOpModeDistribution in scratch");
 // 2 source types × 1 hour/day = 2 rows.
        assert_eq!(rows.len(), 2);

        for r in &rows {
            assert_eq!(r.pol_process_id, TIREWEAR_POL_PROCESS);
            assert_eq!(r.road_type_id, RoadTypeId(5));
            assert_eq!(r.avg_speed_bin_id, 0);
            assert_eq!(r.avg_bin_speed, 5.0);
            assert_eq!(r.op_mode_fraction, 1.0);
 // 5.0 mph → tirewear range [2.5, 7.5) → op mode 402.
            assert_eq!(r.op_mode_id, 402);
        }
    }

    #[test]
    fn generator_is_object_safe() {
 // The registry stores generators as Box<dyn Generator>.
        let gen: Box<dyn Generator> =
            Box::new(AverageSpeedOperatingModeDistributionGenerator::new());
        assert_eq!(gen.name(), "AverageSpeedOperatingModeDistributionGenerator");
    }

    #[test]
    fn factory_builds_a_named_generator() {
        assert_eq!(
            factory().name(),
            "AverageSpeedOperatingModeDistributionGenerator"
        );
    }
}
