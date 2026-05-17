//! Port of `AverageSpeedOperatingModeDistributionGenerator.java`
//! (`gov.epa.otaq.moves.master.implementation.ghg`) — the generator that
//! builds tire-wear `RatesOpModeDistribution` records from average-speed
//! information.
//!
//! Migration plan: Phase 3, Task 31 — the average-speed-binned variant of
//! the drive-schedule-derived `OperatingModeDistributionGenerator`
//! (Task 30). It runs when the RunSpec supplies `AvgSpeedDistribution`
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
//!   as [`rates_first_op_mode_fractions`].
//! * **Project domain** → `calculateTireProjectOpModeFractions` (with the
//!   speed→op-mode `CASE` clause built by `buildOpModeClause`), ported as
//!   [`project_op_mode_fractions`] / [`assign_tirewear_op_mode`].
//!
//! Two branches are **dead code** in the pinned source and are deliberately
//! not ported:
//!
//! * `calculateOpModeFractions` — the non-rates-first `OpModeDistribution`
//!   path; unreachable because `DO_RATES_FIRST` is `true`. (It is the only
//!   path that reads `avgSpeedDistribution`, so that table does not appear
//!   in [`Generator::input_tables`].)
//! * the `USE_2010B_TIREWEAR_RATE_METHOD` branch inside
//!   `calculateRatesFirstOpModeFractions` — the flag is `false`, and the
//!   EPA comment notes the method "is known to make incorrect rates".
//!
//! # The live algorithm
//!
//! Tire-wear operating-mode "distributions" are degenerate: every emitted
//! row carries `opModeFraction = 1`. A vehicle in a given speed context is
//! assigned to exactly one tire-wear operating mode.
//!
//! * **Non-Project** ([`rates_first_op_mode_fractions`]) — every speed bin
//!   already carries its tire-wear operating mode in
//!   `avgSpeedBin.opModeIDTirewear`. The generator cross-joins the speed
//!   bins with the RunSpec's selected source types, road types and
//!   hour/days and emits one `opModeFraction = 1` row each, carrying the
//!   bin's `avgBinSpeed`.
//! * **Project** ([`project_op_mode_fractions`]) — a project link has a
//!   single `linkAvgSpeed`, so the generator assigns the whole link one
//!   operating mode via the [`assign_tirewear_op_mode`] `CASE` clause and
//!   cross-joins it with the selected source types and hour/days
//!   (`avgSpeedBinID = 0`, `avgBinSpeed = linkAvgSpeed`).
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
//! # Data plane (Task 50)
//!
//! [`Generator::execute`] receives a [`CalculatorContext`] whose
//! `ExecutionTables` / `ScratchNamespace` are Phase-2 placeholders until
//! the `DataFrameStore` lands (migration-plan Task 50), so `execute` cannot
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
use moves_data::{PolProcessId, PollutantProcessAssociation, ProcessId, RoadTypeId, SourceTypeId};
use moves_framework::{
    CalculatorContext, CalculatorOutput, CalculatorSubscription, Error, Generator,
};

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

/// Operating mode for a link speed that matches no tire-wear speed range —
/// the `else -1` of the assignment `CASE`.
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
/// references. Once the Task 50 data plane lands, [`Generator::execute`]
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
///   `LIKE 'tirewear%'`), ordered by `speedLower`, of the form
///   `when (speedLower <= linkAvgSpeed and linkAvgSpeed < speedUpper)`;
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
///   polProcessID = 11710` — clears every prior tire-wear row;
/// * **step 020** an `INSERT IGNORE` whose `SELECT` cross-joins the single
///   `link` with `RunSpecSourceType` and `RunSpecHourDay`, assigning the
///   link one operating mode via the [`assign_tirewear_op_mode`] `CASE`
///   clause (`avgSpeedBinID = 0`, `opModeFraction = 1`,
///   `avgBinSpeed = linkAvgSpeed`).
///
/// Because step 010 wipes the table each link iteration,
/// `RatesOpModeDistribution` holds only the *current* link's tire-wear
/// distribution — consumed by that link's rate calculators before the next
/// link overwrites it. This function returns one link's complete row set;
/// the Task 50 `execute` writes it as the full (replacing) table content.
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
    /// **Data plane pending (Task 50).** [`CalculatorContext`] exposes only
    /// placeholder `ExecutionTables` / `ScratchNamespace` today, so this
    /// body cannot read the [`input_tables`](Generator::input_tables) nor
    /// write `RatesOpModeDistribution`. The numerically faithful algorithm
    /// is fully ported and tested in [`rates_first_op_mode_fractions`],
    /// [`project_op_mode_fractions`] and [`assign_tirewear_op_mode`]; once
    /// the `DataFrameStore` lands, `execute` will project the input views
    /// from `ctx.tables()`, dispatch on RunSpec domain, and store the rows.
    fn execute(&self, _ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
        Ok(CalculatorOutput::empty())
    }
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

    #[test]
    fn generator_execute_returns_placeholder_until_data_plane() {
        // execute is a documented placeholder until Task 50; it must still
        // honour the trait contract and return Ok.
        let gen = AverageSpeedOperatingModeDistributionGenerator::new();
        let ctx = CalculatorContext::new();
        assert!(gen.execute(&ctx).is_ok());
    }

    #[test]
    fn generator_is_object_safe() {
        // The registry stores generators as Box<dyn Generator>.
        let gen: Box<dyn Generator> =
            Box::new(AverageSpeedOperatingModeDistributionGenerator::new());
        assert_eq!(gen.name(), "AverageSpeedOperatingModeDistributionGenerator");
    }
}
