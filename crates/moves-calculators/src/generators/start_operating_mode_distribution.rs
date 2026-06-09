//! `StartOperatingModeDistributionGenerator` —.
//!
//! Ports `gov.epa.otaq.moves.master.implementation.ghg.StartOperatingModeDistributionGenerator`
//! (479 lines of Java). The generator builds the **start-exhaust
//! operating-mode distribution**: every engine start is assigned an
//! operating mode from the vehicle's *soak time* — how long the engine sat
//! off since the previous trip — and those per-trip classifications are
//! reduced to the fraction of starts in each operating mode, keyed by source
//! type and hour-of-day.
//!
//! # The Java, in four numbered passes
//!
//! `executeLoop` runs a once-per-run setup — `calculateSoakTime`,
//! `calculateStartOpMode`, `calculateOpModeFraction` (the `@step` 100–399
//! ranges) — then `populateOperatingModeDistribution` (step 400) on every
//! zone change:
//!
//! 1. **Soak time (step 100).** `calculateSoakTime` self-joins
//! `SampleVehicleTrip` on `priorTripID`: `soakTime = keyOnTime −
//! keyOffTime[prior trip]`, the engine-off gap before the start. A trip
//! with no prior trip produces no soak-time row — the self-join is an
//! INNER JOIN.
//! 2. **Start operating mode (step 200).** `calculateStartOpMode` joins each
//! soak time against the `OperatingMode` soak-time bands
//! (`minSoakTime` … `maxSoakTime`) and keeps the matching modes.
//! 3. **Operating-mode fraction (step 300).** `calculateOpModeFraction`
//! counts the starts per (source type, hour-day) and, within each, the
//! starts in every operating mode; `opModeFraction = count(opMode) /
//! starts`.
//! 4. **Populate (step 400).** `populateOperatingModeDistribution` copies the
//! fractions into the `OpModeDistribution` / `RatesOpModeDistribution`
//! execution tables for the start (process 2) and crankcase-start
//! (process 16) processes.
//!
//! # What this port keeps
//!
//! Steps 100–300 are arithmetic over rows; this module ports that arithmetic
//! as pure, individually testable functions — [`soak_time`],
//! [`OperatingMode::matches`] / [`classify_start_op_mode`],
//! [`op_mode_fraction`], [`hour_day_id`] — with [`classify_trip`] tying the
//! soak-time and classification steps into the per-trip operation the
//! generator performs. [`is_recognized_start_exhaust_pol_process`] ports the
//! `getPollutantProcessIDs` filter.
//!
//! Step 400 and the `existingStartOMD` / `SOMDGOpModes` bookkeeping are pure
//! relational copies between execution tables — multi-table `INSERT … SELECT`
//! with no arithmetic of their own. They belong to
//! [`StartOperatingModeDistributionGenerator`]'s [`execute`](Generator::execute)
//! body, a documented shell until the data plane lands (see *Data-plane
//! status*).
//!
//! # Data-plane status
//!
//! The `moves-framework` calculator data plane is still a skeleton:
//! the [`CalculatorContext`] passed to [`execute`](Generator::execute)
//! exposes only placeholder execution tables and scratch namespace with no
//! row storage. So `execute` cannot read `SampleVehicleTrip` or write
//! `OpModeDistribution` yet — it returns an empty [`CalculatorOutput`],
//! matching every other/3 module (empty-output smoke
//! test). (`DataFrameStore`) lands the storage; `execute` then walks
//! the trips through [`classify_trip`], aggregates with [`op_mode_fraction`],
//! and writes the result. The functions below are complete and tested and
//! are what `execute` will call.

use std::collections::BTreeMap;
use std::sync::OnceLock;

use moves_calculator_info::{Granularity, Priority};
use moves_data::{PollutantId, PollutantProcessAssociation, ProcessId};
use moves_framework::{
    CalculatorContext, CalculatorOutput, CalculatorSubscription, DataFrameStoreTyped, Error,
    Generator, ModelScale, TableRow,
};
use polars::prelude::{DataFrame, DataType, NamedFrom, PolarsResult, Schema, Series};

/// The "Start Exhaust" emission process — `processID` 2. The Java caches
/// `EmissionProcess.findByName("Start Exhaust")` as `startExhaust` and
/// subscribes the generator to it.
pub const START_EXHAUST_PROCESS_ID: ProcessId = ProcessId(2);

/// The "Crankcase Start Exhaust" emission process — `processID` 16. Step 400
/// copies the start fractions to processes 2 and 16 alike (the SQL
/// `where ppa.processID in (2,16)`).
pub const CRANKCASE_START_EXHAUST_PROCESS_ID: ProcessId = ProcessId(16);

/// Operating mode 100 — "All Starts". Step 400's `DO_RATES_FIRST` branch adds
/// a `RatesOpModeDistribution` row at op-mode 100 with `opModeFraction` 1.0
/// so a rate can be requested for the undifferentiated total of starts.
pub const ALL_STARTS_OP_MODE_ID: u16 = 100;

/// The pollutants `getPollutantProcessIDs` accepts for the start-exhaust
/// process, by `pollutantID`. The Java tests each runspec
/// [`PollutantProcessAssociation`] against this set (and the start-exhaust
/// process) to build the `polProcessID` filter step 400 applies in its
/// project-domain branch.
///
/// Listed in `Pollutant` id order: Total Gaseous Hydrocarbons (1), Carbon
/// Monoxide (2), Oxides of Nitrogen (3), Nitrous Oxide (6), Ammonia (30),
/// Non-Methane Hydrocarbons (79), Non-Methane Organic Gases (80), Total
/// Organic Gases (86), Volatile Organic Compounds (87), Total Energy
/// Consumption (91), Elemental Carbon (112), Composite – NonECPM (118). The
/// Java declares them in a different order; membership, not order, is what
/// the filter uses.
pub const RECOGNIZED_START_EXHAUST_POLLUTANTS: [PollutantId; 12] = [
    PollutantId(1),
    PollutantId(2),
    PollutantId(3),
    PollutantId(6),
    PollutantId(30),
    PollutantId(79),
    PollutantId(80),
    PollutantId(86),
    PollutantId(87),
    PollutantId(91),
    PollutantId(112),
    PollutantId(118),
];

/// Whether `pollutant` is one of the [`RECOGNIZED_START_EXHAUST_POLLUTANTS`].
#[must_use]
pub fn is_recognized_start_exhaust_pollutant(pollutant: PollutantId) -> bool {
    RECOGNIZED_START_EXHAUST_POLLUTANTS.contains(&pollutant)
}

/// Whether `assoc` is a `(pollutant, process)` pair `getPollutantProcessIDs`
/// keeps — the start-exhaust process paired with a recognised pollutant.
///
/// The Java filters the runspec's pollutant-process associations to those
/// whose `emissionProcess` is Start Exhaust and whose `pollutant` is in
/// [`RECOGNIZED_START_EXHAUST_POLLUTANTS`], then records each survivor's
/// `polProcessID` ([`PollutantProcessAssociation::polproc_id`]).
#[must_use]
pub fn is_recognized_start_exhaust_pol_process(assoc: PollutantProcessAssociation) -> bool {
    assoc.process_id == START_EXHAUST_PROCESS_ID
        && is_recognized_start_exhaust_pollutant(assoc.pollutant_id)
}

/// Soak time of a start — `keyOnTime − keyOffTime` of the prior trip.
///
/// `calculateSoakTime` self-joins `SampleVehicleTrip` so that, for the trip
/// that started the engine at `key_on_time`, `prior_key_off_time` is the
/// `keyOffTime` of the trip named by its `priorTripID`. The difference is how
/// long the engine sat off — the longer the soak, the colder the start.
///
/// The Java keeps the raw difference unfloored, so overlapping sample data
/// (a key-on before the prior key-off) yields a negative soak time;
/// [`OperatingMode::matches`] compares that against the soak-time bands
/// unchanged. `keyOnTime` / `keyOffTime` are `INT` columns.
///
/// ```
/// use moves_calculators::generators::start_operating_mode_distribution::soak_time;
///
/// // Engine off at minute 480, started again at minute 540 — a 60-unit soak.
/// assert_eq!(soak_time(540, 480), 60);
/// ```
#[must_use]
pub fn soak_time(key_on_time: i32, prior_key_off_time: i32) -> i32 {
    key_on_time - prior_key_off_time
}

/// One `OperatingMode` row, narrowed to the columns the start-operating-mode
/// classification reads: the mode id and its soak-time band.
///
/// `OperatingMode.minSoakTime` and `maxSoakTime` are `SMALLINT NULL`; a
/// `None` bound is an open end of the band. The bounds are widened to `i32`
/// here so a soak time computed from `INT` key times compares without a
/// narrowing cast.
///
/// ```
/// use moves_calculators::generators::start_operating_mode_distribution::OperatingMode;
///
/// // A closed band is the half-open interval [min, max).
/// let mode = OperatingMode { op_mode_id: 102, min_soak_time: Some(6), max_soak_time: Some(30) };
/// assert!(mode.matches(6)); // lower bound is inclusive
/// assert!(mode.matches(29));
/// assert!(!mode.matches(30)); // upper bound is exclusive
/// assert!(!mode.matches(5));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OperatingMode {
    /// `opModeID` — the operating-mode primary key.
    pub op_mode_id: u16,
    /// `minSoakTime` — inclusive lower bound of the soak-time band, or
    /// `None` for a band open at the bottom.
    pub min_soak_time: Option<i32>,
    /// `maxSoakTime` — exclusive upper bound of the soak-time band, or
    /// `None` for a band open at the top.
    pub max_soak_time: Option<i32>,
}

impl OperatingMode {
    /// Whether `soak_time` falls in this operating mode's soak-time band.
    ///
    /// Ports the `calculateStartOpMode` WHERE clause verbatim:
    ///
    /// ```sql
    /// (minSoakTime <= soakTime OR (minSoakTime IS NULL AND maxSoakTime IS NOT NULL))
    /// AND (maxSoakTime > soakTime OR (maxSoakTime IS NULL AND minSoakTime IS NOT NULL))
    /// ```
    ///
    /// SQL three-valued logic collapses to four cases:
    ///
    /// * **both bounds present** — the half-open interval `[min, max)`;
    /// * **only `max`** — `soak_time < max` (band open at the bottom);
    /// * **only `min`** — `soak_time >= min` (band open at the top);
    /// * **neither bound** — never matches: each clause is `NULL OR FALSE`,
    /// i.e. SQL `UNKNOWN`, and `UNKNOWN AND UNKNOWN` is not `TRUE`.
    ///
    /// A clause evaluates to `UNKNOWN` only in the both-`None` case — every
    /// other case makes it definitely `TRUE` or definitely `FALSE` — and that
    /// case is rejected anyway, so collapsing an `UNKNOWN` clause to `false`
    /// is exact.
    #[must_use]
    pub fn matches(self, soak_time: i32) -> bool {
        // Lower clause: `minSoakTime <= soakTime`, or — when minSoakTime is
        // NULL — true exactly when maxSoakTime is present.
        let lower_ok = match (self.min_soak_time, self.max_soak_time) {
            (Some(min), _) => min <= soak_time,
            (None, Some(_)) => true,
            (None, None) => false,
        };
        // Upper clause: `maxSoakTime > soakTime`, or — when maxSoakTime is
        // NULL — true exactly when minSoakTime is present.
        let upper_ok = match (self.max_soak_time, self.min_soak_time) {
            (Some(max), _) => max > soak_time,
            (None, Some(_)) => true,
            (None, None) => false,
        };
        lower_ok && upper_ok
    }
}

/// Every operating mode whose soak-time band contains `soak_time`, in the
/// order `operating_modes` lists them.
///
/// Ports `calculateStartOpMode`'s `SoakTime INNER JOIN OperatingMode
/// WHERE …`: the join emits one `StartOpMode` row per matching mode. MOVES's
/// canonical start operating modes tile the soak-time axis without gaps or
/// overlap, so exactly one matches any soak time; the result is a `Vec`
/// rather than an `Option` to stay faithful to the join when a non-canonical
/// `OperatingMode` table leaves a soak time uncovered (empty result) or
/// doubly covered (two ids).
#[must_use]
pub fn classify_start_op_mode(soak_time: i32, operating_modes: &[OperatingMode]) -> Vec<u16> {
    operating_modes
        .iter()
        .filter(|mode| mode.matches(soak_time))
        .map(|mode| mode.op_mode_id)
        .collect()
}

/// The result of classifying one engine start — its [`soak_time`] and the
/// operating mode(s) that soak time falls in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartClassification {
    /// Soak time before the start, `keyOnTime − keyOffTime[prior trip]`.
    pub soak_time: i32,
    /// Matching `opModeID`s — see [`classify_start_op_mode`]. Canonically a
    /// single element.
    pub op_mode_ids: Vec<u16>,
}

/// Classify one engine start: compute its soak time from the start's
/// `key_on_time` and the prior trip's `prior_key_off_time`, then resolve the
/// operating mode(s) from `operating_modes`.
///
/// This is the per-trip work `calculateSoakTime` + `calculateStartOpMode`
/// perform; the (source type, hour-day) aggregation that follows
/// ([`op_mode_fraction`]) is driven by [`execute`](Generator::execute) once
/// the data plane lands.
#[must_use]
pub fn classify_trip(
    key_on_time: i32,
    prior_key_off_time: i32,
    operating_modes: &[OperatingMode],
) -> StartClassification {
    let soak = soak_time(key_on_time, prior_key_off_time);
    StartClassification {
        soak_time: soak,
        op_mode_ids: classify_start_op_mode(soak, operating_modes),
    }
}

/// Fraction of a (source type, hour-day)'s starts that fall in one operating
/// mode — `op_mode_count / total_starts`.
///
/// `calculateOpModeFraction` step 300 writes `opModeFraction =
/// COUNT(opModeID) / starts`, where `starts` is the (source type, hour-day)
/// total from `StartsPerVehicleDay`. Both counts range over the same joined
/// rows, so the fractions of a (source type, hour-day) sum to 1.
///
/// `total_starts` is always positive for a row the generator emits — the
/// `StartOpModeDistribution` join requires a `StartsPerVehicleDay` row, and
/// that row counts the very starts being divided. Callers must uphold that;
/// `total_starts == 0` yields a non-finite result.
///
/// # Fidelity note
///
/// `COUNT()` is a MariaDB `BIGINT`. `BIGINT / BIGINT` is *exact-value*
/// (`DECIMAL`) division: the quotient's scale is the dividend's scale plus
/// `div_precision_increment`, a server variable MOVES leaves at its default
/// of 4. So the production `opModeFraction` is the ratio rounded to **four
/// decimal places** (e.g. `1/3` is stored as `0.3333`), later widened into
/// the single-precision `FLOAT` column `OpModeDistribution.opModeFraction`.
///
/// This port returns the exact `f64` ratio. The four-place rounding is a
/// divergence of up to 5 × 10⁻⁵ — larger than the `(5/9)` rounding noted in
/// `MeteorologyGenerator` — so whether to reproduce MariaDB's `DECIMAL`
/// rounding is deferred to canonical-capture comparison, which can
/// confirm the live `div_precision_increment` and rounding mode.
#[must_use]
pub fn op_mode_fraction(op_mode_count: u64, total_starts: u64) -> f64 {
    op_mode_count as f64 / total_starts as f64
}

/// Compose a `hourDayID` from its hour and day parts — `hourID * 10 + dayID`.
///
/// MOVES keys the (hour, day) pair as a single `hourDayID` throughout, and
/// `populateOperatingModeDistribution` rebuilds it inline as
/// `somd.hourID * 10 + somd.dayID` when copying start fractions into
/// `OpModeDistribution`. `hourID` runs 1–24 and `dayID` is a single digit,
/// so the product stays well inside the `SMALLINT` range `hourDayID` is
/// stored in.
///
/// ```
/// use moves_calculators::generators::start_operating_mode_distribution::hour_day_id;
///
/// // Hour 14 on day 5 (weekday) is hour-day 145.
/// assert_eq!(hour_day_id(14, 5), 145);
/// ```
#[must_use]
pub fn hour_day_id(hour_id: u16, day_id: u16) -> u16 {
    hour_id * 10 + day_id
}

/// Default-DB and execution tables the generator reads. `SampleVehicleTrip`
/// / `SampleVehicleDay` / `OperatingMode` drive the soak-time classification;
/// `HourDay` / `RunSpecHourDay` / `RunSpecSourceType` scope the aggregation;
/// `startsOpModeDistribution`, `OpModeDistribution`, `PollutantProcessAssoc`,
/// `Link`, `SourceTypePolProcess` and `OpModePolProcAssoc` feed the step-400
/// copy.
static INPUT_TABLES: &[&str] = &[
    "SampleVehicleTrip",
    "SampleVehicleDay",
    "OperatingMode",
    "HourDay",
    "RunSpecHourDay",
    "RunSpecSourceType",
    "startsOpModeDistribution",
    "OpModeDistribution",
    "PollutantProcessAssoc",
    "Link",
    "SourceTypePolProcess",
    "OpModePolProcAssoc",
];

/// Execution tables the generator writes — it appends start operating-mode
/// rows to `OpModeDistribution` (inventory runs) and `RatesOpModeDistribution`
/// (rates runs).
static OUTPUT_TABLES: &[&str] = &["OpModeDistribution", "RatesOpModeDistribution"];

// ---- row_err ----------------------------------------------------------------

fn row_err(table: &'static str, row: usize, column: &'static str, msg: String) -> Error {
    Error::RowExtraction {
        table: table.into(),
        row,
        column: column.into(),
        message: msg,
    }
}

// ---- Input row types --------------------------------------------------------

/// One `SampleVehicleTrip` row — a single trip from the sample vehicle trip
/// table. The generator self-joins this on `priorTripID` to compute soak times.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SampleVehicleTripRow {
    /// `vehID` — the sample vehicle primary key.
    pub veh_id: i32,
    /// `dayID` — the day of the week (MOVES5: 2 = weekend, 5 = weekday).
    pub day_id: i32,
    /// `tripID` — this trip's ID.
    pub trip_id: i32,
    /// `hourID` — the hour-of-day this trip starts (1–24).
    pub hour_id: i32,
    /// `priorTripID` — ID of the immediately preceding trip; `None` when
    /// there is no prior trip (i.e. this is the first trip).
    pub prior_trip_id: Option<i32>,
    /// `keyOnTime` — engine-on time (INT minutes since midnight).
    pub key_on_time: i32,
    /// `keyOffTime` — engine-off time (INT minutes since midnight).
    pub key_off_time: i32,
}

impl TableRow for SampleVehicleTripRow {
    fn table_name() -> &'static str {
        "SampleVehicleTrip"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("vehID".into(), DataType::Int32),
            ("dayID".into(), DataType::Int32),
            ("tripID".into(), DataType::Int32),
            ("hourID".into(), DataType::Int32),
            ("priorTripID".into(), DataType::Int32),
            ("keyOnTime".into(), DataType::Int32),
            ("keyOffTime".into(), DataType::Int32),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "vehID".into(),
                    rows.iter().map(|r| r.veh_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "dayID".into(),
                    rows.iter().map(|r| r.day_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "tripID".into(),
                    rows.iter().map(|r| r.trip_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "hourID".into(),
                    rows.iter().map(|r| r.hour_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "priorTripID".into(),
                    rows.iter()
                        .map(|r| r.prior_trip_id)
                        .collect::<Vec<Option<i32>>>(),
                )
                .into(),
                Series::new(
                    "keyOnTime".into(),
                    rows.iter().map(|r| r.key_on_time).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "keyOffTime".into(),
                    rows.iter().map(|r| r.key_off_time).collect::<Vec<i32>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "SampleVehicleTrip";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let veh_id = get_i32("vehID")?;
        let day_id = get_i32("dayID")?;
        let trip_id = get_i32("tripID")?;
        let hour_id = get_i32("hourID")?;
        let prior_trip_id = get_i32("priorTripID")?;
        let key_on_time = get_i32("keyOnTime")?;
        let key_off_time = get_i32("keyOffTime")?;
        // Rows with NULL keyOnTime are marker trips — skip them (Java filter).
        let mut rows = Vec::with_capacity(df.height());
        for i in 0..df.height() {
            let Some(kot) = key_on_time.get(i) else {
                continue;
            };
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            rows.push(SampleVehicleTripRow {
                veh_id: veh_id.get(i).ok_or_else(|| null("vehID"))?,
                day_id: day_id.get(i).ok_or_else(|| null("dayID"))?,
                trip_id: trip_id.get(i).ok_or_else(|| null("tripID"))?,
                hour_id: hour_id.get(i).ok_or_else(|| null("hourID"))?,
                prior_trip_id: prior_trip_id.get(i),
                key_on_time: kot,
                key_off_time: key_off_time.get(i).ok_or_else(|| null("keyOffTime"))?,
            });
        }
        Ok(rows)
    }
}

/// One `SampleVehicleDay` row — maps a `(vehID, dayID)` to a `sourceTypeID`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SampleVehicleDayRow {
    /// `vehID` — the sample vehicle.
    pub veh_id: i32,
    /// `dayID` — the day of the week.
    pub day_id: i32,
    /// `sourceTypeID` — the vehicle source type.
    pub source_type_id: i32,
}

impl TableRow for SampleVehicleDayRow {
    fn table_name() -> &'static str {
        "SampleVehicleDay"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("vehID".into(), DataType::Int32),
            ("dayID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "vehID".into(),
                    rows.iter().map(|r| r.veh_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "dayID".into(),
                    rows.iter().map(|r| r.day_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "SampleVehicleDay";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let veh_id = get_i32("vehID")?;
        let day_id = get_i32("dayID")?;
        let source_type_id = get_i32("sourceTypeID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(SampleVehicleDayRow {
                    veh_id: veh_id.get(i).ok_or_else(|| null("vehID"))?,
                    day_id: day_id.get(i).ok_or_else(|| null("dayID"))?,
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                })
            })
            .collect()
    }
}

/// One `OperatingMode` row read by the start op-mode generator — narrowed to
/// the columns the soak-time classification uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OperatingModeRow {
    /// `opModeID` — the operating mode primary key.
    pub op_mode_id: i32,
    /// `minSoakTime` — inclusive lower bound of the soak-time band (nullable).
    pub min_soak_time: Option<i32>,
    /// `maxSoakTime` — exclusive upper bound of the soak-time band (nullable).
    pub max_soak_time: Option<i32>,
}

impl TableRow for OperatingModeRow {
    fn table_name() -> &'static str {
        "OperatingMode"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("opModeID".into(), DataType::Int32),
            ("minSoakTime".into(), DataType::Int32),
            ("maxSoakTime".into(), DataType::Int32),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "opModeID".into(),
                    rows.iter().map(|r| r.op_mode_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "minSoakTime".into(),
                    rows.iter()
                        .map(|r| r.min_soak_time)
                        .collect::<Vec<Option<i32>>>(),
                )
                .into(),
                Series::new(
                    "maxSoakTime".into(),
                    rows.iter()
                        .map(|r| r.max_soak_time)
                        .collect::<Vec<Option<i32>>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "OperatingMode";
        let op_mode_id = df
            .column("opModeID")
            .map_err(|e| row_err(t, 0, "opModeID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "opModeID", e.to_string()))?;
        let min_soak = df
            .column("minSoakTime")
            .map_err(|e| row_err(t, 0, "minSoakTime", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "minSoakTime", e.to_string()))?;
        let max_soak = df
            .column("maxSoakTime")
            .map_err(|e| row_err(t, 0, "maxSoakTime", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "maxSoakTime", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(OperatingModeRow {
                    op_mode_id: op_mode_id.get(i).ok_or_else(|| null("opModeID"))?,
                    min_soak_time: min_soak.get(i),
                    max_soak_time: max_soak.get(i),
                })
            })
            .collect()
    }
}

// ---- Output row types -------------------------------------------------------

/// One `OpModeDistribution` row produced by the start op-mode generator.
///
/// Step 400 (`populateOperatingModeDistribution`) expands the per-`(sourceType,
/// hourDay, opMode)` soak fractions across the run's start `polProcessID`s — the
/// start-exhaust (process 2) / crankcase-start (process 16) processes paired
/// with recognised start pollutants, joined to `OpModePolProcAssoc` on
/// `(polProcessID, opModeID)` — matching the canonical multi-table
/// `INSERT … SELECT`. `linkID` is the `OpModeDistribution` schema's link column;
/// the start distribution is not link-scoped, so it carries the `0` sentinel (no
/// consumer joins `OpModeDistribution` on `linkID`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct StartOpModeDistributionRow {
    /// `sourceTypeID` — the MOVES source (vehicle) type.
    pub source_type_id: i32,
    /// `hourDayID` — `hourID * 10 + dayID` composite.
    pub hour_day_id: i32,
    /// `linkID` — `0` sentinel; the start distribution is not link-scoped.
    pub link_id: i32,
    /// `polProcessID` — the start pollutant/process this fraction applies to.
    pub pol_process_id: i32,
    /// `opModeID` — the start operating mode.
    pub op_mode_id: i32,
    /// `opModeFraction` — fraction of starts in this mode.
    pub op_mode_fraction: f64,
}

impl TableRow for StartOpModeDistributionRow {
    fn table_name() -> &'static str {
        "OpModeDistribution"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("sourceTypeID".into(), DataType::Int32),
            ("hourDayID".into(), DataType::Int32),
            ("linkID".into(), DataType::Int32),
            ("polProcessID".into(), DataType::Int32),
            ("opModeID".into(), DataType::Int32),
            ("opModeFraction".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "hourDayID".into(),
                    rows.iter().map(|r| r.hour_day_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "linkID".into(),
                    rows.iter().map(|r| r.link_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "polProcessID".into(),
                    rows.iter().map(|r| r.pol_process_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "opModeID".into(),
                    rows.iter().map(|r| r.op_mode_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "opModeFraction".into(),
                    rows.iter()
                        .map(|r| r.op_mode_fraction)
                        .collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "OpModeDistribution";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let source_type_id = get_i32("sourceTypeID")?;
        let hour_day_id = get_i32("hourDayID")?;
        let link_id = get_i32("linkID")?;
        let pol_process_id = get_i32("polProcessID")?;
        let op_mode_id = get_i32("opModeID")?;
        let op_mode_fraction = df
            .column("opModeFraction")
            .map_err(|e| row_err(t, 0, "opModeFraction", e.to_string()))?
            .f64()
            .map_err(|e| row_err(t, 0, "opModeFraction", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(StartOpModeDistributionRow {
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    hour_day_id: hour_day_id.get(i).ok_or_else(|| null("hourDayID"))?,
                    link_id: link_id.get(i).ok_or_else(|| null("linkID"))?,
                    pol_process_id: pol_process_id.get(i).ok_or_else(|| null("polProcessID"))?,
                    op_mode_id: op_mode_id.get(i).ok_or_else(|| null("opModeID"))?,
                    op_mode_fraction: op_mode_fraction
                        .get(i)
                        .ok_or_else(|| null("opModeFraction"))?,
                })
            })
            .collect()
    }
}

/// One `RatesOpModeDistribution` row produced by the start op-mode generator.
///
/// Step 400's `DO_RATES_FIRST` branch writes an extra op-mode-100 ("All
/// Starts") row to `RatesOpModeDistribution` with `opModeFraction = 1.0` for
/// each `(sourceTypeID, hourDayID)` cell that had at least one start, plus all
/// of the standard start-op-mode rows. Shares the same columns as
/// `OpModeDistribution` for the start-exhaust generator's purposes.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RatesOpModeDistributionRow {
    /// `sourceTypeID` — the MOVES source type.
    pub source_type_id: i32,
    /// `hourDayID` — `hourID * 10 + dayID` composite.
    pub hour_day_id: i32,
    /// `opModeID` — operating mode (100 = "All Starts"; 101+ = soak bands).
    pub op_mode_id: i32,
    /// `opModeFraction` — fraction of starts in this mode (1.0 for op-mode 100).
    pub op_mode_fraction: f64,
}

impl TableRow for RatesOpModeDistributionRow {
    fn table_name() -> &'static str {
        "RatesOpModeDistribution"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("sourceTypeID".into(), DataType::Int32),
            ("hourDayID".into(), DataType::Int32),
            ("opModeID".into(), DataType::Int32),
            ("opModeFraction".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "hourDayID".into(),
                    rows.iter().map(|r| r.hour_day_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "opModeID".into(),
                    rows.iter().map(|r| r.op_mode_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "opModeFraction".into(),
                    rows.iter()
                        .map(|r| r.op_mode_fraction)
                        .collect::<Vec<f64>>(),
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
        let source_type_id = get_i32("sourceTypeID")?;
        let hour_day_id = get_i32("hourDayID")?;
        let op_mode_id = get_i32("opModeID")?;
        let op_mode_fraction = df
            .column("opModeFraction")
            .map_err(|e| row_err(t, 0, "opModeFraction", e.to_string()))?
            .f64()
            .map_err(|e| row_err(t, 0, "opModeFraction", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(RatesOpModeDistributionRow {
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    hour_day_id: hour_day_id.get(i).ok_or_else(|| null("hourDayID"))?,
                    op_mode_id: op_mode_id.get(i).ok_or_else(|| null("opModeID"))?,
                    op_mode_fraction: op_mode_fraction
                        .get(i)
                        .ok_or_else(|| null("opModeFraction"))?,
                })
            })
            .collect()
    }
}

// ---- Kernel -----------------------------------------------------------------

/// Inputs to [`build_start_op_mode_distribution`].
#[derive(Debug, Clone)]
pub struct StartOpModeInputs {
    /// `SampleVehicleTrip` rows.
    pub trips: Vec<SampleVehicleTripRow>,
    /// `SampleVehicleDay` rows — maps `(vehID, dayID)` → `sourceTypeID`.
    pub vehicle_days: Vec<SampleVehicleDayRow>,
    /// `OperatingMode` rows — the soak-time band table.
    pub operating_modes: Vec<OperatingModeRow>,
    /// The `polProcessID`s the step-400 `OpModeDistribution` insert fans the
    /// per-`(sourceType, hourDay, opMode)` soak fractions out across — the
    /// `pollutantProcessAssoc` rows whose `processID` is start-exhaust (2) or
    /// crankcase-start (16). The canonical inventory SQL `cross join`s these
    /// (`where ppa.processID in (2,16)`), so every start op-mode fraction is
    /// emitted once per start `polProcessID`.
    pub start_pol_process_ids: Vec<i32>,
}

/// One `PollutantProcessAssoc` row — only the `polProcessID` and `processID`
/// the step-400 fan-out needs (`where ppa.processID in (2,16)`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PollutantProcessAssocRow {
    /// `polProcessID` — `pollutantID * 100 + processID`.
    pub pol_process_id: i32,
    /// `processID` — the emission process.
    pub process_id: i32,
}

impl TableRow for PollutantProcessAssocRow {
    fn table_name() -> &'static str {
        "PollutantProcessAssoc"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("polProcessID".into(), DataType::Int32),
            ("processID".into(), DataType::Int32),
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
                    "processID".into(),
                    rows.iter().map(|r| r.process_id).collect::<Vec<i32>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "PollutantProcessAssoc";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .cast(&DataType::Int32)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
                .map(|c| c.clone())
        };
        let pol_process_id = get_i32("polProcessID")?;
        let process_id = get_i32("processID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(PollutantProcessAssocRow {
                    pol_process_id: pol_process_id.get(i).ok_or_else(|| null("polProcessID"))?,
                    process_id: process_id.get(i).ok_or_else(|| null("processID"))?,
                })
            })
            .collect()
    }
}

/// Build the start operating-mode distribution rows for both
/// `OpModeDistribution` and `RatesOpModeDistribution`.
///
/// Steps 100–300 from `executeLoop`:
///
/// 1. **Soak time (step 100):** self-join `SampleVehicleTrip` on `priorTripID`
/// (INNER JOIN — only trips with a prior trip get a soak-time row).
/// 2. **Start op mode (step 200):** join each soak time against `OperatingMode`
/// soak-time bands, keeping all matching modes.
/// 3. **Op-mode fraction (step 300):** aggregate counts by
/// `(sourceTypeID, hourDayID)` and divide by total starts per cell.
/// 4. **Populate (step 400):** emit `OpModeDistribution` rows from the
/// per-mode fractions, plus `RatesOpModeDistribution` rows that include an
/// extra op-mode-100 ("All Starts") row with fraction 1.0 per cell.
///
/// Returns `(op_mode_rows, rates_rows)`.
pub fn build_start_op_mode_distribution(
    inputs: &StartOpModeInputs,
) -> (
    Vec<StartOpModeDistributionRow>,
    Vec<RatesOpModeDistributionRow>,
) {
    // Index SampleVehicleDay: (vehID, dayID) -> sourceTypeID.
    let veh_day_to_source_type: std::collections::HashMap<(i32, i32), i32> = inputs
        .vehicle_days
        .iter()
        .map(|vd| ((vd.veh_id, vd.day_id), vd.source_type_id))
        .collect();

    // Index SampleVehicleTrip: (vehID, dayID, tripID) -> keyOffTime,
    // for the prior-trip self-join.
    let trip_key_off: std::collections::HashMap<(i32, i32, i32), i32> = inputs
        .trips
        .iter()
        .map(|t| ((t.veh_id, t.day_id, t.trip_id), t.key_off_time))
        .collect();

    // Convert OperatingModeRow to OperatingMode for classify_start_op_mode.
    let op_modes: Vec<OperatingMode> = inputs
        .operating_modes
        .iter()
        .map(|r| OperatingMode {
            op_mode_id: r.op_mode_id as u16,
            min_soak_time: r.min_soak_time,
            max_soak_time: r.max_soak_time,
        })
        .collect();

    // Steps 100–200: for each trip that has a priorTripID, compute soak time
    // and classify into operating mode(s). Accumulate counts by
    // (sourceTypeID, hourDayID, opModeID).
    //
    // `counts[(source_type, hour_day, op_mode_id)]` = number of starts in mode.
    // `totals[(source_type, hour_day)]` = total starts (denominator for fraction).
    let mut counts: BTreeMap<(i32, i32, i32), u64> = BTreeMap::new();
    let mut totals: BTreeMap<(i32, i32), u64> = BTreeMap::new();

    for trip in &inputs.trips {
        // Only process trips with a prior trip (INNER JOIN on priorTripID).
        let Some(prior_trip_id) = trip.prior_trip_id else {
            continue;
        };
        let Some(&prior_key_off) = trip_key_off.get(&(trip.veh_id, trip.day_id, prior_trip_id))
        else {
            continue;
        };

        // Look up sourceTypeID from SampleVehicleDay.
        let Some(&source_type_id) = veh_day_to_source_type.get(&(trip.veh_id, trip.day_id)) else {
            continue;
        };

        // Compute soak time and classify into op mode(s).
        let soak = soak_time(trip.key_on_time, prior_key_off);
        let matched_modes = classify_start_op_mode(soak, &op_modes);

        // Compose hourDayID.
        let hd_id = hour_day_id(trip.hour_id as u16, trip.day_id as u16) as i32;

        // One start contributes to the total for this (sourceType, hourDay).
        *totals.entry((source_type_id, hd_id)).or_insert(0) += 1;

        // And one count for each matching op mode (canonically exactly one).
        for mode_id in &matched_modes {
            *counts
                .entry((source_type_id, hd_id, *mode_id as i32))
                .or_insert(0) += 1;
        }
    }

    // Step 300 + 400: compute fractions and emit rows.
    let mut op_mode_rows: Vec<StartOpModeDistributionRow> = Vec::new();
    let mut rates_rows: Vec<RatesOpModeDistributionRow> = Vec::new();

    // Collect unique (source_type, hour_day) cells for the rates "All Starts" row.
    let cells: std::collections::BTreeSet<(i32, i32)> = totals.keys().copied().collect();

    for (source_type_id, hour_day_id_val) in &cells {
        let total = *totals
            .get(&(*source_type_id, *hour_day_id_val))
            .unwrap_or(&0);
        if total == 0 {
            continue;
        }

        // Emit the op-mode-100 "All Starts" row for RatesOpModeDistribution.
        rates_rows.push(RatesOpModeDistributionRow {
            source_type_id: *source_type_id,
            hour_day_id: *hour_day_id_val,
            op_mode_id: ALL_STARTS_OP_MODE_ID as i32,
            op_mode_fraction: 1.0,
        });
    }

    // Emit per-mode fraction rows. Step 400 fans each `(sourceType, hourDay,
    // opMode)` soak fraction out across the start `polProcessID`s — the
    // `cross join pollutantProcessAssoc ... where ppa.processID in (2,16)` of
    // the canonical inventory `OpModeDistribution` insert. `linkID` is the
    // `0` sentinel (the canonical SQL takes it from the off-network `link`,
    // but no `OpModeDistribution` consumer joins on it). `RatesOpModeDistribution`
    // keeps the unfanned per-`(sourceType, hourDay, opMode)` shape (it is
    // gated out of inventory runs anyway).
    for ((source_type_id, hd_id, op_mode_id), &count) in &counts {
        let total = *totals.get(&(*source_type_id, *hd_id)).unwrap_or(&1);
        let fraction = op_mode_fraction(count, total);

        for &pol_process_id in &inputs.start_pol_process_ids {
            op_mode_rows.push(StartOpModeDistributionRow {
                source_type_id: *source_type_id,
                hour_day_id: *hd_id,
                link_id: 0,
                pol_process_id,
                op_mode_id: *op_mode_id,
                op_mode_fraction: fraction,
            });
        }
        rates_rows.push(RatesOpModeDistributionRow {
            source_type_id: *source_type_id,
            hour_day_id: *hd_id,
            op_mode_id: *op_mode_id,
            op_mode_fraction: fraction,
        });
    }

    (op_mode_rows, rates_rows)
}

/// MOVES `StartOperatingModeDistributionGenerator` ().
///
/// Builds the start-exhaust operating-mode distribution. Holds no per-run
/// state — every input arrives through the [`CalculatorContext`] passed to
/// [`Generator::execute`], as the Java holds only caches rebuilt per run.
#[derive(Debug, Default, Clone, Copy)]
pub struct StartOperatingModeDistributionGenerator;

/// Build the generator's single subscription — Start Exhaust, `PROCESS`
/// granularity, `GENERATOR` priority.
///
/// `subscribeToMe` subscribes to the Start Exhaust process at `GENERATOR`
/// priority and a granularity that depends on the `DO_RATES_FIRST`
/// compilation flag — `PROCESS` when set, `LINK` otherwise. The chain DAG
/// (`characterization/calculator-chains/calculator-dag.json`) was captured
/// from a `DO_RATES_FIRST` build and records `PROCESS`; this port follows the
/// DAG. `Priority::parse` and `CalculatorSubscription::new` are not `const`,
/// hence the [`OnceLock`] in [`Generator::subscriptions`].
fn build_subscriptions() -> Vec<CalculatorSubscription> {
    let priority =
        Priority::parse("GENERATOR").expect("\"GENERATOR\" is a canonical MasterLoopPriority base");
    vec![CalculatorSubscription::new(
        START_EXHAUST_PROCESS_ID,
        Granularity::Process,
        priority,
    )]
}

impl Generator for StartOperatingModeDistributionGenerator {
    fn name(&self) -> &'static str {
        "StartOperatingModeDistributionGenerator"
    }

    fn subscriptions(&self) -> &[CalculatorSubscription] {
        static SUBSCRIPTIONS: OnceLock<Vec<CalculatorSubscription>> = OnceLock::new();
        SUBSCRIPTIONS.get_or_init(build_subscriptions).as_slice()
    }

    fn input_tables(&self) -> &[&'static str] {
        INPUT_TABLES
    }

    fn output_tables(&self) -> &[&'static str] {
        OUTPUT_TABLES
    }

    fn execute(&self, ctx: &mut CalculatorContext) -> Result<CalculatorOutput, Error> {
        // Step 400 fans the soak fractions out across the start `polProcessID`s —
        // `pollutantProcessAssoc` filtered to the start-exhaust (2) and
        // crankcase-start (16) processes (the canonical `cross join ... where
        // ppa.processID in (2,16)`).
        let start_pol_process_ids: Vec<i32> = ctx
            .tables()
            .iter_typed::<PollutantProcessAssocRow>("PollutantProcessAssoc")?
            .into_iter()
            .filter(|r| {
                r.process_id == START_EXHAUST_PROCESS_ID.0 as i32
                    || r.process_id == CRANKCASE_START_EXHAUST_PROCESS_ID.0 as i32
            })
            .map(|r| r.pol_process_id)
            .collect();

        // Read the trip/op-mode input tables the kernel needs.
        let inputs = StartOpModeInputs {
            trips: ctx.tables().iter_typed("SampleVehicleTrip")?,
            vehicle_days: ctx.tables().iter_typed("SampleVehicleDay")?,
            operating_modes: ctx.tables().iter_typed("OperatingMode")?,
            start_pol_process_ids,
        };

        // Run steps 100–400: soak time → start op mode → op-mode fraction →
        // populate both output tables.
        let (op_mode_rows, rates_rows) = build_start_op_mode_distribution(&inputs);

        // Write OpModeDistribution to scratch.
        crate::wiring::write_scratch_table(ctx, OUTPUT_TABLES[0], op_mode_rows)?;

        // RatesOpModeDistribution is a rates-mode table. This generator's start
        // rows carry only a narrow schema (no canonical `roadTypeID`/etc.), and
        // in an inventory run it is the *only* writer of the table — so emitting
        // it leaves a schema-incompatible table that breaks strict extraction in
        // every reader (BaseRateGenerator, SourceTypePhysics). No inventory
        // reader needs it, so only emit it outside inventory mode (rates/project,
        // and the `None` default used by unit tests).
        if ctx.model_scale() != Some(ModelScale::Inventory) {
            let rates_df = RatesOpModeDistributionRow::into_dataframe(rates_rows)
                .map_err(|e| Error::Polars(e.to_string()))?;
            ctx.scratch_mut().insert(OUTPUT_TABLES[1], rates_df);
        }

        Ok(CalculatorOutput::empty())
    }
}

/// Construct a [`StartOperatingModeDistributionGenerator`] as a boxed trait
/// object — the shape the engine's `GeneratorFactory` wiring registers via
/// `CalculatorRegistry::register_generator`.
#[must_use]
pub fn factory() -> Box<dyn Generator> {
    Box::new(StartOperatingModeDistributionGenerator)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- soak_time ----

    #[test]
    fn soak_time_is_key_on_minus_prior_key_off() {
        // The engine sat off from the prior trip's key-off (480) until this
        // trip's key-on (540).
        assert_eq!(soak_time(540, 480), 60);
        assert_eq!(soak_time(1000, 0), 1000);
    }

    #[test]
    fn soak_time_can_be_negative_for_overlapping_trips() {
        // The Java does not floor the difference; overlapping sample trips
        // give a negative soak time.
        assert_eq!(soak_time(100, 130), -30);
        assert_eq!(soak_time(0, 0), 0);
    }

    // ---- OperatingMode::matches ----

    #[test]
    fn closed_band_matches_as_half_open_interval() {
        // [min, max): min inclusive, max exclusive.
        let mode = OperatingMode {
            op_mode_id: 102,
            min_soak_time: Some(10),
            max_soak_time: Some(20),
        };
        assert!(mode.matches(10), "lower bound is inclusive");
        assert!(mode.matches(15));
        assert!(mode.matches(19));
        assert!(!mode.matches(20), "upper bound is exclusive");
        assert!(!mode.matches(9));
        assert!(!mode.matches(21));
    }

    #[test]
    fn open_below_band_matches_everything_under_max() {
        // minSoakTime NULL, maxSoakTime present: `soak < max`.
        let mode = OperatingMode {
            op_mode_id: 101,
            min_soak_time: None,
            max_soak_time: Some(6),
        };
        assert!(mode.matches(-100));
        assert!(mode.matches(0));
        assert!(mode.matches(5));
        assert!(!mode.matches(6));
        assert!(!mode.matches(7));
    }

    #[test]
    fn open_above_band_matches_everything_from_min() {
        // minSoakTime present, maxSoakTime NULL: `soak >= min`.
        let mode = OperatingMode {
            op_mode_id: 150,
            min_soak_time: Some(720),
            max_soak_time: None,
        };
        assert!(!mode.matches(719));
        assert!(mode.matches(720));
        assert!(mode.matches(100_000));
    }

    #[test]
    fn band_with_no_bounds_never_matches() {
        // Both NULL: each WHERE clause is SQL UNKNOWN, never selected.
        let mode = OperatingMode {
            op_mode_id: 999,
            min_soak_time: None,
            max_soak_time: None,
        };
        assert!(!mode.matches(0));
        assert!(!mode.matches(-1));
        assert!(!mode.matches(i32::MAX));
    }

    // ---- classify_start_op_mode ----

    /// Three modes tiling the soak axis: `(-∞,60)`, `[60,360)`, `[360,∞)`.
    fn partitioned_modes() -> [OperatingMode; 3] {
        [
            OperatingMode {
                op_mode_id: 101,
                min_soak_time: None,
                max_soak_time: Some(60),
            },
            OperatingMode {
                op_mode_id: 102,
                min_soak_time: Some(60),
                max_soak_time: Some(360),
            },
            OperatingMode {
                op_mode_id: 103,
                min_soak_time: Some(360),
                max_soak_time: None,
            },
        ]
    }

    #[test]
    fn partitioned_modes_classify_to_exactly_one() {
        let modes = partitioned_modes();
        assert_eq!(classify_start_op_mode(30, &modes), vec![101]);
        assert_eq!(
            classify_start_op_mode(60, &modes),
            vec![102],
            "60 is [60,360)"
        );
        assert_eq!(classify_start_op_mode(200, &modes), vec![102]);
        assert_eq!(
            classify_start_op_mode(360, &modes),
            vec![103],
            "360 is [360,∞)"
        );
        assert_eq!(classify_start_op_mode(5_000, &modes), vec![103]);
    }

    #[test]
    fn a_soak_time_in_a_gap_classifies_to_no_mode() {
        // Non-canonical table with a hole between [0,10) and [20,30).
        let modes = [
            OperatingMode {
                op_mode_id: 1,
                min_soak_time: Some(0),
                max_soak_time: Some(10),
            },
            OperatingMode {
                op_mode_id: 2,
                min_soak_time: Some(20),
                max_soak_time: Some(30),
            },
        ];
        assert!(classify_start_op_mode(15, &modes).is_empty());
    }

    #[test]
    fn overlapping_bands_yield_every_match_in_input_order() {
        // The INNER JOIN emits a row per match; order follows the slice.
        let modes = [
            OperatingMode {
                op_mode_id: 8,
                min_soak_time: Some(10),
                max_soak_time: Some(30),
            },
            OperatingMode {
                op_mode_id: 7,
                min_soak_time: Some(0),
                max_soak_time: Some(20),
            },
        ];
        assert_eq!(classify_start_op_mode(15, &modes), vec![8, 7]);
    }

    // ---- classify_trip ----

    #[test]
    fn classify_trip_combines_soak_time_and_classification() {
        let modes = partitioned_modes();
        // key-on 800, prior key-off 500 → soak 300 → mode 102.
        let result = classify_trip(800, 500, &modes);
        assert_eq!(
            result,
            StartClassification {
                soak_time: 300,
                op_mode_ids: vec![102],
            }
        );
    }

    #[test]
    fn classify_trip_with_no_modes_yields_no_classification() {
        let result = classify_trip(800, 500, &[]);
        assert_eq!(result.soak_time, 300);
        assert!(result.op_mode_ids.is_empty());
    }

    // ---- op_mode_fraction ----

    #[test]
    fn op_mode_fraction_is_count_over_starts() {
        assert!((op_mode_fraction(3, 4) - 0.75).abs() < 1e-12);
        assert!((op_mode_fraction(7, 7) - 1.0).abs() < 1e-12);
    }

    #[test]
    fn op_mode_fraction_of_a_bucket_sums_to_one() {
        // Every start of a (source type, hour-day) lands in exactly one mode,
        // so the per-mode fractions sum to 1.
        let starts = 10;
        let sum: f64 = [3_u64, 5, 2]
            .iter()
            .map(|&count| op_mode_fraction(count, starts))
            .sum();
        assert!((sum - 1.0).abs() < 1e-12, "fractions summed to {sum}");
    }

    #[test]
    fn op_mode_fraction_of_zero_count_is_zero() {
        assert_eq!(op_mode_fraction(0, 12), 0.0);
    }

    // ---- hour_day_id ----

    #[test]
    fn hour_day_id_packs_hour_and_day() {
        assert_eq!(hour_day_id(14, 5), 145);
        assert_eq!(hour_day_id(1, 2), 12);
        assert_eq!(hour_day_id(24, 5), 245);
    }

    // ---- pollutant filter ----

    #[test]
    fn recognized_pollutants_are_the_twelve_start_exhaust_species() {
        assert_eq!(RECOGNIZED_START_EXHAUST_POLLUTANTS.len(), 12);
        // Ascending by id, no duplicates.
        assert!(RECOGNIZED_START_EXHAUST_POLLUTANTS
            .windows(2)
            .all(|w| w[0].0 < w[1].0));
        // Spot-check the endpoints: THC (1) and Composite – NonECPM (118).
        assert!(is_recognized_start_exhaust_pollutant(PollutantId(1)));
        assert!(is_recognized_start_exhaust_pollutant(PollutantId(118)));
    }

    #[test]
    fn unlisted_pollutant_is_not_recognized() {
        // Methane (5) and Benzene (20) are not in the start-exhaust set.
        assert!(!is_recognized_start_exhaust_pollutant(PollutantId(5)));
        assert!(!is_recognized_start_exhaust_pollutant(PollutantId(20)));
    }

    #[test]
    fn pol_process_filter_requires_the_start_exhaust_process() {
        // CO is recognised, but only paired with Start Exhaust (2).
        let co_start = PollutantProcessAssociation {
            pollutant_id: PollutantId(2),
            process_id: START_EXHAUST_PROCESS_ID,
        };
        let co_running = PollutantProcessAssociation {
            pollutant_id: PollutantId(2),
            process_id: ProcessId(1), // Running Exhaust
        };
        assert!(is_recognized_start_exhaust_pol_process(co_start));
        assert!(!is_recognized_start_exhaust_pol_process(co_running));
    }

    #[test]
    fn pol_process_filter_rejects_an_unrecognized_pollutant() {
        // Methane (5) at the start-exhaust process is still rejected.
        let ch4_start = PollutantProcessAssociation {
            pollutant_id: PollutantId(5),
            process_id: START_EXHAUST_PROCESS_ID,
        };
        assert!(!is_recognized_start_exhaust_pol_process(ch4_start));
    }

    // ---- Generator trait ----

    #[test]
    fn generator_name_matches_java_class() {
        assert_eq!(
            StartOperatingModeDistributionGenerator.name(),
            "StartOperatingModeDistributionGenerator"
        );
    }

    #[test]
    fn generator_has_a_single_start_exhaust_subscription() {
        // The chain DAG records one subscription: Start Exhaust, PROCESS
        // granularity, GENERATOR priority.
        let subs = StartOperatingModeDistributionGenerator.subscriptions();
        assert_eq!(subs.len(), 1);
        assert_eq!(subs[0].process_id, START_EXHAUST_PROCESS_ID);
        assert_eq!(subs[0].granularity, Granularity::Process);
        assert_eq!(subs[0].priority.display().as_str(), "GENERATOR");
    }

    #[test]
    fn generator_declares_input_and_output_tables() {
        let gen = StartOperatingModeDistributionGenerator;
        let inputs = gen.input_tables();
        assert!(inputs.contains(&"SampleVehicleTrip"));
        assert!(inputs.contains(&"OperatingMode"));
        assert_eq!(
            gen.output_tables(),
            &["OpModeDistribution", "RatesOpModeDistribution"]
        );
    }

    #[test]
    fn generator_has_no_upstream() {
        // The DAG lists no `depends_on` — the generator is a root subscriber.
        assert!(StartOperatingModeDistributionGenerator
            .upstream()
            .is_empty());
    }

    #[test]
    fn generator_subscriptions_are_stable_across_calls() {
        // The OnceLock-backed slice is identical on every call.
        let first = StartOperatingModeDistributionGenerator.subscriptions();
        let second = StartOperatingModeDistributionGenerator.subscriptions();
        assert_eq!(first, second);
    }

    #[test]
    fn execute_writes_both_output_tables_to_scratch() {
        // Integration test: seed SampleVehicleTrip / SampleVehicleDay /
        // OperatingMode, run execute(), and verify both OpModeDistribution
        // and RatesOpModeDistribution appear in scratch with correct contents.
        use moves_framework::{DataFrameStore, InMemoryStore};

        // Two trips for vehicle 1, day 5 (weekday):
        // trip 1: no priorTripID (first trip of the day — no soak time).
        // trip 2: priorTripID = 1, key-on at 540, prior key-off at 480
        // → soak time = 60.
        // OperatingMode: [(-∞,60) → 101], [60,∞) → 102].
        // soak = 60 falls in [60,∞) → opModeID 102.
        // sourceTypeID for (vehID=1, dayID=5) is 21.
        // hourID for trip 2 is 9 (8 am–9 am slot), dayID 5.
        // hourDayID = 9*10+5 = 95.
        // Only one start: opModeFraction(102) = 1/1 = 1.0.
        // RatesOpModeDistribution also gets opMode 100 with fraction 1.0.

        let mut store = InMemoryStore::new();
        store.insert(
            "SampleVehicleTrip",
            SampleVehicleTripRow::into_dataframe(vec![
                SampleVehicleTripRow {
                    veh_id: 1,
                    day_id: 5,
                    trip_id: 1,
                    hour_id: 8,
                    prior_trip_id: None,
                    key_on_time: 400,
                    key_off_time: 480,
                },
                SampleVehicleTripRow {
                    veh_id: 1,
                    day_id: 5,
                    trip_id: 2,
                    hour_id: 9,
                    prior_trip_id: Some(1),
                    key_on_time: 540,
                    key_off_time: 620,
                },
            ])
            .unwrap(),
        );
        store.insert(
            "SampleVehicleDay",
            SampleVehicleDayRow::into_dataframe(vec![SampleVehicleDayRow {
                veh_id: 1,
                day_id: 5,
                source_type_id: 21,
            }])
            .unwrap(),
        );
        store.insert(
            "OperatingMode",
            OperatingModeRow::into_dataframe(vec![
                OperatingModeRow {
                    op_mode_id: 101,
                    min_soak_time: None,
                    max_soak_time: Some(60),
                },
                OperatingModeRow {
                    op_mode_id: 102,
                    min_soak_time: Some(60),
                    max_soak_time: None,
                },
            ])
            .unwrap(),
        );

        // Step 400 fans the soak fractions across start polProcessIDs (processes
        // 2 and 16); the running process (1) row is filtered out.
        store.insert(
            "PollutantProcessAssoc",
            PollutantProcessAssocRow::into_dataframe(vec![
                PollutantProcessAssocRow {
                    pol_process_id: 202,
                    process_id: 2,
                },
                PollutantProcessAssocRow {
                    pol_process_id: 302,
                    process_id: 2,
                },
                PollutantProcessAssocRow {
                    pol_process_id: 216,
                    process_id: 16,
                },
                PollutantProcessAssocRow {
                    pol_process_id: 201,
                    process_id: 1,
                },
            ])
            .unwrap(),
        );

        let mut ctx = CalculatorContext::with_tables(store);
        let out = StartOperatingModeDistributionGenerator
            .execute(&mut ctx)
            .expect("execute ok");
        // Generator writes to scratch — main output is empty.
        assert!(out.dataframe().is_none());

        // Read back OpModeDistribution — one soak op-mode (102) fanned out across
        // the 3 start polProcessIDs (202, 302, 216).
        let omd: Vec<StartOpModeDistributionRow> = ctx
            .scratch()
            .store
            .iter_typed("OpModeDistribution")
            .expect("OpModeDistribution in scratch");
        assert_eq!(omd.len(), 3, "1 op-mode × 3 start polProcessIDs");
        let pps: std::collections::BTreeSet<i32> = omd.iter().map(|r| r.pol_process_id).collect();
        assert_eq!(
            pps,
            [202, 216, 302]
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>()
        );
        for r in &omd {
            assert_eq!(r.source_type_id, 21);
            // hourDayID = hourID*10 + dayID = 9*10 + 5 = 95
            assert_eq!(r.hour_day_id, 95);
            assert_eq!(r.link_id, 0);
            assert_eq!(r.op_mode_id, 102); // soak = 60, which is [60,∞)
            assert!((r.op_mode_fraction - 1.0).abs() < 1e-12);
        }

        // Read back RatesOpModeDistribution.
        let romd: Vec<RatesOpModeDistributionRow> = ctx
            .scratch()
            .store
            .iter_typed("RatesOpModeDistribution")
            .expect("RatesOpModeDistribution in scratch");
        // Expect: op-mode 100 (All Starts, fraction 1.0) + op-mode 102 (fraction 1.0).
        assert_eq!(romd.len(), 2, "rates: All-Starts row + per-mode row");
        let all_starts = romd
            .iter()
            .find(|r| r.op_mode_id == 100)
            .expect("op-mode 100 present");
        assert_eq!(all_starts.source_type_id, 21);
        assert_eq!(all_starts.hour_day_id, 95);
        assert!((all_starts.op_mode_fraction - 1.0).abs() < 1e-12);
        let mode_102 = romd
            .iter()
            .find(|r| r.op_mode_id == 102)
            .expect("op-mode 102 present");
        assert!((mode_102.op_mode_fraction - 1.0).abs() < 1e-12);
    }

    #[test]
    fn execute_drops_first_trips_with_no_prior_trip() {
        // Trip 1 has no prior trip → no soak time → no start row.
        // Only trips with a priorTripID contribute starts.
        use moves_framework::{DataFrameStore, InMemoryStore};

        let mut store = InMemoryStore::new();
        store.insert(
            "SampleVehicleTrip",
            SampleVehicleTripRow::into_dataframe(vec![SampleVehicleTripRow {
                veh_id: 1,
                day_id: 5,
                trip_id: 1,
                hour_id: 8,
                prior_trip_id: None,
                key_on_time: 400,
                key_off_time: 480,
            }])
            .unwrap(),
        );
        store.insert(
            "SampleVehicleDay",
            SampleVehicleDayRow::into_dataframe(vec![SampleVehicleDayRow {
                veh_id: 1,
                day_id: 5,
                source_type_id: 21,
            }])
            .unwrap(),
        );
        store.insert(
            "OperatingMode",
            OperatingModeRow::into_dataframe(vec![OperatingModeRow {
                op_mode_id: 101,
                min_soak_time: None,
                max_soak_time: Some(60),
            }])
            .unwrap(),
        );

        store.insert(
            "PollutantProcessAssoc",
            PollutantProcessAssocRow::into_dataframe(vec![PollutantProcessAssocRow {
                pol_process_id: 202,
                process_id: 2,
            }])
            .unwrap(),
        );

        let mut ctx = CalculatorContext::with_tables(store);
        StartOperatingModeDistributionGenerator
            .execute(&mut ctx)
            .expect("execute ok");

        let omd: Vec<StartOpModeDistributionRow> = ctx
            .scratch()
            .store
            .iter_typed("OpModeDistribution")
            .expect("table present");
        assert!(omd.is_empty(), "no prior-trip → no output rows");

        let romd: Vec<RatesOpModeDistributionRow> = ctx
            .scratch()
            .store
            .iter_typed("RatesOpModeDistribution")
            .expect("table present");
        assert!(romd.is_empty(), "no prior-trip → no rates rows");
    }

    #[test]
    fn execute_aggregates_two_starts_across_op_modes() {
        // Two trips with priorTripID, one classifying into mode 101, one into 102.
        // Fractions: mode 101 = 1/2 = 0.5, mode 102 = 1/2 = 0.5.
        use moves_framework::{DataFrameStore, InMemoryStore};

        let mut store = InMemoryStore::new();
        store.insert(
            "SampleVehicleTrip",
            SampleVehicleTripRow::into_dataframe(vec![
                // trip 1: no prior (anchor trip)
                SampleVehicleTripRow {
                    veh_id: 1,
                    day_id: 5,
                    trip_id: 1,
                    hour_id: 8,
                    prior_trip_id: None,
                    key_on_time: 0,
                    key_off_time: 30,
                },
                // trip 2: soak = 540 - 30 = 510 → [360,∞) which is NOT in these modes; but we use simpler modes:
                // Let's set soak = 540 - 30 = 510. With mode 101 = (-∞,60) and 102 = [60,∞), it goes to 102.
                SampleVehicleTripRow {
                    veh_id: 1,
                    day_id: 5,
                    trip_id: 2,
                    hour_id: 9,
                    prior_trip_id: Some(1),
                    key_on_time: 540,
                    key_off_time: 600,
                },
                // trip 3: soak = 601 - 600 = 1 → mode 101 (-∞,60).
                SampleVehicleTripRow {
                    veh_id: 1,
                    day_id: 5,
                    trip_id: 3,
                    hour_id: 10,
                    prior_trip_id: Some(2),
                    key_on_time: 601,
                    key_off_time: 660,
                },
            ])
            .unwrap(),
        );
        store.insert(
            "SampleVehicleDay",
            SampleVehicleDayRow::into_dataframe(vec![SampleVehicleDayRow {
                veh_id: 1,
                day_id: 5,
                source_type_id: 21,
            }])
            .unwrap(),
        );
        store.insert(
            "OperatingMode",
            OperatingModeRow::into_dataframe(vec![
                OperatingModeRow {
                    op_mode_id: 101,
                    min_soak_time: None,
                    max_soak_time: Some(60),
                },
                OperatingModeRow {
                    op_mode_id: 102,
                    min_soak_time: Some(60),
                    max_soak_time: None,
                },
            ])
            .unwrap(),
        );

        store.insert(
            "PollutantProcessAssoc",
            PollutantProcessAssocRow::into_dataframe(vec![PollutantProcessAssocRow {
                pol_process_id: 202,
                process_id: 2,
            }])
            .unwrap(),
        );

        let mut ctx = CalculatorContext::with_tables(store);
        StartOperatingModeDistributionGenerator
            .execute(&mut ctx)
            .expect("execute ok");

        let omd: Vec<StartOpModeDistributionRow> = ctx
            .scratch()
            .store
            .iter_typed("OpModeDistribution")
            .expect("table present");
        // Two starts across two different (hour_day, op_mode) cells — different hourIDs,
        // so two distinct hourDayIDs: 9*10+5=95 and 10*10+5=105. One start
        // polProcessID seeded (202) → each cell emits exactly one row.
        assert_eq!(omd.len(), 2, "one mode per distinct (hourDay, opMode) cell");
        for row in &omd {
            assert_eq!(row.source_type_id, 21);
            assert_eq!(row.pol_process_id, 202);
            assert_eq!(row.link_id, 0);
            // Each cell has exactly 1 start of 1 total → fraction 1.0.
            assert!(
                (row.op_mode_fraction - 1.0).abs() < 1e-12,
                "fraction for op_mode {} hourDay {}: {}",
                row.op_mode_id,
                row.hour_day_id,
                row.op_mode_fraction
            );
        }
    }

    #[test]
    fn generator_execute_is_ok() {
        // Smoke-test that execute is callable with an empty context.
        // Empty tables cause iter_typed to fail, so we seed minimal tables.
        use moves_framework::{DataFrameStore, InMemoryStore};
        let mut store = InMemoryStore::new();
        store.insert(
            "SampleVehicleTrip",
            SampleVehicleTripRow::into_dataframe(vec![]).unwrap(),
        );
        store.insert(
            "SampleVehicleDay",
            SampleVehicleDayRow::into_dataframe(vec![]).unwrap(),
        );
        store.insert(
            "OperatingMode",
            OperatingModeRow::into_dataframe(vec![]).unwrap(),
        );
        store.insert(
            "PollutantProcessAssoc",
            PollutantProcessAssocRow::into_dataframe(vec![]).unwrap(),
        );
        let mut ctx = CalculatorContext::with_tables(store);
        assert!(StartOperatingModeDistributionGenerator
            .execute(&mut ctx)
            .is_ok());
    }

    #[test]
    fn factory_builds_a_named_generator() {
        assert_eq!(factory().name(), "StartOperatingModeDistributionGenerator");
    }

    #[test]
    fn generator_is_object_safe() {
        let gens: Vec<Box<dyn Generator>> =
            vec![factory(), Box::new(StartOperatingModeDistributionGenerator)];
        assert_eq!(gens.len(), 2);
        assert!(gens
            .iter()
            .all(|g| g.name() == "StartOperatingModeDistributionGenerator"));
    }
}
