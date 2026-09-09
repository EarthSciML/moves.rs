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
//! 4. **Populate (step 400).** `populateOperatingModeDistribution` does *not*
//! read the `StartOpModeDistribution` the preceding step just computed.
//! Outside the project domain it reads the default-database
//! **`startsOpModeDistribution`** — a different table, one dimension wider
//! (`ageID`) and keyed on `(hourID, dayID)` rather than `hourDayID`. It
//! `SELECT DISTINCT`s the age dimension away, cross-joins the run's
//! `pollutantProcessAssoc` rows whose `processID` is 2 or 16, and inserts
//! the result into `RatesOpModeDistribution` (`DO_RATES_FIRST`, always
//! true in the pinned tree) or `OpModeDistribution` (the dead `else`
//! branch). A second statement then adds the "All Starts" row — op mode
//! 100, fraction 1, and the **literal** `polProcessID` 602 — for every
//! `runSpecSourceType` x `runSpecHourDay` cell.
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
//! Step 400 is ported as [`populate_rates_op_mode_distribution`] and
//! [`populate_op_mode_distribution`] — relational copies out of
//! `startsOpModeDistribution` with no arithmetic of their own. The
//! `existingStartOMD` / `SOMDGOpModes` bookkeeping tables are step-300
//! scratch that nothing downstream reads and are not ported.
//!
//! # Two steps, two tables — naming the partition
//!
//! Steps 100–300 and step 400 write *different* tables from *different*
//! inputs, and this module keeps them apart:
//!
//! | Step | Output table | Built from |
//! |-------|--------------------------------------------------|-------------------------------|
//! | 300 | `StartOpModeDistribution` | `SampleVehicleTrip` soak times |
//! | 400 | `RatesOpModeDistribution` / `OpModeDistribution` | `startsOpModeDistribution` |
//!
//! Modelling step 400 as a copy of the step-300 soak fractions — what this
//! port did before — put the wrong numbers in the wrong table. On the
//! `sample-runspec` fixture that is `opModeFraction` **0.4444** for source
//! type 21 / hour-day 72 / op mode 101, where canonical MOVES emits
//! **0.124754**.
//!
//! # Numeric fidelity
//!
//! Two storage effects the canonical captures pin down:
//!
//! * Step 300's `COUNT(opModeID)/starts` is MariaDB *exact-value*
//! (`DECIMAL`) division, not the IEEE ratio — see [`op_mode_fraction`].
//! * `RatesOpModeDistribution.opModeFraction` and
//! `OpModeDistribution.opModeFraction` are `FLOAT` (single-precision)
//! columns, so a canonical capture of them carries ~6 significant
//! digits. This port stores `f64` throughout, as the rest of the port
//! does; a capture comparison has to allow the `f32` round-trip.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::OnceLock;

use moves_calculator_info::{Granularity, Priority};
use moves_data::{PollutantId, PollutantProcessAssociation, ProcessId};
use moves_framework::{
    CalculatorContext, CalculatorOutput, CalculatorSubscription, DataFrameStoreTyped, Error,
    Generator, TableRow,
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

/// `polProcessID` 602 — Nitrous Oxide (pollutant 6) / Start Exhaust
/// (process 2). Step 400's "All Starts" statement stamps this **literal**
/// onto its op-mode-100 rows (`602 as polProcessID`) whatever the run
/// selects: the canonical captures carry 602 rows in fixtures whose
/// `pollutantProcessAssoc` holds no 602 at all (`expand-criteria`, whose
/// start pol-processes are 102/202/302/3102).
pub const ALL_STARTS_POL_PROCESS_ID: i32 = 602;

/// `roadTypeID` 1 — Off-Network. Every step-400 row carries it
/// (`1 as roadTypeID`); engine starts happen off-network.
pub const OFF_NETWORK_ROAD_TYPE_ID: i32 = 1;

/// `avgSpeedBinID` 0 — start rows are not speed-binned
/// (`0 as avgSpeedBinID`).
pub const START_AVG_SPEED_BIN_ID: i32 = 0;

/// MariaDB's `div_precision_increment` — the decimal places an exact-value
/// (`DECIMAL`) division carries beyond the dividend's scale. MOVES leaves the
/// server at its default of 4, so `COUNT(...)/starts` (an integer dividend,
/// scale 0) lands on four decimal places. See [`op_mode_fraction`].
pub const DIV_PRECISION_INCREMENT: u32 = 4;

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
/// mode — `op_mode_count / total_starts`, **as MariaDB divides it**.
///
/// `calculateOpModeFraction` step 300 writes `opModeFraction =
/// COUNT(opModeID) / starts`, where `starts` is the (source type, hour-day)
/// total from `StartsPerVehicleDay`. Both counts range over the same joined
/// rows, so the fractions of a (source type, hour-day) sum to 1.
///
/// `total_starts` is always positive for a row the generator emits — the
/// `StartOpModeDistribution` join requires a `StartsPerVehicleDay` row, and
/// that row counts the very starts being divided. Callers must uphold that;
/// `total_starts == 0` falls back to the IEEE quotient (`0/0` is NaN, `n/0`
/// is infinite).
///
/// # Fidelity note — this is the rounded quotient, not the IEEE ratio
///
/// `COUNT()` is a MariaDB `BIGINT`, and `BIGINT / BIGINT` is *exact-value*
/// (`DECIMAL`) division: the quotient's scale is the dividend's scale (0)
/// plus [`DIV_PRECISION_INCREMENT`], the `div_precision_increment` server
/// variable MOVES leaves at its default of 4. So the production
/// `opModeFraction` is the ratio rounded half-away-from-zero to **four
/// decimal places** — `1/3` is stored as `0.3333`.
///
/// Measured over the 124 `StartOpModeDistribution` rows the canonical
/// captures hold (the nine onroad fixtures whose traces run this
/// generator):
///
/// | candidate | bit-exact rows | worst relative error |
/// |------------------------------|---------------:|---------------------:|
/// | rounded quotient (`DECIMAL`) | **124 / 124** | — |
/// | exact IEEE ratio | 15 / 124 | 5.767e-03 |
///
/// Note this is the *opposite* answer to the `(5/9)` question in
/// `MeteorologyGenerator`, where the exact ratio wins. Neither is a general
/// rule about MariaDB: each candidate has to be evaluated against the
/// reference capture.
///
/// The rounding is done in integer arithmetic —
/// `floor((2·n·10⁴ + d) / (2·d))` — so it is exact for every input rather
/// than a float `round()` that can land on the wrong side of a tie.
#[must_use]
pub fn op_mode_fraction(op_mode_count: u64, total_starts: u64) -> f64 {
    if total_starts == 0 {
        // Contract violation; keep the IEEE behaviour the docs promise.
        return op_mode_count as f64 / total_starts as f64;
    }
    let scale = 10_u128.pow(DIV_PRECISION_INCREMENT);
    let numerator = u128::from(op_mode_count) * scale;
    let denominator = u128::from(total_starts);
    // floor(q + 1/2) with q = numerator/denominator — round half up, exactly.
    let scaled = (2 * numerator + denominator) / (2 * denominator);
    scaled as f64 / scale as f64
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

/// Default-DB and execution tables the generator reads.
///
/// `SampleVehicleTrip` / `SampleVehicleDay` / `OperatingMode` drive the
/// steps-100–300 soak-time classification and `RunSpecHourDay` scopes its
/// aggregation; `startsOpModeDistribution`, `RunSpecPollutantProcess`,
/// `RunSpecSourceType` and `RunSpecHourDay` feed the step-400 copy.
static INPUT_TABLES: &[&str] = &[
    "SampleVehicleTrip",
    "SampleVehicleDay",
    "OperatingMode",
    "RunSpecHourDay",
    "RunSpecSourceType",
    "startsOpModeDistribution",
    "RunSpecPollutantProcess",
];

/// Execution tables the generator writes — the step-300
/// `StartOpModeDistribution` soak fractions, and the two step-400 copies of
/// `startsOpModeDistribution`.
static OUTPUT_TABLES: &[&str] = &[
    "StartOpModeDistribution",
    "OpModeDistribution",
    "RatesOpModeDistribution",
];

/// Per-generator scratch marker (see [`crate::wiring::merge_op_mode_distribution`]).
const START_OMDG_DONE_MARKER: &str = "__omdg_done__StartOperatingModeDistributionGenerator";

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

/// One `startsOpModeDistribution` row — the **default-database** start
/// operating-mode distribution, and the table step 400 copies out.
///
/// Distinct from the `StartOpModeDistribution` steps 100–300 compute: this
/// one carries an `ageID` dimension and keys the hour/day as two columns.
/// `opModeFraction` is a `DOUBLE` here (`database/CreateDefault.sql`),
/// narrowed to `FLOAT` only when step 400 writes it into
/// `RatesOpModeDistribution` / `OpModeDistribution`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct StartsOpModeDistributionRow {
    /// `dayID` — the day of the week.
    pub day_id: i32,
    /// `hourID` — the hour of the day (1–24).
    pub hour_id: i32,
    /// `sourceTypeID` — the MOVES source (vehicle) type.
    pub source_type_id: i32,
    /// `ageID` — vehicle age in years. Step 400 `DISTINCT`s this away.
    pub age_id: i32,
    /// `opModeID` — the start operating mode.
    pub op_mode_id: i32,
    /// `opModeFraction` — the share of this cell's starts in the mode.
    pub op_mode_fraction: f64,
}

impl TableRow for StartsOpModeDistributionRow {
    fn table_name() -> &'static str {
        "startsOpModeDistribution"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("dayID".into(), DataType::Int32),
            ("hourID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("ageID".into(), DataType::Int32),
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
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "ageID".into(),
                    rows.iter().map(|r| r.age_id).collect::<Vec<i32>>(),
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
        let t = "startsOpModeDistribution";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let day_id = get_i32("dayID")?;
        let hour_id = get_i32("hourID")?;
        let source_type_id = get_i32("sourceTypeID")?;
        let age_id = get_i32("ageID")?;
        let op_mode_id = get_i32("opModeID")?;
        let op_mode_fraction = df
            .column("opModeFraction")
            .map_err(|e| row_err(t, 0, "opModeFraction", e.to_string()))?
            .f64()
            .map_err(|e| row_err(t, 0, "opModeFraction", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(StartsOpModeDistributionRow {
                    day_id: day_id.get(i).ok_or_else(|| null("dayID"))?,
                    hour_id: hour_id.get(i).ok_or_else(|| null("hourID"))?,
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    age_id: age_id.get(i).ok_or_else(|| null("ageID"))?,
                    op_mode_id: op_mode_id.get(i).ok_or_else(|| null("opModeID"))?,
                    op_mode_fraction: op_mode_fraction
                        .get(i)
                        .ok_or_else(|| null("opModeFraction"))?,
                })
            })
            .collect()
    }
}

/// One `RunSpecPollutantProcess` row — the `polProcessID`s the run selects
/// (post chain-expansion). See
/// [`PopulateOpModeDistributionInputs::start_pol_process_ids`] for why this
/// stands in for the execution database's run-scoped `pollutantProcessAssoc`.
/// `processID = polProcessID % 100`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunSpecPollutantProcessRow {
    /// `polProcessID` — `pollutantID * 100 + processID`.
    pub pol_process_id: i32,
}

impl TableRow for RunSpecPollutantProcessRow {
    fn table_name() -> &'static str {
        "RunSpecPollutantProcess"
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
        let t = "RunSpecPollutantProcess";
        let pol_process_id = df
            .column("polProcessID")
            .map_err(|e| row_err(t, 0, "polProcessID", e.to_string()))?
            .cast(&DataType::Int32)
            .map_err(|e| row_err(t, 0, "polProcessID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "polProcessID", e.to_string()))?
            .clone();
        (0..df.height())
            .map(|i| {
                Ok(RunSpecPollutantProcessRow {
                    pol_process_id: pol_process_id
                        .get(i)
                        .ok_or_else(|| row_err(t, i, "polProcessID", "null value".into()))?,
                })
            })
            .collect()
    }
}

/// One `RunSpecHourDay` row — an hour/day cell the run selects. Step 300
/// inner-joins it; step 400's "All Starts" statement crosses it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunSpecHourDayRow {
    /// `hourDayID` — `hourID * 10 + dayID`.
    pub hour_day_id: i32,
}

impl TableRow for RunSpecHourDayRow {
    fn table_name() -> &'static str {
        "RunSpecHourDay"
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
        let t = "RunSpecHourDay";
        let hour_day_id = df
            .column("hourDayID")
            .map_err(|e| row_err(t, 0, "hourDayID", e.to_string()))?
            .cast(&DataType::Int32)
            .map_err(|e| row_err(t, 0, "hourDayID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "hourDayID", e.to_string()))?
            .clone();
        (0..df.height())
            .map(|i| {
                Ok(RunSpecHourDayRow {
                    hour_day_id: hour_day_id
                        .get(i)
                        .ok_or_else(|| row_err(t, i, "hourDayID", "null value".into()))?,
                })
            })
            .collect()
    }
}

/// One `RunSpecSourceType` row — a source type the run selects. Step 400's
/// "All Starts" statement crosses it with [`RunSpecHourDayRow`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunSpecSourceTypeRow {
    /// `sourceTypeID`.
    pub source_type_id: i32,
}

impl TableRow for RunSpecSourceTypeRow {
    fn table_name() -> &'static str {
        "RunSpecSourceType"
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
        let t = "RunSpecSourceType";
        let source_type_id = df
            .column("sourceTypeID")
            .map_err(|e| row_err(t, 0, "sourceTypeID", e.to_string()))?
            .cast(&DataType::Int32)
            .map_err(|e| row_err(t, 0, "sourceTypeID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "sourceTypeID", e.to_string()))?
            .clone();
        (0..df.height())
            .map(|i| {
                Ok(RunSpecSourceTypeRow {
                    source_type_id: source_type_id
                        .get(i)
                        .ok_or_else(|| row_err(t, i, "sourceTypeID", "null value".into()))?,
                })
            })
            .collect()
    }
}

// ---- Output row types -------------------------------------------------------

/// One `StartOpModeDistribution` row — the **step-300** execution table.
///
/// `calculateOpModeFraction` creates this table from the sample-vehicle soak
/// times: one row per `(sourceTypeID, hourDayID, opModeID)` carrying the
/// share of that cell's starts that fall in the mode. It is *not* what step
/// 400 copies out — see the module docs' partition table — but MOVES does
/// materialise it in the execution database, and the canonical captures hold
/// it, so the port names and emits it rather than folding it into step 400.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct StartOpModeDistributionRow {
    /// `sourceTypeID` — the MOVES source (vehicle) type.
    pub source_type_id: i32,
    /// `hourDayID` — `hourID * 10 + dayID` composite.
    pub hour_day_id: i32,
    /// `opModeID` — the start operating mode.
    pub op_mode_id: i32,
    /// `opModeFraction` — `COUNT(opModeID)/starts`, four-decimal `DECIMAL`
    /// (see [`op_mode_fraction`]).
    pub op_mode_fraction: f64,
}

impl TableRow for StartOpModeDistributionRow {
    fn table_name() -> &'static str {
        "StartOpModeDistribution"
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
        let t = "StartOpModeDistribution";
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
                Ok(StartOpModeDistributionRow {
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

/// One `OpModeDistribution` row produced by step 400's non-`DO_RATES_FIRST`
/// branch.
///
/// ```sql
/// insert ignore into opModeDistribution (…)
///  select distinct somd.sourceTypeID, (somd.hourID*10+somd.dayID), l.linkID,
///                  ppa.polProcessID, somd.opModeID, somd.opModeFraction, null
///    from startsOpModeDistribution somd
///    cross join pollutantprocessassoc ppa
///    cross join link l
///   where ppa.processID in (2,16)
/// ```
///
/// `CompilationFlags.DO_RATES_FIRST` is a `static final true` in the pinned
/// MOVES 5.0.1 tree, so this branch never runs there and no canonical capture
/// exercises it: `opModeDistribution` is empty in all nine traces that run
/// this generator. It is ported because the port's own downstream readers
/// take the start processes out of `OpModeDistribution`, and if they read it
/// the numbers must be the step-400 numbers. `linkID` is the `0` sentinel
/// (the canonical SQL crosses the off-network `link`, but no
/// `OpModeDistribution` reader in the port joins on `linkID`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OpModeDistributionRow {
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

/// Primary-key tuple of `OpModeDistribution`
/// (`XPKOpModeDistribution`, `database/CreateDefault.sql`) — the
/// `INSERT IGNORE` de-duplication key.
type OmdKey = (i32, i32, i32, i32, i32);

impl OpModeDistributionRow {
    fn key(&self) -> OmdKey {
        (
            self.source_type_id,
            self.hour_day_id,
            self.link_id,
            self.pol_process_id,
            self.op_mode_id,
        )
    }
}

impl TableRow for OpModeDistributionRow {
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
                Ok(OpModeDistributionRow {
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

/// One `RatesOpModeDistribution` row produced by step 400's `DO_RATES_FIRST`
/// branch — the branch the pinned MOVES tree always takes.
///
/// The seven columns the two `INSERT IGNORE` statements name. The execution
/// table has three more — `opModeFractionCV`, `avgBinSpeed`,
/// `avgSpeedFraction` — that neither statement sets; they are emitted at
/// their schema defaults (`NULL`, `NULL`, `0`) so the frame carries the full
/// `database/CreateExecutionRates.sql` shape every reader extracts, but they
/// are not modelled as fields.
///
/// Primary key (the `INSERT IGNORE` de-duplication key):
/// `(sourceTypeID, polProcessID, roadTypeID, hourDayID, opModeID,
/// avgSpeedBinID)`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RatesOpModeDistributionRow {
    /// `sourceTypeID` — the MOVES source type.
    pub source_type_id: i32,
    /// `roadTypeID` — always [`OFF_NETWORK_ROAD_TYPE_ID`].
    pub road_type_id: i32,
    /// `avgSpeedBinID` — always [`START_AVG_SPEED_BIN_ID`].
    pub avg_speed_bin_id: i32,
    /// `hourDayID` — `hourID * 10 + dayID` composite.
    pub hour_day_id: i32,
    /// `polProcessID` — a start pol-process from the cross join, or the
    /// literal [`ALL_STARTS_POL_PROCESS_ID`] on the "All Starts" row.
    pub pol_process_id: i32,
    /// `opModeID` — a soak band, or [`ALL_STARTS_OP_MODE_ID`].
    pub op_mode_id: i32,
    /// `opModeFraction` — the `startsOpModeDistribution` fraction, or `1.0`
    /// on the "All Starts" row.
    pub op_mode_fraction: f64,
}

/// Primary-key tuple of `RatesOpModeDistribution`
/// (`database/CreateExecutionRates.sql`), in primary-key order.
type RatesKey = (i32, i32, i32, i32, i32, i32);

impl RatesOpModeDistributionRow {
    fn key(&self) -> RatesKey {
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
            ("opModeFractionCV".into(), DataType::Float64),
            ("avgBinSpeed".into(), DataType::Float64),
            ("avgSpeedFraction".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        let unset = || vec![None::<f64>; n];
        DataFrame::new(
            n,
            vec![
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
                    "avgSpeedBinID".into(),
                    rows.iter()
                        .map(|r| r.avg_speed_bin_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "hourDayID".into(),
                    rows.iter().map(|r| r.hour_day_id).collect::<Vec<i32>>(),
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
                // Columns step 400 never names — schema defaults.
                Series::new("opModeFractionCV".into(), unset()).into(),
                Series::new("avgBinSpeed".into(), unset()).into(),
                Series::new("avgSpeedFraction".into(), vec![0.0f64; n]).into(),
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
        let road_type_id = get_i32("roadTypeID")?;
        let avg_speed_bin_id = get_i32("avgSpeedBinID")?;
        let hour_day_id = get_i32("hourDayID")?;
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
                Ok(RatesOpModeDistributionRow {
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    road_type_id: road_type_id.get(i).ok_or_else(|| null("roadTypeID"))?,
                    avg_speed_bin_id: avg_speed_bin_id
                        .get(i)
                        .ok_or_else(|| null("avgSpeedBinID"))?,
                    hour_day_id: hour_day_id.get(i).ok_or_else(|| null("hourDayID"))?,
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

// ---- Kernel -----------------------------------------------------------------

/// Inputs to [`build_start_op_mode_distribution`] — steps 100–300.
#[derive(Debug, Clone, Default)]
pub struct StartOpModeInputs {
    /// `SampleVehicleTrip` rows.
    pub trips: Vec<SampleVehicleTripRow>,
    /// `SampleVehicleDay` rows — maps `(vehID, dayID)` → `sourceTypeID`.
    pub vehicle_days: Vec<SampleVehicleDayRow>,
    /// `OperatingMode` rows — the soak-time band table.
    pub operating_modes: Vec<OperatingModeRow>,
    /// `RunSpecHourDay.hourDayID` — the hour/day cells the run selects. Step
    /// 300 inner-joins this table, so a start outside the selected hour/days
    /// contributes to no fraction at all (not even to its denominator).
    pub run_spec_hour_day_ids: Vec<i32>,
}

/// Inputs to step 400 — [`populate_rates_op_mode_distribution`] and
/// [`populate_op_mode_distribution`].
///
/// Note what is *not* here: the step-300 soak fractions. Step 400 does not
/// read them.
#[derive(Debug, Clone, Default)]
pub struct PopulateOpModeDistributionInputs {
    /// `startsOpModeDistribution` — the default-database start op-mode
    /// distribution, the table step 400 actually copies out.
    pub starts_op_mode_distribution: Vec<StartsOpModeDistributionRow>,
    /// The `polProcessID`s the cross join contributes: the run's pol-processes
    /// whose `processID` is start exhaust (2) or crankcase start (16).
    ///
    /// The canonical SQL crosses the *execution* database's
    /// `pollutantProcessAssoc`, which `ExecutionRunSpec` has already narrowed
    /// to the run. The port's `PollutantProcessAssoc` is the full default
    /// table, so its faithful equivalent is `RunSpecPollutantProcess` filtered
    /// the same way — a set the canonical captures confirm is identical in all
    /// nine traces that run this generator.
    pub start_pol_process_ids: Vec<i32>,
    /// `runSpecSourceType.sourceTypeID` — one half of the "All Starts" cross
    /// join.
    pub run_spec_source_type_ids: Vec<i32>,
    /// `runSpecHourDay.hourDayID` — the other half.
    pub run_spec_hour_day_ids: Vec<i32>,
}

/// Steps 100–300: build the `StartOpModeDistribution` soak-fraction table.
///
/// 1. **Soak time (step 100):** self-join `SampleVehicleTrip` on `priorTripID`
///    (INNER JOIN — only trips with a prior trip get a soak-time row).
/// 2. **Start op mode (step 200):** join each soak time against `OperatingMode`
///    soak-time bands, keeping all matching modes.
/// 3. **Op-mode fraction (step 300):** aggregate counts by
///    `(sourceTypeID, hourDayID)` over the `RunSpecHourDay`-selected cells and
///    divide by that cell's total starts.
#[must_use]
pub fn build_start_op_mode_distribution(
    inputs: &StartOpModeInputs,
) -> Vec<StartOpModeDistributionRow> {
    // Index SampleVehicleDay: (vehID, dayID) -> sourceTypeID.
    let veh_day_to_source_type: BTreeMap<(i32, i32), i32> = inputs
        .vehicle_days
        .iter()
        .map(|vd| ((vd.veh_id, vd.day_id), vd.source_type_id))
        .collect();

    // Index SampleVehicleTrip: (vehID, dayID, tripID) -> keyOffTime,
    // for the prior-trip self-join.
    let trip_key_off: BTreeMap<(i32, i32, i32), i32> = inputs
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

    let selected_hour_days: BTreeSet<i32> = inputs.run_spec_hour_day_ids.iter().copied().collect();

    // `counts[(source_type, hour_day, op_mode_id)]` = starts in the mode.
    // `totals[(source_type, hour_day)]` = StartsPerVehicleDay.starts.
    let mut counts: BTreeMap<(i32, i32, i32), u64> = BTreeMap::new();
    let mut totals: BTreeMap<(i32, i32), u64> = BTreeMap::new();

    for trip in &inputs.trips {
        // Only trips with a prior trip (INNER JOIN on priorTripID).
        let Some(prior_trip_id) = trip.prior_trip_id else {
            continue;
        };
        let Some(&prior_key_off) = trip_key_off.get(&(trip.veh_id, trip.day_id, prior_trip_id))
        else {
            continue;
        };
        let Some(&source_type_id) = veh_day_to_source_type.get(&(trip.veh_id, trip.day_id)) else {
            continue;
        };

        // INNER JOIN RunSpecHourDay: an unselected hour/day drops out entirely.
        let hd_id = hour_day_id(trip.hour_id as u16, trip.day_id as u16) as i32;
        if !selected_hour_days.contains(&hd_id) {
            continue;
        }

        let soak = soak_time(trip.key_on_time, prior_key_off);
        for mode_id in classify_start_op_mode(soak, &op_modes) {
            // One joined row: it counts once towards its mode and once
            // towards `StartsPerVehicleDay.starts` (both aggregates range
            // over the same join).
            *totals.entry((source_type_id, hd_id)).or_insert(0) += 1;
            *counts
                .entry((source_type_id, hd_id, mode_id as i32))
                .or_insert(0) += 1;
        }
    }

    counts
        .into_iter()
        .map(|((source_type_id, hour_day_id_val, op_mode_id), count)| {
            let total = totals
                .get(&(source_type_id, hour_day_id_val))
                .copied()
                .unwrap_or(0);
            StartOpModeDistributionRow {
                source_type_id,
                hour_day_id: hour_day_id_val,
                op_mode_id,
                op_mode_fraction: op_mode_fraction(count, total),
            }
        })
        .collect()
}

/// `SELECT DISTINCT somd.sourceTypeID, (somd.hourID*10+somd.dayID),
/// somd.opModeID, somd.opModeFraction FROM startsOpModeDistribution somd`.
///
/// The `DISTINCT` is what collapses the table's `ageID` dimension: the
/// default distribution is age-invariant, so 21 age rows become one. It is a
/// `DISTINCT`, not a `GROUP BY`, so an age-varying user input would keep one
/// row per distinct fraction — which is what MOVES does, faithfully
/// reproduced here by keying on the fraction's bit pattern.
fn distinct_start_cells(rows: &[StartsOpModeDistributionRow]) -> Vec<(i32, i32, i32, f64)> {
    rows.iter()
        .map(|r| {
            (
                r.source_type_id,
                hour_day_id(r.hour_id as u16, r.day_id as u16) as i32,
                r.op_mode_id,
                r.op_mode_fraction.to_bits(),
            )
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|(st, hd, om, bits)| (st, hd, om, f64::from_bits(bits)))
        .collect()
}

/// Step 400, `DO_RATES_FIRST` non-project branch: build the
/// `RatesOpModeDistribution` rows.
///
/// Two `INSERT IGNORE` statements, in canonical order:
///
/// 1. `startsOpModeDistribution` (age-collapsed by `DISTINCT`) crossed with
///    the run's start `polProcessID`s.
/// 2. The "All Starts" row — op mode [`ALL_STARTS_OP_MODE_ID`], fraction 1,
///    literal `polProcessID` [`ALL_STARTS_POL_PROCESS_ID`] — for every
///    `runSpecSourceType` × `runSpecHourDay` cell.
///
/// Returned in primary-key order with `INSERT IGNORE` de-duplication applied.
#[must_use]
pub fn populate_rates_op_mode_distribution(
    inputs: &PopulateOpModeDistributionInputs,
) -> Vec<RatesOpModeDistributionRow> {
    let cells = distinct_start_cells(&inputs.starts_op_mode_distribution);
    let mut rows: Vec<RatesOpModeDistributionRow> =
        Vec::with_capacity(cells.len() * inputs.start_pol_process_ids.len());

    for &(source_type_id, hour_day_id_val, op_mode_id, op_mode_fraction) in &cells {
        for &pol_process_id in &inputs.start_pol_process_ids {
            rows.push(RatesOpModeDistributionRow {
                source_type_id,
                road_type_id: OFF_NETWORK_ROAD_TYPE_ID,
                avg_speed_bin_id: START_AVG_SPEED_BIN_ID,
                hour_day_id: hour_day_id_val,
                pol_process_id,
                op_mode_id,
                op_mode_fraction,
            });
        }
    }

    for &source_type_id in &inputs.run_spec_source_type_ids {
        for &hour_day_id_val in &inputs.run_spec_hour_day_ids {
            rows.push(RatesOpModeDistributionRow {
                source_type_id,
                road_type_id: OFF_NETWORK_ROAD_TYPE_ID,
                avg_speed_bin_id: START_AVG_SPEED_BIN_ID,
                hour_day_id: hour_day_id_val,
                pol_process_id: ALL_STARTS_POL_PROCESS_ID,
                op_mode_id: ALL_STARTS_OP_MODE_ID as i32,
                op_mode_fraction: 1.0,
            });
        }
    }

    let mut seen: BTreeSet<RatesKey> = BTreeSet::new();
    let mut out: Vec<RatesOpModeDistributionRow> =
        rows.into_iter().filter(|r| seen.insert(r.key())).collect();
    out.sort_unstable_by_key(RatesOpModeDistributionRow::key);
    out
}

/// Step 400, non-`DO_RATES_FIRST` branch: build the `OpModeDistribution`
/// rows.
///
/// Same source table and same cross join as
/// [`populate_rates_op_mode_distribution`], without the "All Starts" row
/// (that statement is inside the `DO_RATES_FIRST` arm) and with `linkID`
/// rather than road type / speed bin. Dead in the pinned MOVES tree — see
/// [`OpModeDistributionRow`].
#[must_use]
pub fn populate_op_mode_distribution(
    inputs: &PopulateOpModeDistributionInputs,
) -> Vec<OpModeDistributionRow> {
    let cells = distinct_start_cells(&inputs.starts_op_mode_distribution);
    let mut rows: Vec<OpModeDistributionRow> =
        Vec::with_capacity(cells.len() * inputs.start_pol_process_ids.len());
    for &(source_type_id, hour_day_id_val, op_mode_id, op_mode_fraction) in &cells {
        for &pol_process_id in &inputs.start_pol_process_ids {
            rows.push(OpModeDistributionRow {
                source_type_id,
                hour_day_id: hour_day_id_val,
                link_id: 0,
                pol_process_id,
                op_mode_id,
                op_mode_fraction,
            });
        }
    }
    let mut seen: BTreeSet<OmdKey> = BTreeSet::new();
    let mut out: Vec<OpModeDistributionRow> =
        rows.into_iter().filter(|r| seen.insert(r.key())).collect();
    out.sort_unstable_by_key(OpModeDistributionRow::key);
    out
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
        // Skip when this generator has already contributed its rows to the shared
        // OpModeDistribution table, or when it is provided by a snapshot.
        if crate::wiring::op_mode_distribution_already_built(ctx, START_OMDG_DONE_MARKER) {
            return Ok(CalculatorOutput::empty());
        }

        let tables = ctx.tables();
        let run_spec_hour_day_ids: Vec<i32> = tables
            .iter_typed_or_empty::<RunSpecHourDayRow>("RunSpecHourDay")?
            .into_iter()
            .map(|r| r.hour_day_id)
            .collect();
        let run_spec_source_type_ids: Vec<i32> = tables
            .iter_typed_or_empty::<RunSpecSourceTypeRow>("RunSpecSourceType")?
            .into_iter()
            .map(|r| r.source_type_id)
            .collect();

        // The cross join's pol-processes: the run's, narrowed to start exhaust
        // (2) and crankcase start (16). `processID = polProcessID % 100`.
        let start_exhaust = START_EXHAUST_PROCESS_ID.0 as i32;
        let crankcase_start = CRANKCASE_START_EXHAUST_PROCESS_ID.0 as i32;
        let start_pol_process_ids: Vec<i32> = tables
            .iter_typed_or_empty::<RunSpecPollutantProcessRow>("RunSpecPollutantProcess")?
            .into_iter()
            .map(|r| r.pol_process_id)
            .filter(|&pp| {
                let process = pp.rem_euclid(100);
                process == start_exhaust || process == crankcase_start
            })
            .collect();

        // Steps 100–300: soak time → start op mode → op-mode fraction.
        let soak_inputs = StartOpModeInputs {
            trips: tables.iter_typed_or_empty("SampleVehicleTrip")?,
            vehicle_days: tables.iter_typed_or_empty("SampleVehicleDay")?,
            operating_modes: tables.iter_typed_or_empty("OperatingMode")?,
            run_spec_hour_day_ids: run_spec_hour_day_ids.clone(),
        };
        let start_omd_rows = build_start_op_mode_distribution(&soak_inputs);

        // Step 400: copy `startsOpModeDistribution` out. NOT the step-300
        // fractions above — see the module docs' partition table.
        let populate_inputs = PopulateOpModeDistributionInputs {
            starts_op_mode_distribution: tables.iter_typed_or_empty("startsOpModeDistribution")?,
            start_pol_process_ids,
            run_spec_source_type_ids,
            run_spec_hour_day_ids,
        };
        let rates_rows = populate_rates_op_mode_distribution(&populate_inputs);
        let omd_rows = populate_op_mode_distribution(&populate_inputs);

        // The step-300 table, named as its own partition so a reader cannot
        // mistake it for the step-400 output.
        let start_omd_df = StartOpModeDistributionRow::into_dataframe(start_omd_rows)
            .map_err(|e| Error::Polars(e.to_string()))?;
        ctx.scratch_mut().insert(OUTPUT_TABLES[0], start_omd_df);

        // Merge the start rows into the shared OpModeDistribution table (the
        // running/brake and evap generators contribute the other processes).
        crate::wiring::merge_op_mode_distribution(ctx, START_OMDG_DONE_MARKER, omd_rows)?;

        // `CompilationFlags.DO_RATES_FIRST` is `static final true` in the
        // pinned tree, so canonical MOVES takes the rates branch in *every*
        // run — the nine canonical traces that run this generator are all
        // `Inv`/`MACROSCALE` and all carry these rows in
        // `RatesOpModeDistribution` with `OpModeDistribution` empty. The
        // scale therefore does not gate the write.
        crate::wiring::write_scratch_table(ctx, OUTPUT_TABLES[2], rates_rows)?;

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
        assert!(inputs.contains(&"startsOpModeDistribution"));
        assert_eq!(
            gen.output_tables(),
            &[
                "StartOpModeDistribution",
                "OpModeDistribution",
                "RatesOpModeDistribution"
            ]
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

    /// Seed the tables both steps read.
    ///
    /// Steps 100–300: vehicle 1 / day 5 makes two trips; trip 2's soak is
    /// `540 − 480 = 60`, which lands in `[60,∞)` → op mode 102. Trip 1 has no
    /// prior trip, so it is not a start. `hourDayID = 9*10 + 5 = 95`.
    ///
    /// Step 400: a two-age `startsOpModeDistribution` whose age rows collapse
    /// under `DISTINCT` to one cell per op mode, deliberately carrying
    /// *different* numbers from the soak fractions so a test can tell the two
    /// tables apart.
    fn seeded_store() -> moves_framework::InMemoryStore {
        use moves_framework::{DataFrameStore, InMemoryStore};
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
        store.insert(
            "RunSpecHourDay",
            RunSpecHourDayRow::into_dataframe(vec![RunSpecHourDayRow { hour_day_id: 95 }]).unwrap(),
        );
        store.insert(
            "RunSpecSourceType",
            RunSpecSourceTypeRow::into_dataframe(vec![RunSpecSourceTypeRow { source_type_id: 21 }])
                .unwrap(),
        );
        // Step 400 fans the copy across the run's start polProcessIDs
        // (processID = polProcessID % 100 in {2,16}); the running-exhaust
        // row 201 is filtered out.
        store.insert(
            "RunSpecPollutantProcess",
            RunSpecPollutantProcessRow::into_dataframe(vec![
                RunSpecPollutantProcessRow {
                    pol_process_id: 202,
                },
                RunSpecPollutantProcessRow {
                    pol_process_id: 302,
                },
                RunSpecPollutantProcessRow {
                    pol_process_id: 216,
                },
                RunSpecPollutantProcessRow {
                    pol_process_id: 201,
                },
            ])
            .unwrap(),
        );
        store.insert(
            "startsOpModeDistribution",
            StartsOpModeDistributionRow::into_dataframe(
                [0, 1]
                    .into_iter()
                    .flat_map(|age_id| {
                        [(101, 0.25_f64), (102, 0.75_f64)].into_iter().map(
                            move |(op_mode_id, op_mode_fraction)| StartsOpModeDistributionRow {
                                day_id: 5,
                                hour_id: 9,
                                source_type_id: 21,
                                age_id,
                                op_mode_id,
                                op_mode_fraction,
                            },
                        )
                    })
                    .collect(),
            )
            .unwrap(),
        );
        store
    }

    #[test]
    fn execute_writes_the_step_300_and_step_400_tables_to_scratch() {
        let mut ctx = CalculatorContext::with_tables(seeded_store());
        let out = StartOperatingModeDistributionGenerator
            .execute(&mut ctx)
            .expect("execute ok");
        // Generator writes to scratch — main output is empty.
        assert!(out.dataframe().is_none());

        // Step 300: the soak fractions, in their own table. One start, in
        // op mode 102 → fraction 1.
        let soak: Vec<StartOpModeDistributionRow> = ctx
            .scratch()
            .store
            .iter_typed("StartOpModeDistribution")
            .expect("StartOpModeDistribution in scratch");
        assert_eq!(
            soak,
            vec![StartOpModeDistributionRow {
                source_type_id: 21,
                hour_day_id: 95,
                op_mode_id: 102,
                op_mode_fraction: 1.0,
            }]
        );

        // Step 400 copies `startsOpModeDistribution`, NOT the soak fractions:
        // both of its op modes appear, with its 0.25 / 0.75 — the soak table
        // has only op mode 102 at 1.0.
        let rates: Vec<RatesOpModeDistributionRow> = ctx
            .scratch()
            .store
            .iter_typed("RatesOpModeDistribution")
            .expect("RatesOpModeDistribution in scratch");
        // 2 op modes × 3 start pol-processes, + the All Starts row.
        assert_eq!(rates.len(), 7);
        for r in &rates {
            assert_eq!(r.source_type_id, 21);
            assert_eq!(r.road_type_id, OFF_NETWORK_ROAD_TYPE_ID);
            assert_eq!(r.avg_speed_bin_id, START_AVG_SPEED_BIN_ID);
            assert_eq!(r.hour_day_id, 95);
        }
        let copied: BTreeSet<(i32, i32, u64)> = rates
            .iter()
            .filter(|r| r.op_mode_id != ALL_STARTS_OP_MODE_ID as i32)
            .map(|r| (r.pol_process_id, r.op_mode_id, r.op_mode_fraction.to_bits()))
            .collect();
        let expected: BTreeSet<(i32, i32, u64)> = [202, 216, 302]
            .into_iter()
            .flat_map(|pp| {
                [(101, 0.25_f64), (102, 0.75_f64)]
                    .into_iter()
                    .map(move |(om, f)| (pp, om, f.to_bits()))
            })
            .collect();
        assert_eq!(copied, expected);

        // The All Starts row carries the literal polProcessID 602 even though
        // 602 is not one of the run's pol-processes.
        let all_starts: Vec<&RatesOpModeDistributionRow> = rates
            .iter()
            .filter(|r| r.op_mode_id == ALL_STARTS_OP_MODE_ID as i32)
            .collect();
        assert_eq!(all_starts.len(), 1);
        assert_eq!(all_starts[0].pol_process_id, ALL_STARTS_POL_PROCESS_ID);
        assert_eq!(all_starts[0].op_mode_fraction, 1.0);

        // The dead inventory branch reads the same source table.
        let omd: Vec<OpModeDistributionRow> = ctx
            .scratch()
            .store
            .iter_typed("OpModeDistribution")
            .expect("OpModeDistribution in scratch");
        assert_eq!(omd.len(), 6, "2 op modes × 3 start polProcessIDs");
        for r in &omd {
            assert_eq!(r.link_id, 0);
            assert!(r.op_mode_fraction == 0.25 || r.op_mode_fraction == 0.75);
        }
    }

    #[test]
    fn step_400_ignores_the_step_300_soak_fractions_entirely() {
        // The defect this test pins: with no `startsOpModeDistribution` the
        // step-400 tables are empty *even though* the soak fractions exist.
        use moves_framework::DataFrameStore;

        let mut store = seeded_store();
        store.insert(
            "startsOpModeDistribution",
            StartsOpModeDistributionRow::into_dataframe(vec![]).unwrap(),
        );
        let mut ctx = CalculatorContext::with_tables(store);
        StartOperatingModeDistributionGenerator
            .execute(&mut ctx)
            .expect("execute ok");

        let soak: Vec<StartOpModeDistributionRow> = ctx
            .scratch()
            .store
            .iter_typed("StartOpModeDistribution")
            .expect("StartOpModeDistribution in scratch");
        assert_eq!(soak.len(), 1, "the soak fractions are still computed");

        let rates: Vec<RatesOpModeDistributionRow> = ctx
            .scratch()
            .store
            .iter_typed("RatesOpModeDistribution")
            .expect("RatesOpModeDistribution in scratch");
        // Only the All Starts row, which comes from runSpec* — not from the
        // soak fractions.
        assert_eq!(rates.len(), 1);
        assert_eq!(rates[0].op_mode_id, ALL_STARTS_OP_MODE_ID as i32);

        let omd: Vec<OpModeDistributionRow> = ctx
            .scratch()
            .store
            .iter_typed("OpModeDistribution")
            .expect("OpModeDistribution in scratch");
        assert!(omd.is_empty());
    }

    #[test]
    fn step_300_drops_first_trips_and_unselected_hour_days() {
        // Trip 1 has no prior trip → no soak time → no start row; and a start
        // in an hour/day the run did not select is dropped by the
        // RunSpecHourDay inner join.
        let one_trip = vec![SampleVehicleTripRow {
            veh_id: 1,
            day_id: 5,
            trip_id: 1,
            hour_id: 9,
            prior_trip_id: None,
            key_on_time: 400,
            key_off_time: 480,
        }];
        let modes = vec![OperatingModeRow {
            op_mode_id: 102,
            min_soak_time: Some(60),
            max_soak_time: None,
        }];
        let days = vec![SampleVehicleDayRow {
            veh_id: 1,
            day_id: 5,
            source_type_id: 21,
        }];
        assert!(build_start_op_mode_distribution(&StartOpModeInputs {
            trips: one_trip,
            vehicle_days: days.clone(),
            operating_modes: modes.clone(),
            run_spec_hour_day_ids: vec![95],
        })
        .is_empty());

        // Same two trips as `seeded_store`, but hour-day 95 is not selected.
        let trips = vec![
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
        ];
        assert_eq!(
            build_start_op_mode_distribution(&StartOpModeInputs {
                trips: trips.clone(),
                vehicle_days: days.clone(),
                operating_modes: modes.clone(),
                run_spec_hour_day_ids: vec![95],
            })
            .len(),
            1
        );
        assert!(build_start_op_mode_distribution(&StartOpModeInputs {
            trips,
            vehicle_days: days,
            operating_modes: modes,
            run_spec_hour_day_ids: vec![125],
        })
        .is_empty());
    }

    #[test]
    fn step_300_fractions_are_the_four_decimal_decimal_quotient() {
        // Three starts in one cell: two in mode 101, one in mode 102.
        // 1/3 is stored as 0.3333, not 0.33333333….
        let trips: Vec<SampleVehicleTripRow> = [(2, 1, 30), (4, 3, 30), (6, 5, 600)]
            .into_iter()
            .flat_map(|(trip_id, prior_id, soak)| {
                [
                    SampleVehicleTripRow {
                        veh_id: 1,
                        day_id: 5,
                        trip_id: prior_id,
                        hour_id: 9,
                        prior_trip_id: None,
                        key_on_time: 0,
                        key_off_time: 100,
                    },
                    SampleVehicleTripRow {
                        veh_id: 1,
                        day_id: 5,
                        trip_id,
                        hour_id: 9,
                        prior_trip_id: Some(prior_id),
                        key_on_time: 100 + soak,
                        key_off_time: 100 + soak + 10,
                    },
                ]
            })
            .collect();
        let rows = build_start_op_mode_distribution(&StartOpModeInputs {
            trips,
            vehicle_days: vec![SampleVehicleDayRow {
                veh_id: 1,
                day_id: 5,
                source_type_id: 21,
            }],
            operating_modes: vec![
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
            ],
            run_spec_hour_day_ids: vec![95],
        });
        assert_eq!(
            rows,
            vec![
                StartOpModeDistributionRow {
                    source_type_id: 21,
                    hour_day_id: 95,
                    op_mode_id: 101,
                    op_mode_fraction: 0.6667,
                },
                StartOpModeDistributionRow {
                    source_type_id: 21,
                    hour_day_id: 95,
                    op_mode_id: 102,
                    op_mode_fraction: 0.3333,
                },
            ]
        );
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
            "RunSpecPollutantProcess",
            RunSpecPollutantProcessRow::into_dataframe(vec![]).unwrap(),
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
