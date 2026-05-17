//! `StartOperatingModeDistributionGenerator` — Phase 3 Task 32.
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
//!    `SampleVehicleTrip` on `priorTripID`: `soakTime = keyOnTime −
//!    keyOffTime[prior trip]`, the engine-off gap before the start. A trip
//!    with no prior trip produces no soak-time row — the self-join is an
//!    INNER JOIN.
//! 2. **Start operating mode (step 200).** `calculateStartOpMode` joins each
//!    soak time against the `OperatingMode` soak-time bands
//!    (`minSoakTime` … `maxSoakTime`) and keeps the matching modes.
//! 3. **Operating-mode fraction (step 300).** `calculateOpModeFraction`
//!    counts the starts per (source type, hour-day) and, within each, the
//!    starts in every operating mode; `opModeFraction = count(opMode) /
//!    starts`.
//! 4. **Populate (step 400).** `populateOperatingModeDistribution` copies the
//!    fractions into the `OpModeDistribution` / `RatesOpModeDistribution`
//!    execution tables for the start (process 2) and crankcase-start
//!    (process 16) processes.
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
//! The `moves-framework` calculator data plane is still a Phase 2 skeleton:
//! the [`CalculatorContext`] passed to [`execute`](Generator::execute)
//! exposes only placeholder execution tables and scratch namespace with no
//! row storage. So `execute` cannot read `SampleVehicleTrip` or write
//! `OpModeDistribution` yet — it returns an empty [`CalculatorOutput`],
//! matching every other Phase 2/3 module (Task 28's empty-output smoke
//! test). Task 50 (`DataFrameStore`) lands the storage; `execute` then walks
//! the trips through [`classify_trip`], aggregates with [`op_mode_fraction`],
//! and writes the result. The functions below are complete and tested and
//! are what `execute` will call.

use std::sync::OnceLock;

use moves_calculator_info::{Granularity, Priority};
use moves_data::{PollutantId, PollutantProcessAssociation, ProcessId};
use moves_framework::{
    CalculatorContext, CalculatorOutput, CalculatorSubscription, Error, Generator,
};

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
    ///   i.e. SQL `UNKNOWN`, and `UNKNOWN AND UNKNOWN` is not `TRUE`.
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
/// rounding is deferred to Task 44's canonical-capture comparison, which can
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

/// MOVES `StartOperatingModeDistributionGenerator` (migration plan Task 32).
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

    fn execute(&self, _ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
        // Steps 100–300 are ported as the pure functions above; step 400 is a
        // multi-table copy into `OpModeDistribution` / `RatesOpModeDistribution`.
        // Both need `SampleVehicleTrip` and the execution tables, which the
        // Phase 2 placeholder `CalculatorContext` does not store yet — so the
        // generator contributes no rows until the Task 50 data plane lands,
        // matching every other Phase 2/3 module.
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
    fn generator_execute_is_ok() {
        // Phase 2/3 skeleton: execute returns an empty output until the data
        // plane lands. Smoke-test that it is callable and Ok.
        let ctx = CalculatorContext::new();
        assert!(StartOperatingModeDistributionGenerator
            .execute(&ctx)
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
