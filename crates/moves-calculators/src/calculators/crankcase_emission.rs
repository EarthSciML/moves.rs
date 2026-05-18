//! Port of `CrankcaseEmissionCalculator.java`, its `NonPM` / `PM` subclasses,
//! and `database/CrankcaseEmissionCalculator.sql` — migration plan Phase 3,
//! Task 63.
//!
//! Crankcase emissions are the blow-by gases and particulates that escape
//! past the piston rings into the engine crankcase and are then vented. MOVES
//! does not model them from first principles: it computes them as a fixed
//! ratio of the corresponding *exhaust*-process emission of the same
//! pollutant — so the crankcase calculator runs *after* the exhaust
//! calculators and simply rescales their output.
//!
//! # The three Java classes
//!
//! Task 63 names three classes; the SQL holds the algorithm and the Java
//! splits it into an abstract base plus two thin subclasses:
//!
//! | Java class | This module | Runtime role |
//! |------------|-------------|--------------|
//! | `CrankcaseEmissionCalculator` (base) | [`CrankcaseEmissionCalculator`] | The shared algorithm. Never instantiated directly and carries no `Registration` directive — *not* a [`Calculator`]; it exposes the ported [`calculate`](CrankcaseEmissionCalculator::calculate) the two variants delegate to. |
//! | `CrankcaseEmissionCalculatorNonPM` | [`CrankcaseEmissionCalculatorNonPM`] | The runtime calculator for the 60 gaseous / non-particulate pollutants — 180 `(pollutant, process)` registrations. |
//! | `CrankcaseEmissionCalculatorPM` | [`CrankcaseEmissionCalculatorPM`] | The particulate variant — superseded by `SulfatePMCalculator`, so it is absent from the runtime registration file and registers nothing. |
//!
//! In Java the only difference between the two subclasses is the constructor:
//! each passes the base a different pollutant-ID list and a different
//! `tablePrefix` (`"NonPM"` / `"PM"`). Both are *extract*-side concerns — they
//! choose which `CrankcaseEmissionRatio` / `PollutantProcessAssoc` rows the
//! SQL's "Extract Data" section pulls. The "Processing" algorithm is identical
//! but for one branch: the `-- Section SulfatePM10` block, which the Java
//! `doExecute` enables only when pollutant 105 (Sulfate PM10) is in the
//! calculator's set — i.e. only for the PM variant.
//!
//! # Chained calculator
//!
//! `CrankcaseEmissionCalculator.subscribeToMe` does **not** subscribe to the
//! MasterLoop. It is a *chained* calculator: it finds the calculators that
//! produce the exhaust-process emissions it needs and chains itself onto
//! them, running in the same master-loop pass. `calculator-dag.json` records
//! `CrankcaseEmissionCalculatorNonPM` as `subscribes_directly: false`,
//! `subscriptions: []`, `depends_on: ["AirToxicsCalculator",
//! "BaseRateCalculator", "HCSpeciationCalculator", "NO2Calculator",
//! "NOCalculator", "SO2Calculator"]` — every calculator producing one of the
//! 60 non-PM pollutants in running, start or extended-idle exhaust. The
//! [`Calculator`] metadata mirrors it: [`subscriptions`](Calculator::subscriptions)
//! is empty and [`upstream`](Calculator::upstream) names those six.
//!
//! # What it computes
//!
//! For each emission record an exhaust process produced, the crankcase
//! emission of the same pollutant is a fixed multiple of it:
//!
//! ```text
//! crankcaseEmission = exhaustEmission × crankcaseRatio
//! ```
//!
//! `crankcaseRatio` is looked up from the `CrankcaseEmissionRatio` default-DB
//! table per `(pollutant-process, sourceType, regClass, fuelType, modelYear)`.
//! The output row carries the **crankcase** process in place of the source
//! exhaust process —
//!
//! | source exhaust process | crankcase process |
//! |------------------------|-------------------|
//! | Running Exhaust (1) | Crankcase Running Exhaust (15) |
//! | Start Exhaust (2) | Crankcase Start Exhaust (16) |
//! | Extended Idle Exhaust (90) | Crankcase Extended Idle Exhaust (17) |
//!
//! — and the pollutant, the dimension cell and the model year are unchanged.
//! Both `emissionQuant` and `emissionRate` are scaled by the same ratio.
//!
//! # Algorithm — the SQL "Processing" section
//!
//! [`CrankcaseEmissionCalculator::calculate`] ports the SQL's "Processing"
//! section. The SQL builds two prefixed working tables and one temp table;
//! the port folds them into two index maps and one join loop:
//!
//! | SQL working table / step | This port |
//! |--------------------------|-----------|
//! | `##prefix##CrankcasePollutantProcessAssoc` | [`CrankcasePollutantProcessAssocRow`] rows, indexed `(pollutantID, crankcase processID) → polProcessID` |
//! | `##prefix##CrankcaseEmissionRatio` | [`CrankcaseEmissionRatioRow`] rows, indexed `(polProcessID, sourceTypeID, regClassID, fuelTypeID) → [ratio rows]` |
//! | main `INSERT … SELECT` | the per-[`MovesWorkerOutputRow`] join loop |
//! | `-- Section SulfatePM10` | the optional pollutant-115 → 105 relabel |
//! | `MOVESWorkerOutput` (post-`INSERT`) | the returned `Vec<`[`MovesWorkerOutputRow`]`>` |
//!
//! Every SQL join is an `INNER JOIN`, so a row with no match on the join key
//! is dropped; the port reproduces that with map lookups that skip on a miss.
//! `CrankcaseEmissionRatio`'s primary key includes the model-year range, so a
//! single `(polProcess, sourceType, regClass, fuelType)` cell may carry
//! several rows with different `[minModelYearID, maxModelYearID]` windows; the
//! port keeps the per-cell rows in a `Vec` and applies the
//! `minModelYearID ≤ modelYearID ≤ maxModelYearID` range test per row, so an
//! exhaust row matching two windows yields two output rows exactly as the
//! `INNER JOIN` would.
//!
//! # The SulfatePM10 section
//!
//! Pollutant 105 (Sulfate PM10) carries no crankcase ratio of its own —
//! `CalculatorInfo.txt` registers no calculator for it on any process.
//! Crankcase sulfate is size-cut-independent, so MOVES derives the PM10
//! sulfate crankcase emission by relabelling the PM2.5 sulfate
//! (`Sulfate Particulate`, pollutant 115) crankcase rows: the SQL's
//! `-- Section SulfatePM10` copies every `MOVESWorkerOutput` row with
//! `pollutantID = 115` and a crankcase process (15, 16, 17) back into
//! `MOVESWorkerOutput` with `pollutantID` overwritten to 105 and the emission
//! values untouched.
//!
//! By the time that section runs, `MOVESWorkerOutput` is the original input
//! plus the main step's freshly inserted crankcase rows. Crankcase processes
//! 15/16/17 are produced *only* by this calculator, so in practice the only
//! rows the section matches are the main step's own pollutant-115 output;
//! [`calculate`](CrankcaseEmissionCalculator::calculate) still scans both the
//! input rows and the main-step output, mirroring the literal
//! `from MOVESWorkerOutput`.
//!
//! The Java `doExecute` enables the section only when a pollutant with
//! `databaseKey == 105` is in the calculator's set. Only
//! `CrankcaseEmissionCalculatorPM` carries 105 (its constructor's IDs are
//! `{105, 118, 112, 115}`), so the section runs for the PM variant and not
//! the NonPM one — modelled by the `produce_sulfate_pm10` argument to
//! [`CrankcaseEmissionCalculator::calculate`].
//!
//! # Registrations
//!
//! **NonPM.** The Java `CrankcaseEmissionCalculatorNonPM` constructor passes
//! 63 pollutant IDs to the base. Three of them — 47, 179 and 186 — are not
//! legal MOVES pollutants: `Pollutant.findByID` returns `null` and the base
//! constructor's registration loop skips them. `CalculatorInfo.txt` therefore
//! records 60 distinct pollutants × 3 crankcase processes = 180 `Registration`
//! directives (`registrations_count: 180` in `calculator-dag.json`).
//! [`CrankcaseEmissionCalculatorNonPM::registrations`] is built from those 60,
//! not from the raw Java array.
//!
//! **PM — superseded.** `CrankcaseEmissionCalculatorPM` is **not wired into
//! the pinned MOVES runtime**. `CalculatorInfo.txt` carries no `Registration`
//! directive for it: the crankcase particulate pollutants 112 / 115 / 118 are
//! registered to `SulfatePMCalculator` instead, and 105 is registered to no
//! calculator at all. `calculator-dag.json` has no `CrankcaseEmissionCalculatorPM`
//! entry. The base-rate / `SulfatePMCalculator` approach superseded this older
//! scripted-SQL particulate calculator. The migration plan still lists the
//! class as a Task 63 deliverable, so this module ports its algorithm
//! faithfully for reference and cross-validation; to stay consistent with the
//! runtime, [`CrankcaseEmissionCalculatorPM::registrations`] returns an empty
//! slice — the registry must not double-register against `SulfatePMCalculator`.
//!
//! # Fidelity notes
//!
//! `CrankcaseEmissionRatio.crankcaseRatio` is a `FLOAT` (32-bit) column, but
//! it is a model **input** — already `f32`-quantised before
//! [`calculate`](CrankcaseEmissionCalculator::calculate) sees it (the port
//! widens it to `f64`). Both the temp-table `CrankcaseMOVESWorkerOutputTemp`
//! and `MOVESWorkerOutput` store `emissionQuant` / `emissionRate` as `DOUBLE`,
//! and MariaDB evaluates `emissionQuant * crankcaseRatio` in `DOUBLE`, so —
//! unlike the `SO2Calculator` / `CH4N2ORunningStartCalculator` ports — there
//! is **no `FLOAT` intermediate column** to truncate: the port's `f64`
//! multiplication reproduces the MariaDB result exactly. The exhaust
//! `emissionQuant` / `emissionRate` inputs are themselves `DOUBLE`.
//!
//! The model-year window test is integer comparison (`SMALLINT`) — exact, and
//! with no integer division it does not meet the MariaDB
//! `div_precision_increment` rounding gotcha. The SQL's "Extract Data" section
//! uses the `MYMAP` / `MYRMAP` model-year-mapping macros and a
//! `[context.year − 40, context.year]` window to *narrow* the
//! `CrankcaseEmissionRatio` extract; that is data-plane plumbing left to the
//! Task 50 wiring. The [`CrankcaseEmissionRatioRow`] this port consumes
//! already carries real model years (post-`MYRMAP`).
//!
//! # Scope of this port — data plane (Task 50)
//!
//! [`calculate`](CrankcaseEmissionCalculator::calculate) is the SQL
//! "Processing" section. Its [`CrankcaseInputs`] argument is the set of tables
//! the SQL's "Extract Data" section produces, as plain row vectors; a future
//! Task 50 (`DataFrameStore`) wiring populates it from the per-run filtered
//! execution database. `MOVESRunID`, `iterationID` and `SCC` are pure
//! pass-through columns the SQL copies verbatim from the exhaust row into the
//! crankcase row; following the `SO2Calculator` / `DistanceCalculator`
//! precedent they are not modelled here — the Task 50 output wiring carries
//! them.
//!
//! [`Calculator::execute`] receives a [`CalculatorContext`] whose
//! `ExecutionTables` / `ScratchNamespace` are Phase 2 placeholders until the
//! `DataFrameStore` lands, so `execute` cannot yet read the input tables nor
//! emit `MOVESWorkerOutput`. The numeric algorithm is fully ported and
//! unit-tested on [`calculate`](CrankcaseEmissionCalculator::calculate);
//! `execute` is a documented shell returning an empty [`CalculatorOutput`].
//! Once the data plane exists, `execute` materialises a [`CrankcaseInputs`]
//! from `ctx.tables()`, calls [`calculate`](CrankcaseEmissionCalculator::calculate),
//! and writes the rows back to `MOVESWorkerOutput`.

use std::collections::HashMap;

use moves_data::{PollutantId, PollutantProcessAssociation, ProcessId};
use moves_framework::{
    Calculator, CalculatorContext, CalculatorOutput, CalculatorSubscription, Error,
};

/// Stable module name of the NonPM variant — matches the Java class and the
/// `CrankcaseEmissionCalculatorNonPM` entry in `calculator-dag.json`.
const NONPM_NAME: &str = "CrankcaseEmissionCalculatorNonPM";

/// Stable module name of the PM variant — matches the Java class. The PM
/// variant is superseded and has no `calculator-dag.json` entry; the name is
/// still its stable identifier.
const PM_NAME: &str = "CrankcaseEmissionCalculatorPM";

/// Sulfate PM10 — `Pollutant` id 105. The `-- Section SulfatePM10` block
/// stamps this id onto the relabelled rows. MOVES has the base class's
/// `sPM10ID` field hold the same value.
const SULFATE_PM10_POLLUTANT: i32 = 105;

/// Sulfate Particulate (PM2.5 sulfate) — `Pollutant` id 115. The
/// `-- Section SulfatePM10` block reads the crankcase rows for this pollutant
/// and copies them as [`SULFATE_PM10_POLLUTANT`].
const SULFATE_PARTICULATE_POLLUTANT: i32 = 115;

// ===========================================================================
// Input tables — plain Rust mirrors of the tables `CrankcaseEmissionCalculator.sql`'s
// "Extract Data" section pulls. Following the Phase 3 convention every
// `INT`/`SMALLINT` identifier is an `i32` and every `FLOAT`/`DOUBLE` quantity
// is an `f64`. Only the columns the "Processing" algorithm reads are modelled.
// ===========================================================================

/// One `CrankcaseEmissionRatio` row — the crankcase-to-exhaust ratio for a
/// `(pollutant-process, model-year range, sourceType, regClass, fuelType)`
/// cell.
///
/// The default-DB table also carries `crankcaseRatioCV` (a coefficient of
/// variation, used only by the uncertainty machinery); the "Processing"
/// section never reads it, so it is not modelled. `minModelYearID` /
/// `maxModelYearID` are real model years — the SQL's `MYRMAP` macro un-maps
/// them during extraction.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CrankcaseEmissionRatioRow {
    /// `polProcessID` — `pollutantID × 100 + processID` for the *crankcase*
    /// process; joins to [`CrankcasePollutantProcessAssocRow::pol_process_id`].
    pub pol_process_id: i32,
    /// `minModelYearID` — inclusive lower bound of the model-year window.
    pub min_model_year_id: i32,
    /// `maxModelYearID` — inclusive upper bound of the model-year window.
    pub max_model_year_id: i32,
    /// `sourceTypeID` — the source (vehicle) type the ratio applies to.
    pub source_type_id: i32,
    /// `regClassID` — the regulatory class the ratio applies to.
    pub reg_class_id: i32,
    /// `fuelTypeID` — the fuel type the ratio applies to.
    pub fuel_type_id: i32,
    /// `crankcaseRatio` — the multiplier. `FLOAT` in MOVES; a model input.
    pub crankcase_ratio: f64,
}

/// One `CrankcasePollutantProcessAssoc` row — a `(pollutant, crankcase
/// process)` pairing and its composite `polProcessID`.
///
/// The default-DB extract also carries `isAffectedByExhaustIM` /
/// `isAffectedByEvapIM`; the "Processing" section never reads them, so they
/// are not modelled. `process_id` is always a crankcase process (15, 16, 17).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CrankcasePollutantProcessAssocRow {
    /// `polProcessID` — `pollutantID × 100 + processID`.
    pub pol_process_id: i32,
    /// `processID` — the crankcase emission process (15, 16 or 17).
    pub process_id: i32,
    /// `pollutantID` — the pollutant.
    pub pollutant_id: i32,
}

/// One `MOVESWorkerOutput` row — the subset of columns the crankcase
/// algorithm reads and writes.
///
/// The same shape serves both the calculator's **input** (the exhaust-process
/// emission records the upstream calculators produced) and its **output** (the
/// crankcase rows the SQL inserts back into `MOVESWorkerOutput`): the SQL
/// reads and writes one table, and a crankcase row is an exhaust row with the
/// process remapped and the emissions rescaled. `MOVESRunID`, `iterationID`
/// and `SCC` are pure pass-through columns and are not modelled (see the
/// [module documentation](self)).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MovesWorkerOutputRow {
    /// `yearID`.
    pub year_id: i32,
    /// `monthID`.
    pub month_id: i32,
    /// `dayID`.
    pub day_id: i32,
    /// `hourID`.
    pub hour_id: i32,
    /// `stateID`.
    pub state_id: i32,
    /// `countyID`.
    pub county_id: i32,
    /// `zoneID`.
    pub zone_id: i32,
    /// `linkID`.
    pub link_id: i32,
    /// `pollutantID` — unchanged from the source exhaust row by the main
    /// step; overwritten to 105 (Sulfate PM10) by the SulfatePM10 relabel.
    pub pollutant_id: i32,
    /// `processID` — a source exhaust process (1, 2, 90) on an input row; a
    /// crankcase process (15, 16, 17) on an output row.
    pub process_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `regClassID`.
    pub reg_class_id: i32,
    /// `fuelTypeID`.
    pub fuel_type_id: i32,
    /// `modelYearID`.
    pub model_year_id: i32,
    /// `roadTypeID`.
    pub road_type_id: i32,
    /// `emissionQuant` — the emission quantity. `DOUBLE` in MOVES.
    pub emission_quant: f64,
    /// `emissionRate` — the emission rate. `DOUBLE` in MOVES.
    pub emission_rate: f64,
}

impl MovesWorkerOutputRow {
    /// The integer dimension tuple — every column except the two emission
    /// values. Used to sort the output deterministically: MOVES leaves
    /// `MOVESWorkerOutput` physically unordered (the SQL `INSERT … SELECT`
    /// has no `ORDER BY`), so the port sorts purely to make the result
    /// reproducible.
    fn dimension_key(&self) -> [i32; 15] {
        [
            self.year_id,
            self.month_id,
            self.day_id,
            self.hour_id,
            self.state_id,
            self.county_id,
            self.zone_id,
            self.link_id,
            self.pollutant_id,
            self.process_id,
            self.source_type_id,
            self.reg_class_id,
            self.fuel_type_id,
            self.model_year_id,
            self.road_type_id,
        ]
    }
}

/// Inputs to [`CrankcaseEmissionCalculator::calculate`] — the tables the SQL's
/// "Extract Data" section produces, as plain row vectors.
///
/// A future Task 50 (`DataFrameStore`) wiring populates this from the per-run
/// filtered execution database; until then it is the explicit data-plane
/// contract the unit tests build directly.
#[derive(Debug, Clone, Default)]
pub struct CrankcaseInputs {
    /// `CrankcaseEmissionRatio` rows — the crankcase-to-exhaust ratios.
    pub crankcase_emission_ratio: Vec<CrankcaseEmissionRatioRow>,
    /// `CrankcasePollutantProcessAssoc` rows — the legal `(pollutant,
    /// crankcase process)` pairings.
    pub crankcase_pollutant_process_assoc: Vec<CrankcasePollutantProcessAssocRow>,
    /// `MOVESWorkerOutput` rows — the exhaust-process emission records the
    /// crankcase algorithm rescales. Rows whose process is not a source
    /// exhaust process (1, 2, 90) are ignored, as the SQL's `WHERE` clause
    /// does.
    pub worker_output: Vec<MovesWorkerOutputRow>,
}

/// The crankcase emission calculator's shared algorithm — the port of the
/// abstract Java base class `CrankcaseEmissionCalculator`.
///
/// The Java base is never instantiated directly (only the
/// [`NonPM`](CrankcaseEmissionCalculatorNonPM) /
/// [`PM`](CrankcaseEmissionCalculatorPM) subclasses are) and carries no
/// `Registration` directive, so it is **not** a [`Calculator`]: it has no
/// runtime identity to register. It exists here purely to host
/// [`calculate`](Self::calculate) — the SQL "Processing" section — and the
/// process-pairing constant both variants share.
#[derive(Debug, Clone, Copy, Default)]
pub struct CrankcaseEmissionCalculator;

impl CrankcaseEmissionCalculator {
    /// `(source exhaust process, crankcase process)` pairs — the running,
    /// start and extended-idle exhaust processes mapped to their crankcase
    /// counterparts.
    ///
    /// The Java base's `processIDs` array holds the same pairing (as
    /// `{crankcase, source}` flattened pairs); the SQL "Processing" join keys
    /// off `source → crankcase`, so the port stores it source-first.
    pub const PROCESS_PAIRS: [(i32, i32); 3] = [(1, 15), (2, 16), (90, 17)];

    /// Compute the crankcase emission rows — the port of the
    /// `CrankcaseEmissionCalculator.sql` "Processing" section.
    ///
    /// Each input exhaust row (`MOVESWorkerOutput` row with a source process
    /// 1 / 2 / 90) is matched to its crankcase `(pollutant, process)` pairing
    /// and then to the `CrankcaseEmissionRatio` rows for its
    /// `(polProcess, sourceType, regClass, fuelType)` cell; for every ratio
    /// whose model-year window contains the row's model year, one crankcase
    /// row is emitted with the process remapped and `emissionQuant` /
    /// `emissionRate` scaled by `crankcaseRatio`. Every join is an
    /// `INNER JOIN`, so a row that fails to match on any key is dropped.
    ///
    /// `produce_sulfate_pm10` ports the SQL's optional `-- Section
    /// SulfatePM10`: when `true`, every crankcase row for pollutant 115
    /// (Sulfate Particulate) is additionally copied as a pollutant-105
    /// (Sulfate PM10) row. The Java `doExecute` enables that section only
    /// when pollutant 105 is in the calculator's set, i.e. only for the
    /// [`PM`](CrankcaseEmissionCalculatorPM) variant — pass `false` for
    /// [`NonPM`](CrankcaseEmissionCalculatorNonPM).
    ///
    /// The result is sorted by its integer dimension columns for deterministic
    /// output; MOVES leaves `MOVESWorkerOutput` physically unordered.
    #[must_use]
    pub fn calculate(
        inputs: &CrankcaseInputs,
        produce_sulfate_pm10: bool,
    ) -> Vec<MovesWorkerOutputRow> {
        // CrankcasePollutantProcessAssoc, indexed (pollutantID, crankcase
        // processID) → polProcessID. polProcessID is unique per
        // (pollutant, process), so the map value is unique.
        let pol_process_of: HashMap<(i32, i32), i32> = inputs
            .crankcase_pollutant_process_assoc
            .iter()
            .map(|ppa| ((ppa.pollutant_id, ppa.process_id), ppa.pol_process_id))
            .collect();

        // CrankcaseEmissionRatio, indexed (polProcessID, sourceTypeID,
        // regClassID, fuelTypeID) → ratio rows. The table's primary key
        // includes the model-year range, so a cell may carry several rows;
        // the per-row model-year test below picks the matching one(s).
        let mut ratio_index: HashMap<(i32, i32, i32, i32), Vec<&CrankcaseEmissionRatioRow>> =
            HashMap::new();
        for r in &inputs.crankcase_emission_ratio {
            ratio_index
                .entry((
                    r.pol_process_id,
                    r.source_type_id,
                    r.reg_class_id,
                    r.fuel_type_id,
                ))
                .or_default()
                .push(r);
        }

        // --- main ratio multiply: the SQL's main INSERT … SELECT ----------
        let mut out: Vec<MovesWorkerOutputRow> = Vec::new();
        for mwo in &inputs.worker_output {
            // WHERE (mwo.processID=1 and ppa.processID=15) or (2,16) or (90,17)
            // — an exhaust row for any other process is dropped here.
            let Some(crankcase_process) = crankcase_process_of(mwo.process_id) else {
                continue;
            };
            // INNER JOIN ppa ON ppa.pollutantID = mwo.pollutantID, paired to
            // the crankcase process by the WHERE clause above.
            let Some(&pol_process_id) = pol_process_of.get(&(mwo.pollutant_id, crankcase_process))
            else {
                continue;
            };
            // INNER JOIN r ON polProcessID, sourceTypeID, regClassID,
            // fuelTypeID.
            let Some(ratios) = ratio_index.get(&(
                pol_process_id,
                mwo.source_type_id,
                mwo.reg_class_id,
                mwo.fuel_type_id,
            )) else {
                continue;
            };
            for r in ratios {
                // … and r.minModelYearID <= mwo.modelYearID <= r.maxModelYearID.
                if mwo.model_year_id < r.min_model_year_id
                    || mwo.model_year_id > r.max_model_year_id
                {
                    continue;
                }
                out.push(MovesWorkerOutputRow {
                    // emissionQuant/emissionRate × crankcaseRatio; the
                    // process is remapped to the crankcase process; every
                    // other column is carried through from the exhaust row.
                    process_id: crankcase_process,
                    emission_quant: mwo.emission_quant * r.crankcase_ratio,
                    emission_rate: mwo.emission_rate * r.crankcase_ratio,
                    ..*mwo
                });
            }
        }

        // --- SulfatePM10 section (PM variant only) ------------------------
        // The SQL reads `from MOVESWorkerOutput where pollutantID = 115 and
        // processID in (15, 16, 17)`. At this point MOVESWorkerOutput is the
        // original input plus the main-step output; crankcase processes are
        // produced only here, so in practice only main-step rows match — the
        // port still scans both, mirroring the literal SQL.
        if produce_sulfate_pm10 {
            let sulfate_pm10_rows: Vec<MovesWorkerOutputRow> = inputs
                .worker_output
                .iter()
                .chain(out.iter())
                .filter(|row| {
                    row.pollutant_id == SULFATE_PARTICULATE_POLLUTANT
                        && is_crankcase_process(row.process_id)
                })
                .map(|row| MovesWorkerOutputRow {
                    pollutant_id: SULFATE_PM10_POLLUTANT,
                    ..*row
                })
                .collect();
            out.extend(sulfate_pm10_rows);
        }

        out.sort_unstable_by_key(MovesWorkerOutputRow::dimension_key);
        out
    }
}

/// Map a source exhaust process to its crankcase process — `1 → 15`,
/// `2 → 16`, `90 → 17`. Returns `None` for any other process, which the
/// caller treats as "this row is not crankcase input".
fn crankcase_process_of(source_process: i32) -> Option<i32> {
    CrankcaseEmissionCalculator::PROCESS_PAIRS
        .iter()
        .copied()
        .find(|&(source, _)| source == source_process)
        .map(|(_, crankcase)| crankcase)
}

/// Whether `process_id` is one of the three crankcase processes (15, 16, 17).
fn is_crankcase_process(process_id: i32) -> bool {
    CrankcaseEmissionCalculator::PROCESS_PAIRS
        .iter()
        .any(|&(_, crankcase)| crankcase == process_id)
}

/// The 60 pollutants `CrankcaseEmissionCalculatorNonPM` produces crankcase
/// emissions for — the distinct `pollutantID`s recorded in its `Registration`
/// directives in `CalculatorInfo.txt`.
///
/// The Java constructor lists 63 IDs; ids 47, 179 and 186 are not legal MOVES
/// pollutants and the base constructor skips them (see the [module
/// documentation](self)). The order here is the order the IDs appear in the
/// Java constructor, minus those three.
const NONPM_POLLUTANT_IDS: [u16; 60] = [
    1, 2, 3, 5, 6, 20, 21, 22, 23, 24, 25, 26, 27, 30, 31, 32, 33, 34, 79, 80, 86, 87, 40, 41, 42,
    43, 44, 45, 46, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 81, 82, 83, 84, 168, 169, 170, 171,
    172, 173, 174, 175, 176, 177, 178, 181, 182, 183, 184, 185,
];

/// The three crankcase exhaust processes — Crankcase Running Exhaust (15),
/// Crankcase Start Exhaust (16), Crankcase Extended Idle Exhaust (17).
const CRANKCASE_PROCESS_IDS: [u16; 3] = [15, 16, 17];

/// Build the 180 `(pollutant, process)` registration pairs for the NonPM
/// variant — the Cartesian product of [`NONPM_POLLUTANT_IDS`] (60) and
/// [`CRANKCASE_PROCESS_IDS`] (3), matching `registrations_count: 180` in
/// `calculator-dag.json`.
const fn build_nonpm_registrations() -> [PollutantProcessAssociation; 180] {
    let mut regs = [PollutantProcessAssociation {
        pollutant_id: PollutantId(0),
        process_id: ProcessId(0),
    }; 180];
    let mut i = 0;
    let mut p = 0;
    while p < NONPM_POLLUTANT_IDS.len() {
        let mut q = 0;
        while q < CRANKCASE_PROCESS_IDS.len() {
            regs[i] = PollutantProcessAssociation {
                pollutant_id: PollutantId(NONPM_POLLUTANT_IDS[p]),
                process_id: ProcessId(CRANKCASE_PROCESS_IDS[q]),
            };
            i += 1;
            q += 1;
        }
        p += 1;
    }
    regs
}

/// The 180 `(pollutant, process)` pairs `CrankcaseEmissionCalculatorNonPM`
/// registers — see [`build_nonpm_registrations`].
static NONPM_REGISTRATIONS: [PollutantProcessAssociation; 180] = build_nonpm_registrations();

/// Both crankcase variants are chained calculators — `subscribes_directly:
/// false` — so they declare no MasterLoop subscription.
static NO_SUBSCRIPTIONS: &[CalculatorSubscription] = &[];

/// The PM variant is superseded by `SulfatePMCalculator` and registers
/// nothing — see the [module documentation](self).
static NO_REGISTRATIONS: &[PollutantProcessAssociation] = &[];

/// The upstream calculators `CrankcaseEmissionCalculatorNonPM` chains off —
/// the six calculators producing the non-PM pollutants in running, start and
/// extended-idle exhaust. `calculator-dag.json` records this `depends_on` set.
static NONPM_UPSTREAM: &[&str] = &[
    "AirToxicsCalculator",
    "BaseRateCalculator",
    "HCSpeciationCalculator",
    "NO2Calculator",
    "NOCalculator",
    "SO2Calculator",
];

/// The PM variant is absent from `calculator-dag.json`, so no `depends_on`
/// set is recorded for it — its `upstream` is empty.
static NO_UPSTREAM: &[&str] = &[];

/// Default-DB tables the crankcase computation consumes. `CrankcaseEmissionRatio`
/// holds the ratios; `PollutantProcessAssoc` is the source of the extracted
/// `CrankcasePollutantProcessAssoc`; `MOVESWorkerOutput` carries the upstream
/// exhaust emissions. The SQL also joins `RunSpecSourceFuelType`, but that
/// only narrows the extract and does not feed the algorithm, so it is not
/// listed (matching the `SO2Calculator` / `DistanceCalculator` treatment of
/// their `RunSpec*` joins). Both variants read the same tables.
static INPUT_TABLES: &[&str] = &[
    "CrankcaseEmissionRatio",
    "MOVESWorkerOutput",
    "PollutantProcessAssoc",
];

/// The MOVES crankcase emission calculator for the **non-particulate**
/// pollutants — the port of `CrankcaseEmissionCalculatorNonPM`.
///
/// A zero-sized value type: it owns no per-run state, exactly as the
/// [`Calculator`] trait contract requires. All run-varying input flows through
/// the [`CrankcaseInputs`] argument to [`calculate`](Self::calculate).
#[derive(Debug, Clone, Copy, Default)]
pub struct CrankcaseEmissionCalculatorNonPM;

impl CrankcaseEmissionCalculatorNonPM {
    /// Stable module name — matches the Java class and the chain-DAG entry.
    pub const NAME: &'static str = NONPM_NAME;

    /// Compute the non-PM crankcase emission rows.
    ///
    /// Delegates to [`CrankcaseEmissionCalculator::calculate`] with the
    /// SulfatePM10 section disabled: pollutant 105 is not in the NonPM
    /// pollutant set, so the Java `doExecute` never enables that section for
    /// this variant.
    #[must_use]
    pub fn calculate(&self, inputs: &CrankcaseInputs) -> Vec<MovesWorkerOutputRow> {
        CrankcaseEmissionCalculator::calculate(inputs, false)
    }
}

impl Calculator for CrankcaseEmissionCalculatorNonPM {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    /// `CrankcaseEmissionCalculatorNonPM` is a chained calculator:
    /// `calculator-dag.json` records `subscribes_directly: false` and an empty
    /// `subscriptions` list.
    fn subscriptions(&self) -> &[CalculatorSubscription] {
        NO_SUBSCRIPTIONS
    }

    /// The 180 `(pollutant, process)` pairs from `CalculatorInfo.txt` — 60
    /// pollutants × the 3 crankcase processes. See the [module
    /// documentation](self) for the 63 → 60 reconciliation.
    fn registrations(&self) -> &[PollutantProcessAssociation] {
        &NONPM_REGISTRATIONS
    }

    /// The six calculators this one chains off — `calculator-dag.json`
    /// records them as `depends_on`.
    fn upstream(&self) -> &[&'static str] {
        NONPM_UPSTREAM
    }

    fn input_tables(&self) -> &[&'static str] {
        INPUT_TABLES
    }

    /// Phase 2 skeleton — returns an empty [`CalculatorOutput`].
    ///
    /// [`CalculatorContext`] cannot yet surface the input tables or accept the
    /// `MOVESWorkerOutput` rows — its row storage lands with the Task 50
    /// `DataFrameStore`. The computation itself is ported and tested in
    /// [`CrankcaseEmissionCalculator::calculate`]; see the [module
    /// documentation](self).
    fn execute(&self, _ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
        Ok(CalculatorOutput::empty())
    }
}

/// The MOVES crankcase emission calculator for the **particulate** pollutants
/// — the port of `CrankcaseEmissionCalculatorPM`.
///
/// Superseded by `SulfatePMCalculator` and absent from the pinned MOVES
/// runtime registration file; the algorithm is ported for reference (see the
/// [module documentation](self)). A zero-sized value type, like its NonPM
/// sibling.
#[derive(Debug, Clone, Copy, Default)]
pub struct CrankcaseEmissionCalculatorPM;

impl CrankcaseEmissionCalculatorPM {
    /// Stable module name — matches the Java class.
    pub const NAME: &'static str = PM_NAME;

    /// Compute the PM crankcase emission rows.
    ///
    /// Delegates to [`CrankcaseEmissionCalculator::calculate`] with the
    /// SulfatePM10 section enabled: pollutant 105 is in the PM pollutant set,
    /// so the Java `doExecute` enables that section for this variant — the
    /// pollutant-115 crankcase rows are additionally relabelled as
    /// pollutant 105.
    #[must_use]
    pub fn calculate(&self, inputs: &CrankcaseInputs) -> Vec<MovesWorkerOutputRow> {
        CrankcaseEmissionCalculator::calculate(inputs, true)
    }
}

impl Calculator for CrankcaseEmissionCalculatorPM {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    /// `CrankcaseEmissionCalculatorPM` shares the chained-calculator base, so
    /// it declares no MasterLoop subscription.
    fn subscriptions(&self) -> &[CalculatorSubscription] {
        NO_SUBSCRIPTIONS
    }

    /// Empty — the PM variant is superseded by `SulfatePMCalculator` and
    /// carries no `Registration` directive in `CalculatorInfo.txt`. Returning
    /// an empty slice keeps this port consistent with the runtime and
    /// prevents a double-registration against `SulfatePMCalculator`. See the
    /// [module documentation](self).
    fn registrations(&self) -> &[PollutantProcessAssociation] {
        NO_REGISTRATIONS
    }

    /// Empty — the PM variant has no `calculator-dag.json` entry, so no
    /// `depends_on` set is recorded for it.
    fn upstream(&self) -> &[&'static str] {
        NO_UPSTREAM
    }

    fn input_tables(&self) -> &[&'static str] {
        INPUT_TABLES
    }

    /// Phase 2 skeleton — returns an empty [`CalculatorOutput`]. See
    /// [`CrankcaseEmissionCalculatorNonPM::execute`] and the [module
    /// documentation](self).
    fn execute(&self, _ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
        Ok(CalculatorOutput::empty())
    }
}

/// Construct the NonPM crankcase calculator as a boxed trait object — matches
/// the engine's calculator-factory signature so the registry can register it.
#[must_use]
pub fn nonpm_factory() -> Box<dyn Calculator> {
    Box::new(CrankcaseEmissionCalculatorNonPM)
}

/// Construct the PM crankcase calculator as a boxed trait object.
///
/// The PM variant is superseded (see the [module documentation](self)); the
/// factory is provided for completeness and for the algorithm cross-validation
/// tests.
#[must_use]
pub fn pm_factory() -> Box<dyn Calculator> {
    Box::new(CrankcaseEmissionCalculatorPM)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `polProcessID = pollutantID × 100 + processID` — the MOVES composite
    /// id, used to keep the test fixtures self-consistent.
    fn polproc(pollutant: i32, process: i32) -> i32 {
        pollutant * 100 + process
    }

    /// One exhaust input row: pollutant 2 (CO), Running Exhaust (process 1),
    /// `emissionQuant = 200.0`, `emissionRate = 5.0`, in the source/reg/fuel
    /// cell `(21, 30, 2)` and model year 2018.
    fn exhaust_row() -> MovesWorkerOutputRow {
        MovesWorkerOutputRow {
            year_id: 2020,
            month_id: 7,
            day_id: 5,
            hour_id: 8,
            state_id: 26,
            county_id: 26_161,
            zone_id: 261_610,
            link_id: 5001,
            pollutant_id: 2,
            process_id: 1,
            source_type_id: 21,
            reg_class_id: 30,
            fuel_type_id: 2,
            model_year_id: 2018,
            road_type_id: 4,
            emission_quant: 200.0,
            emission_rate: 5.0,
        }
    }

    /// A one-row input that yields exactly one crankcase row: CO Running
    /// Exhaust scaled by `crankcaseRatio = 0.05` → `emissionQuant = 10.0`,
    /// `emissionRate = 0.25`, on Crankcase Running Exhaust (process 15).
    fn minimal_inputs() -> CrankcaseInputs {
        CrankcaseInputs {
            crankcase_emission_ratio: vec![CrankcaseEmissionRatioRow {
                pol_process_id: polproc(2, 15),
                min_model_year_id: 1960,
                max_model_year_id: 2050,
                source_type_id: 21,
                reg_class_id: 30,
                fuel_type_id: 2,
                crankcase_ratio: 0.05,
            }],
            crankcase_pollutant_process_assoc: vec![CrankcasePollutantProcessAssocRow {
                pol_process_id: polproc(2, 15),
                process_id: 15,
                pollutant_id: 2,
            }],
            worker_output: vec![exhaust_row()],
        }
    }

    /// Assert `actual` matches `expected` within `f64` slack.
    fn assert_close(actual: f64, expected: f64) {
        assert!(
            (actual - expected).abs() < 1e-9,
            "{actual} != expected {expected}",
        );
    }

    #[test]
    fn calculate_minimal_input_yields_one_crankcase_row() {
        let rows = CrankcaseEmissionCalculatorNonPM.calculate(&minimal_inputs());
        assert_eq!(rows.len(), 1);
        let r = rows[0];
        // The dimension cell is carried straight from the exhaust row.
        assert_eq!(r.year_id, 2020);
        assert_eq!(r.month_id, 7);
        assert_eq!(r.day_id, 5);
        assert_eq!(r.hour_id, 8);
        assert_eq!(r.state_id, 26);
        assert_eq!(r.county_id, 26_161);
        assert_eq!(r.zone_id, 261_610);
        assert_eq!(r.link_id, 5001);
        assert_eq!(r.source_type_id, 21);
        assert_eq!(r.reg_class_id, 30);
        assert_eq!(r.fuel_type_id, 2);
        assert_eq!(r.model_year_id, 2018);
        assert_eq!(r.road_type_id, 4);
        // The pollutant is unchanged; the process is remapped 1 → 15.
        assert_eq!(r.pollutant_id, 2);
        assert_eq!(r.process_id, 15);
        // Both emission values are scaled by crankcaseRatio = 0.05.
        assert_close(r.emission_quant, 10.0);
        assert_close(r.emission_rate, 0.25);
    }

    #[test]
    fn calculate_remaps_each_source_process_to_its_crankcase_process() {
        // One exhaust row per source process (1, 2, 90), each with its own
        // ppa pairing and ratio; the output processes are 15, 16, 17.
        let mut inputs = minimal_inputs();
        for &(source, crankcase) in &CrankcaseEmissionCalculator::PROCESS_PAIRS {
            if source == 1 {
                continue; // already in minimal_inputs
            }
            inputs
                .crankcase_pollutant_process_assoc
                .push(CrankcasePollutantProcessAssocRow {
                    pol_process_id: polproc(2, crankcase),
                    process_id: crankcase,
                    pollutant_id: 2,
                });
            inputs
                .crankcase_emission_ratio
                .push(CrankcaseEmissionRatioRow {
                    pol_process_id: polproc(2, crankcase),
                    min_model_year_id: 1960,
                    max_model_year_id: 2050,
                    source_type_id: 21,
                    reg_class_id: 30,
                    fuel_type_id: 2,
                    crankcase_ratio: 0.05,
                });
            inputs.worker_output.push(MovesWorkerOutputRow {
                process_id: source,
                ..exhaust_row()
            });
        }

        let rows = CrankcaseEmissionCalculatorNonPM.calculate(&inputs);
        let mut processes: Vec<i32> = rows.iter().map(|r| r.process_id).collect();
        processes.sort_unstable();
        assert_eq!(processes, vec![15, 16, 17]);
    }

    #[test]
    fn calculate_drops_exhaust_row_with_a_non_source_process() {
        // A row already on a crankcase process (15) is not crankcase input —
        // the SQL's WHERE clause only matches source processes 1, 2, 90.
        let mut inputs = minimal_inputs();
        inputs.worker_output[0].process_id = 15;
        assert!(CrankcaseEmissionCalculatorNonPM
            .calculate(&inputs)
            .is_empty());

        // Likewise an unrelated process such as evaporative permeation (11).
        let mut other = minimal_inputs();
        other.worker_output[0].process_id = 11;
        assert!(CrankcaseEmissionCalculatorNonPM
            .calculate(&other)
            .is_empty());
    }

    #[test]
    fn calculate_drops_exhaust_row_without_a_pollutant_process_pairing() {
        // The exhaust row's pollutant has no CrankcasePollutantProcessAssoc
        // entry — the ppa inner join drops it.
        let mut inputs = minimal_inputs();
        inputs.crankcase_pollutant_process_assoc.clear();
        assert!(CrankcaseEmissionCalculatorNonPM
            .calculate(&inputs)
            .is_empty());
    }

    #[test]
    fn calculate_drops_exhaust_row_without_a_matching_ratio() {
        // No ratio row at all → the CrankcaseEmissionRatio inner join drops it.
        let mut no_ratio = minimal_inputs();
        no_ratio.crankcase_emission_ratio.clear();
        assert!(CrankcaseEmissionCalculatorNonPM
            .calculate(&no_ratio)
            .is_empty());

        // A ratio exists but for a different source/reg/fuel cell.
        let mut wrong_cell = minimal_inputs();
        wrong_cell.crankcase_emission_ratio[0].fuel_type_id = 99;
        assert!(CrankcaseEmissionCalculatorNonPM
            .calculate(&wrong_cell)
            .is_empty());
    }

    #[test]
    fn calculate_applies_the_model_year_window() {
        // The ratio covers model years 2015..=2020.
        let mut inputs = minimal_inputs();
        inputs.crankcase_emission_ratio[0].min_model_year_id = 2015;
        inputs.crankcase_emission_ratio[0].max_model_year_id = 2020;

        // 2018 is inside the window — one row.
        assert_eq!(CrankcaseEmissionCalculatorNonPM.calculate(&inputs).len(), 1);

        // The window bounds are inclusive.
        inputs.worker_output[0].model_year_id = 2015;
        assert_eq!(CrankcaseEmissionCalculatorNonPM.calculate(&inputs).len(), 1);
        inputs.worker_output[0].model_year_id = 2020;
        assert_eq!(CrankcaseEmissionCalculatorNonPM.calculate(&inputs).len(), 1);

        // A model year just outside the window — no row.
        inputs.worker_output[0].model_year_id = 2014;
        assert!(CrankcaseEmissionCalculatorNonPM
            .calculate(&inputs)
            .is_empty());
        inputs.worker_output[0].model_year_id = 2021;
        assert!(CrankcaseEmissionCalculatorNonPM
            .calculate(&inputs)
            .is_empty());
    }

    #[test]
    fn calculate_emits_one_row_per_matching_model_year_window() {
        // Two ratio rows for the same cell with overlapping windows both
        // contain model year 2018 — the INNER JOIN matches both, so the port
        // emits two crankcase rows.
        let mut inputs = minimal_inputs();
        inputs
            .crankcase_emission_ratio
            .push(CrankcaseEmissionRatioRow {
                pol_process_id: polproc(2, 15),
                min_model_year_id: 2010,
                max_model_year_id: 2025,
                source_type_id: 21,
                reg_class_id: 30,
                fuel_type_id: 2,
                crankcase_ratio: 0.20,
            });

        let rows = CrankcaseEmissionCalculatorNonPM.calculate(&inputs);
        assert_eq!(rows.len(), 2);
        let mut quants: Vec<f64> = rows.iter().map(|r| r.emission_quant).collect();
        quants.sort_by(f64::total_cmp);
        assert_close(quants[0], 10.0); // 200.0 × 0.05
        assert_close(quants[1], 40.0); // 200.0 × 0.20
    }

    #[test]
    fn calculate_nonpm_does_not_run_the_sulfate_pm10_section() {
        // A pollutant-115 exhaust row produces a pollutant-115 crankcase row;
        // the NonPM variant must not derive a pollutant-105 row from it.
        let mut inputs = minimal_inputs();
        inputs.worker_output[0].pollutant_id = SULFATE_PARTICULATE_POLLUTANT;
        inputs.crankcase_pollutant_process_assoc[0] = CrankcasePollutantProcessAssocRow {
            pol_process_id: polproc(SULFATE_PARTICULATE_POLLUTANT, 15),
            process_id: 15,
            pollutant_id: SULFATE_PARTICULATE_POLLUTANT,
        };
        inputs.crankcase_emission_ratio[0].pol_process_id =
            polproc(SULFATE_PARTICULATE_POLLUTANT, 15);

        let rows = CrankcaseEmissionCalculatorNonPM.calculate(&inputs);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].pollutant_id, SULFATE_PARTICULATE_POLLUTANT);
    }

    #[test]
    fn calculate_pm_derives_sulfate_pm10_from_sulfate_particulate() {
        // A pollutant-115 (Sulfate Particulate) exhaust row: the PM variant
        // produces the pollutant-115 crankcase row and additionally relabels
        // it as a pollutant-105 (Sulfate PM10) row with the same values.
        let mut inputs = minimal_inputs();
        inputs.worker_output[0].pollutant_id = SULFATE_PARTICULATE_POLLUTANT;
        inputs.crankcase_pollutant_process_assoc[0] = CrankcasePollutantProcessAssocRow {
            pol_process_id: polproc(SULFATE_PARTICULATE_POLLUTANT, 15),
            process_id: 15,
            pollutant_id: SULFATE_PARTICULATE_POLLUTANT,
        };
        inputs.crankcase_emission_ratio[0].pol_process_id =
            polproc(SULFATE_PARTICULATE_POLLUTANT, 15);

        let rows = CrankcaseEmissionCalculatorPM.calculate(&inputs);
        assert_eq!(rows.len(), 2);

        let pm10 = rows
            .iter()
            .find(|r| r.pollutant_id == SULFATE_PM10_POLLUTANT)
            .expect("a Sulfate PM10 row");
        let pm25 = rows
            .iter()
            .find(|r| r.pollutant_id == SULFATE_PARTICULATE_POLLUTANT)
            .expect("a Sulfate Particulate row");
        // The PM10 row is a verbatim copy of the PM2.5 sulfate row but for
        // the pollutant id — same process, same cell, same emissions.
        assert_eq!(pm10.process_id, 15);
        assert_eq!(pm10.process_id, pm25.process_id);
        assert_close(pm10.emission_quant, pm25.emission_quant);
        assert_close(pm10.emission_rate, pm25.emission_rate);
        assert_close(pm10.emission_quant, 10.0); // 200.0 × 0.05
    }

    #[test]
    fn calculate_pm_without_sulfate_particulate_emits_no_sulfate_pm10() {
        // The PM variant on a non-115 pollutant runs the SulfatePM10 section
        // but finds nothing to relabel — only the main crankcase row remains.
        let rows = CrankcaseEmissionCalculatorPM.calculate(&minimal_inputs());
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].pollutant_id, 2);
        assert!(rows
            .iter()
            .all(|r| r.pollutant_id != SULFATE_PM10_POLLUTANT));
    }

    #[test]
    fn calculate_output_is_sorted_by_dimension_key() {
        // Two exhaust rows on distinct links produce two rows; the result
        // comes back dimension-key sorted regardless of input order.
        let mut inputs = minimal_inputs();
        inputs.worker_output.insert(
            0,
            MovesWorkerOutputRow {
                link_id: 9999, // sorts after link 5001
                ..exhaust_row()
            },
        );

        let rows = CrankcaseEmissionCalculatorNonPM.calculate(&inputs);
        assert_eq!(rows.len(), 2);
        assert!(
            rows.windows(2)
                .all(|w| w[0].dimension_key() <= w[1].dimension_key()),
            "calculate output is not sorted by dimension key",
        );
        assert_eq!(rows[0].link_id, 5001);
        assert_eq!(rows[1].link_id, 9999);
    }

    #[test]
    fn calculate_empty_input_yields_no_rows() {
        let empty = CrankcaseInputs::default();
        assert!(CrankcaseEmissionCalculator::calculate(&empty, false).is_empty());
        assert!(CrankcaseEmissionCalculator::calculate(&empty, true).is_empty());
    }

    #[test]
    fn process_pairs_map_source_exhaust_to_crankcase() {
        assert_eq!(
            CrankcaseEmissionCalculator::PROCESS_PAIRS,
            [(1, 15), (2, 16), (90, 17)],
        );
        assert_eq!(crankcase_process_of(1), Some(15));
        assert_eq!(crankcase_process_of(2), Some(16));
        assert_eq!(crankcase_process_of(90), Some(17));
        assert_eq!(crankcase_process_of(11), None);
        assert!(is_crankcase_process(15));
        assert!(is_crankcase_process(17));
        assert!(!is_crankcase_process(1));
    }

    #[test]
    fn calculator_names_match_the_java_classes() {
        assert_eq!(
            CrankcaseEmissionCalculatorNonPM.name(),
            "CrankcaseEmissionCalculatorNonPM",
        );
        assert_eq!(
            CrankcaseEmissionCalculatorPM.name(),
            "CrankcaseEmissionCalculatorPM",
        );
    }

    #[test]
    fn both_variants_are_chained_calculators_with_no_subscriptions() {
        // calculator-dag.json: subscribes_directly false, subscriptions [].
        assert!(CrankcaseEmissionCalculatorNonPM.subscriptions().is_empty());
        assert!(CrankcaseEmissionCalculatorPM.subscriptions().is_empty());
    }

    #[test]
    fn nonpm_registers_180_pairs_over_60_pollutants_and_3_processes() {
        // calculator-dag.json records registrations_count 180 for
        // CrankcaseEmissionCalculatorNonPM.
        let regs = CrankcaseEmissionCalculatorNonPM.registrations();
        assert_eq!(regs.len(), 180);

        // 60 distinct pollutants.
        let mut pollutants: Vec<u16> = regs.iter().map(|r| r.pollutant_id.0).collect();
        pollutants.sort_unstable();
        pollutants.dedup();
        assert_eq!(pollutants.len(), 60);

        // The three crankcase processes, 60 registrations each.
        let mut processes: Vec<u16> = regs.iter().map(|r| r.process_id.0).collect();
        processes.sort_unstable();
        let distinct: Vec<u16> = {
            let mut p = processes.clone();
            p.dedup();
            p
        };
        assert_eq!(distinct, vec![15, 16, 17]);
        for process in [15, 16, 17] {
            assert_eq!(
                regs.iter().filter(|r| r.process_id.0 == process).count(),
                60,
            );
        }

        // Spot-check a few pairs from CalculatorInfo.txt: Total Gaseous
        // Hydrocarbons (1) and Volatile Organic Compounds (87) on Crankcase
        // Running Exhaust (15).
        for pollutant in [1_u16, 87] {
            assert!(regs.contains(&PollutantProcessAssociation {
                pollutant_id: PollutantId(pollutant),
                process_id: ProcessId(15),
            }));
        }
        // The skipped Java ids 47 / 179 / 186 are not registered.
        for skipped in [47_u16, 179, 186] {
            assert!(regs.iter().all(|r| r.pollutant_id.0 != skipped));
        }
    }

    #[test]
    fn pm_registers_nothing_being_superseded() {
        // CrankcaseEmissionCalculatorPM is absent from CalculatorInfo.txt —
        // superseded by SulfatePMCalculator.
        assert!(CrankcaseEmissionCalculatorPM.registrations().is_empty());
        assert!(CrankcaseEmissionCalculatorPM.upstream().is_empty());
    }

    #[test]
    fn nonpm_chains_off_the_six_dag_calculators() {
        // calculator-dag.json records this depends_on set.
        assert_eq!(
            CrankcaseEmissionCalculatorNonPM.upstream(),
            &[
                "AirToxicsCalculator",
                "BaseRateCalculator",
                "HCSpeciationCalculator",
                "NO2Calculator",
                "NOCalculator",
                "SO2Calculator",
            ],
        );
    }

    #[test]
    fn both_variants_declare_input_tables() {
        for tables in [
            CrankcaseEmissionCalculatorNonPM.input_tables(),
            CrankcaseEmissionCalculatorPM.input_tables(),
        ] {
            for expected in [
                "CrankcaseEmissionRatio",
                "MOVESWorkerOutput",
                "PollutantProcessAssoc",
            ] {
                assert!(tables.contains(&expected), "missing input table {expected}");
            }
        }
    }

    #[test]
    fn execute_is_a_shell_until_the_data_plane_lands() {
        let ctx = CalculatorContext::new();
        assert!(CrankcaseEmissionCalculatorNonPM.execute(&ctx).is_ok());
        assert!(CrankcaseEmissionCalculatorPM.execute(&ctx).is_ok());
    }

    #[test]
    fn factories_build_named_calculators() {
        assert_eq!(nonpm_factory().name(), "CrankcaseEmissionCalculatorNonPM");
        assert_eq!(pm_factory().name(), "CrankcaseEmissionCalculatorPM");
    }

    #[test]
    fn calculators_are_object_safe() {
        // The registry stores calculators as Box<dyn Calculator>.
        let nonpm: Box<dyn Calculator> = Box::new(CrankcaseEmissionCalculatorNonPM);
        let pm: Box<dyn Calculator> = Box::new(CrankcaseEmissionCalculatorPM);
        assert_eq!(nonpm.name(), "CrankcaseEmissionCalculatorNonPM");
        assert_eq!(pm.name(), "CrankcaseEmissionCalculatorPM");
    }
}
