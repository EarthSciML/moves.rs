//! Port of `EvaporativeEmissionsOperatingModeDistributionGenerator.java`
//! (`gov.epa.otaq.moves.master.implementation.ghg`) — the generator that
//! builds `OpModeDistribution` records for the evaporative-emission
//! processes.
//!
//! //!
//! # What this generator produces
//!
//! Evaporative emissions — tank/hose permeation, fuel-vapor venting and
//! fuel leaks — occur mostly while a vehicle is parked and *soaking*, not
//! while it is operating. This generator therefore splits each
//! `(sourceType, hourDay)` cell into the soak/parked operating modes plus
//! operating mode 300 ("Operating"), writing the per-`(sourceType, hourDay,
//! link, polProcess, opMode)` operating-mode fractions into the
//! execution-database `OpModeDistribution` table that the evaporative
//! calculators consume.
//!
//! Conceptually: an `hourDay`'s activity is part operating and part
//! soaking. The operating share comes from the activity tables
//! (`SHO` / `sourceHours`); the soaking share is sub-divided across the
//! soak operating modes by `SoakActivityFraction` (produced upstream by
//! `TankTemperatureGenerator`).
//!
//! # The three evaporative processes
//!
//! Java `subscribeToMe` names four processes — "Evap Permeation", "Evap
//! Fuel Vapor Venting", "Evap Fuel Leaks" and "Evap Non-Fuel Vapors" — and
//! subscribes only to the ones `EmissionProcess.findByName` resolves. "Evap
//! Non-Fuel Vapors" is not a process in the MOVES5 default database, so the
//! null-guard drops it: the generator subscribes to exactly three
//! processes — Evap Permeation (11), Evap Fuel Vapor Venting (12) and Evap
//! Fuel Leaks (13) — each at `MONTH` granularity / `GENERATOR` priority.
//! The pinned `CalculatorInfo.txt` runtime log records exactly those three
//! `Subscribe` directives, confirming the dropped fourth name.
//!
//! Java subscribes at `MONTH` granularity because evaporative activity is
//! temperature-dependent (so it varies by month) while `OpModeDistribution`
//! itself has no month column.
//!
//! # The algorithm
//!
//! Java `calculateOpModeDistribution` fires once per
//! `(process, link, month, year)` and runs three SQL steps, all tagged
//! `@step 010`. [`op_mode_distribution`] ports them:
//!
//! 1. **`FractionOfOperating`** — for each `(hourDayID, sourceTypeID)`,
//! `fractionOfOperating = least(1, COALESCE(SUM(SHO),0) / SUM(sourceHours))`,
//! summed across the age dimension. `SHO` is source-hours-operating and
//! `sourceHours` is total source-hours; their ratio is the operating
//! share of activity. See [`fraction_of_operating`].
//! 2. **Non-operating modes** — join `FractionOfOperating` to
//! `SoakActivityFraction` (on `sourceType, hourDay`), to `OpModePolProcAssoc`
//! (on `opMode`) and to `PollutantProcessAssoc` (on `polProcess`, filtered
//! to the loop's process). Each joined row gets
//! `opModeFraction = soakActivityFraction * (1 - fractionOfOperating)`:
//! the soaking share of activity, sub-divided across soak op modes.
//! 3. **Operating mode 300** — for each `(sourceType, hourDay, link,
//! polProcess)` group, add an op-mode-300 row carrying whatever fraction
//! the non-operating modes left:
//! `opModeFraction = greatest(0, 1 - SUM(opModeFraction))`.
//!
//! The rows are finally written to `OpModeDistribution` with a MySQL
//! `INSERT IGNORE` (`isUserInput = 'N'`), so a user-supplied
//! (`isUserInput = 'Y'`) row for the same primary key is left in place.
//!
//! # Fidelity note
//!
//! MOVES stores `FractionOfOperating.fractionOfOperating` and
//! `OpModeDistribution.opModeFraction` in `FLOAT` (32-bit) columns, while
//! MySQL evaluates the arithmetic itself in `DOUBLE`. This port computes
//! and carries every fraction in `f64`, the way the rest of the Rust port
//! does; it does not reproduce the intermediate `FLOAT`-column truncation
//! of `FractionOfOperating`. The resulting divergence is on the order of
//! the `f32` round-off (~1e-7 relative) and is well within any reasonable
//! tolerance budget; (generator integration validation) is where
//! canonical captures decide whether bug-compatible `f32` truncation is
//! ever required here.
//!
//! # Data plane
//!
//! [`Generator::execute`] receives a [`CalculatorContext`] whose
//! `ExecutionTables` / `ScratchNamespace` are Phase-2 placeholders until
//! the `DataFrameStore` lands (), so `execute` cannot
//! yet read the input tables nor write `OpModeDistribution`. The
//! numerically faithful algorithm is fully ported and unit-tested in the
//! free functions [`fraction_of_operating`] and [`op_mode_distribution`];
//! once the data plane exists, `execute` projects an [`EvapOpModeContext`]
//! from `ctx.position()` and an [`EvapOpModeInputs`] from `ctx.tables()`,
//! calls [`op_mode_distribution`], and `INSERT IGNORE`s the result into the
//! scratch `OpModeDistribution` table.
//!
//! # Out of scope
//!
//! `cleanDataLoop` and the `contextForLink`-keyed cross-year/month
//! `DELETE FROM OpModeDistribution` are MariaDB execution-database
//! lifecycle management: they keep one persistent `OpModeDistribution`
//! table correct as the master loop revisits a link in a new month. The
//! Rust scratch tier is produced fresh per iteration and owned by the
//! registry, so that bookkeeping has no analogue here and is not
//! ported. The vestigial `isMesoscaleLookup` field (assigned in the Java
//! but never read — mesoscale-lookup runs use a separate generator) is
//! likewise dropped.

use std::collections::{BTreeMap, HashMap, HashSet};

use moves_calculator_info::{Granularity, Priority};
use moves_data::{
    EmissionProcess, PolProcessId, PollutantId, PollutantProcessAssociation, ProcessId,
    SourceTypeId,
};
use moves_framework::{
    CalculatorContext, CalculatorOutput, CalculatorSubscription, DataFrameStore,
    DataFrameStoreTyped, Error, Generator, TableRow,
};
use polars::prelude::{DataFrame, DataType, NamedFrom, PolarsResult, Series};

/// The process names Java `subscribeToMe` looks up, in source order.
///
/// `EmissionProcess::find_by_name` resolves the first three against the
/// MOVES5 default database; "Evap Non-Fuel Vapors" resolves to nothing and
/// is dropped by the null-guard. See the module docs.
const EVAP_PROCESS_NAMES: [&str; 4] = [
    "Evap Permeation",
    "Evap Fuel Vapor Venting",
    "Evap Fuel Leaks",
    "Evap Non-Fuel Vapors",
];

/// Operating mode 300 — "Operating". Java step 3 adds one row in this mode
/// per `(sourceType, hourDay, link, polProcess)` group carrying the share
/// of activity the soak modes did not claim.
const OPERATING_OP_MODE: i16 = 300;

/// One `OpModeDistribution` row this generator contributes.
///
/// Models the six data columns the Java `INSERT IGNORE` populates. The
/// execution-database table also carries `opModeFractionCV` (left `NULL`/// this generator never sets it) and `isUserInput` (always `'N'` for a
/// generated row); neither is modeled here.
///
/// The `FLOAT` column `opModeFraction` is held as `f64` for consistency
/// with the rest of the Rust port; see the module-level *Fidelity note*.
///
/// Primary key — the `XPKOpModeDistribution` unique index from
/// `database/CreateDefault.sql`, and the `INSERT IGNORE` de-duplication
/// key: `(sourceTypeID, hourDayID, linkID, polProcessID, opModeID)`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OpModeDistributionRow {
 /// `sourceTypeID` — the MOVES source (vehicle) type.
    pub source_type_id: SourceTypeId,
 /// `hourDayID` — one of the RunSpec's selected hour/day combinations.
    pub hour_day_id: i16,
 /// `linkID` — the link currently iterating; constant for one
 /// generator invocation (`MONTH`-granularity loop, link in context).
    pub link_id: u32,
 /// `polProcessID` — the pollutant/process this fraction applies to.
    pub pol_process_id: PolProcessId,
 /// `opModeID` — the operating mode this fraction applies to; a soak
 /// op mode for the non-operating rows, `OPERATING_OP_MODE` for the
 /// operating row.
    pub op_mode_id: i16,
 /// `opModeFraction` — the share of this `(sourceType, hourDay,
 /// polProcess)` cell's activity that falls in this operating mode.
    pub op_mode_fraction: f64,
}

/// Primary-key tuple of `OpModeDistribution`, in primary-key order — the
/// columns the `INSERT IGNORE` de-duplicates on and the sort key giving
/// the generator output a deterministic order.
type RowKey = (SourceTypeId, i16, u32, PolProcessId, i16);

impl OpModeDistributionRow {
 /// The primary-key projection — see [`RowKey`].
    fn key(&self) -> RowKey {
        (
            self.source_type_id,
            self.hour_day_id,
            self.link_id,
            self.pol_process_id,
            self.op_mode_id,
        )
    }
}

/// A `sourceHours` row — total source-hours of activity for a
/// `(hourDay, sourceType, age, link, month, year)` cell.
///
/// Java step 1 filters this table to the loop's `(link, month, year)` and
/// to `sourceHours > 0`, then sums it as the denominator of
/// `fractionOfOperating`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SourceHoursRow {
 /// `hourDayID`.
    pub hour_day_id: i16,
 /// `sourceTypeID`.
    pub source_type_id: SourceTypeId,
 /// `ageID` — the age dimension `fractionOfOperating` sums across.
    pub age_id: i16,
 /// `linkID`.
    pub link_id: u32,
 /// `monthID`.
    pub month_id: u8,
 /// `yearID`.
    pub year_id: u16,
 /// `sourceHours` — total source-hours of activity.
    pub source_hours: f64,
}

/// An `SHO` row — source-hours-*operating* for a `(hourDay, sourceType,
/// age, link, month, year)` cell.
///
/// Java step 1 `LEFT JOIN`s this onto `sourceHours`; an unmatched
/// `sourceHours` row contributes `0` operating hours (`COALESCE(SUM(SHO),0)`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ShoRow {
 /// `hourDayID`.
    pub hour_day_id: i16,
 /// `sourceTypeID`.
    pub source_type_id: SourceTypeId,
 /// `ageID`.
    pub age_id: i16,
 /// `linkID`.
    pub link_id: u32,
 /// `monthID`.
    pub month_id: u8,
 /// `yearID`.
    pub year_id: u16,
 /// `SHO` — source-hours operating.
    pub sho: f64,
}

/// A `SoakActivityFraction` row — the fraction of an `hourDay`'s soaking
/// activity that falls in operating mode `opModeID`.
///
/// Produced upstream by `TankTemperatureGenerator`. The soak op modes are
/// the parked/cooling modes (150, 151, …); none is `OPERATING_OP_MODE`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SoakActivityFractionRow {
 /// `sourceTypeID`.
    pub source_type_id: SourceTypeId,
 /// `zoneID` — Java step 2 filters this to the loop's zone.
    pub zone_id: u32,
 /// `monthID` — Java step 2 filters this to the loop's month.
    pub month_id: u8,
 /// `hourDayID`.
    pub hour_day_id: i16,
 /// `opModeID` — a soak operating mode.
    pub op_mode_id: i16,
 /// `soakActivityFraction` — the soak op mode's share of soaking
 /// activity.
    pub soak_activity_fraction: f64,
}

/// An `OpModePolProcAssoc` row — which operating modes a pollutant/process
/// is associated with. Java step 2 joins it on `opModeID` to attach a
/// `polProcessID` to each `SoakActivityFraction` op mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpModePolProcAssoc {
 /// `polProcessID`.
    pub pol_process_id: PolProcessId,
 /// `opModeID`.
    pub op_mode_id: i16,
}

/// The master-loop position this generator fires for — one
/// `(process, link, zone, month, year)` tuple.
///
/// Once the data plane lands, [`Generator::execute`] builds this
/// from `ctx.position()`: `process_id` from the iteration process,
/// `link_id` / `zone_id` from `position().location` and `month_id` /
/// `year_id` from `position().time`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EvapOpModeContext {
 /// The evaporative process the loop is currently dispatching — one of
 /// 11, 12, 13.
    pub process_id: ProcessId,
 /// `Link.linkID` of the link currently iterating.
    pub link_id: u32,
 /// `Zone.zoneID` of the zone currently iterating.
    pub zone_id: u32,
 /// `MonthOfAnyYear.monthID` of the month currently iterating.
    pub month_id: u8,
 /// Calendar year currently iterating.
    pub year_id: u16,
}

/// The projected default-/scratch-database tables Java
/// `calculateOpModeDistribution` reads.
///
/// Each field is the Rust analogue of one MySQL table the Java `SELECT`s
/// reference. Once the data plane lands, [`Generator::execute`]
/// builds this view from `ctx.tables()`.
#[derive(Debug, Clone, Copy)]
pub struct EvapOpModeInputs<'a> {
 /// `sourceHours` — total source-hours; the `fractionOfOperating`
 /// denominator.
    pub source_hours: &'a [SourceHoursRow],
 /// `SHO` — source-hours-operating; the `fractionOfOperating` numerator.
    pub sho: &'a [ShoRow],
 /// `SoakActivityFraction` — soak-op-mode fractions of soaking activity.
    pub soak_activity_fraction: &'a [SoakActivityFractionRow],
 /// `OpModePolProcAssoc` — operating modes per pollutant/process.
    pub op_mode_pol_proc_assoc: &'a [OpModePolProcAssoc],
 /// `PollutantProcessAssoc` — every modeled `(pollutant, process)`
 /// pair; step 2 filters it by `processID`.
    pub pollutant_process_assoc: &'a [PollutantProcessAssociation],
}

/// One `FractionOfOperating` row — the operating share of activity for a
/// `(hourDayID, sourceTypeID)` cell. The intermediate Java step 1 builds.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FractionOfOperatingRow {
 /// `hourDayID`.
    pub hour_day_id: i16,
 /// `sourceTypeID`.
    pub source_type_id: SourceTypeId,
 /// `fractionOfOperating` — `least(1, SUM(SHO)/SUM(sourceHours))`, in
 /// `[0, 1]`.
    pub fraction_of_operating: f64,
}

/// Port of Java step 1 — build the `FractionOfOperating` table.
///
/// For each `(hourDayID, sourceTypeID)` cell, sum `SHO` and `sourceHours`
/// across the age dimension and take
/// `fractionOfOperating = least(1, COALESCE(SUM(SHO),0) / SUM(sourceHours))`.
///
/// Faithful details of the MySQL `SELECT`:
///
/// * `sourceHours` and `SHO` are filtered to `(link, month+1, year)` — both
/// tables are written by activity generators for the NEXT month relative to
/// the evap-OpMode iteration context, matching the Java behavior where
/// `##context.monthID##` in the sourceHours SQL resolves to the already-
/// prepared next-month data. Canonical snapshots confirm: a month=7 run
/// has sourceHours/SHO rows with monthID=8.
/// * `sourceHours > 0` (`WHERE sourceHours > 0`); a `NULL` or
/// non-positive `sourceHours` is dropped, exactly as `> 0` would.
/// * `SHO` joins on `(hourDayID, ageID, sourceTypeID)` within the context.
/// The Java `LEFT JOIN` means an unmatched `sourceHours` row contributes
/// `0` operating hours — `COALESCE(SUM(SHO), 0)`.
/// * `SUM(sourceHours)` is never zero: every grouped row passed
/// `sourceHours > 0`, so the division is always well defined.
/// * `least(1, …)` clamps the ratio — if a cell records more operating
/// hours than total source-hours, the operating share is capped at `1`.
///
/// The result is returned in `(hourDayID, sourceTypeID)` order, matching
/// the Java `ORDER BY`. `SHO` is assumed unique per `(hourDayID, ageID,
/// sourceTypeID)` within the context, as the activity tables guarantee.
#[must_use]
pub fn fraction_of_operating(
    ctx: &EvapOpModeContext,
    inputs: &EvapOpModeInputs<'_>,
) -> Vec<FractionOfOperatingRow> {
 // sourceHours and SHO are written for MONTH+1 (same convention as SAF).
    let next_month = if ctx.month_id == 12 {
        1
    } else {
        ctx.month_id + 1
    };
 // `LEFT JOIN sho`: `monthID`/`yearID`/`linkID` are pinned to the
 // context, so the live join key is `(hourDayID, ageID, sourceTypeID)`.
    let mut sho_lookup: HashMap<(i16, i16, SourceTypeId), f64> = HashMap::new();
    for row in inputs.sho {
        if row.link_id == ctx.link_id && row.month_id == next_month && row.year_id == ctx.year_id {
            sho_lookup.insert((row.hour_day_id, row.age_id, row.source_type_id), row.sho);
        }
    }

 // `GROUP BY (hourDayID, sourceTypeID)`: accumulate
 // `COALESCE(SUM(SHO), 0)` and `SUM(sourceHours)` across the ages.
 // BTreeMap keeps the output in the Java `ORDER BY` order.
    let mut groups: BTreeMap<(i16, SourceTypeId), (f64, f64)> = BTreeMap::new();
    for sh in inputs.source_hours {
        if sh.link_id == ctx.link_id
            && sh.month_id == next_month
            && sh.year_id == ctx.year_id
            && sh.source_hours > 0.0
        {
            let sho = sho_lookup
                .get(&(sh.hour_day_id, sh.age_id, sh.source_type_id))
                .copied()
                .unwrap_or(0.0);
            let cell = groups
                .entry((sh.hour_day_id, sh.source_type_id))
                .or_insert((0.0, 0.0));
            cell.0 += sho;
            cell.1 += sh.source_hours;
        }
    }

    groups
        .into_iter()
        .map(
            |((hour_day_id, source_type_id), (sum_sho, sum_source_hours))| FractionOfOperatingRow {
                hour_day_id,
                source_type_id,
                fraction_of_operating: (sum_sho / sum_source_hours).min(1.0),
            },
        )
        .collect()
}

/// Collect the `polProcessID`s whose process is `process`, from
/// `PollutantProcessAssoc` — Java step 2's
/// `INNER JOIN PollutantProcessAssoc ppa … WHERE ppa.processID = …`.
fn polprocs_for_process(
    pollutant_process_assoc: &[PollutantProcessAssociation],
    process: ProcessId,
) -> HashSet<PolProcessId> {
    pollutant_process_assoc
        .iter()
        .filter(|ppa| ppa.process_id == process)
        .map(|ppa| ppa.polproc_id())
        .collect()
}

/// Port of Java `calculateOpModeDistribution` — build the
/// `OpModeDistribution` rows for one `(process, link, zone, month, year)`
/// loop iteration.
///
/// Runs the three `@step 010` SQL stages (see the module docs):
///
/// 1. [`fraction_of_operating`] — the operating share per `(hourDay,
/// sourceType)`.
/// 2. **Non-operating modes** — for every `SoakActivityFraction` row in
/// the loop's `(month, zone)` whose `(sourceType, hourDay)` has a
/// `FractionOfOperating` entry, emit one row per
/// `OpModePolProcAssoc` association of its op mode whose `polProcessID`
/// belongs to the loop's process, with
/// `opModeFraction = soakActivityFraction * (1 - fractionOfOperating)`.
/// 3. **Operating mode 300** — for each `(sourceType, hourDay,
/// polProcess)` group produced by step 2, append an
/// `OPERATING_OP_MODE` row with
/// `opModeFraction = greatest(0, 1 - SUM(opModeFraction))`.
///
/// The returned rows are the contents of the Java `OpModeDistributionTemp`
/// table — the generator's *candidate* `OpModeDistribution` rows. They
/// carry distinct primary keys (steps 2 and 3 cannot collide — step 2
/// never emits `OPERATING_OP_MODE` — and the `SoakActivityFraction` /
/// `OpModePolProcAssoc` unique indexes keep step 2's own keys distinct),
/// and are sorted by `RowKey`. The final MySQL `INSERT IGNORE` into the
/// live `OpModeDistribution` — which lets an existing user-input
/// (`isUserInput = 'Y'`) row win — is a data-plane step performed by
/// [`Generator::execute`] once lands.
#[must_use]
pub fn op_mode_distribution(
    ctx: &EvapOpModeContext,
    inputs: &EvapOpModeInputs<'_>,
) -> Vec<OpModeDistributionRow> {
 // @step 010, stage 1: FractionOfOperating, indexed for the join.
    let fractions: HashMap<(i16, SourceTypeId), f64> = fraction_of_operating(ctx, inputs)
        .into_iter()
        .map(|r| ((r.hour_day_id, r.source_type_id), r.fraction_of_operating))
        .collect();

 // The `polProcessID`s the `PollutantProcessAssoc` join admits.
    let relevant_polprocs = polprocs_for_process(inputs.pollutant_process_assoc, ctx.process_id);

 // @step 010, stage 2: the non-operating (soak) modes.
 // opModeFraction = soakActivityFraction * (1 - fractionOfOperating).
 //
 // TankTemperatureGenerator writes SoakActivityFraction for the NEXT month
 // (month + 1, wrapping December → January) relative to its loop context:
 // a vehicle's soak activity from month M carries forward into month M+1.
 // The Java EvapOpModeGen's SQL therefore joins on
 // `saf.monthID = ##context.monthID## + 1`, and the canonical snapshots
 // confirm this — SAF rows for a month=7 run carry month_id=8.
    let next_month = if ctx.month_id == 12 {
        1
    } else {
        ctx.month_id + 1
    };
    let mut rows: Vec<OpModeDistributionRow> = Vec::new();
    for saf in inputs.soak_activity_fraction {
        if saf.month_id != next_month || saf.zone_id != ctx.zone_id {
            continue;
        }
 // INNER JOIN FractionOfOperating on (sourceType, hourDay).
        let Some(&fraction) = fractions.get(&(saf.hour_day_id, saf.source_type_id)) else {
            continue;
        };
        let op_mode_fraction = saf.soak_activity_fraction * (1.0 - fraction);
 // INNER JOIN OpModePolProcAssoc on opModeID, then the process filter.
        for omppa in inputs.op_mode_pol_proc_assoc {
            if omppa.op_mode_id == saf.op_mode_id
                && relevant_polprocs.contains(&omppa.pol_process_id)
            {
                rows.push(OpModeDistributionRow {
                    source_type_id: saf.source_type_id,
                    hour_day_id: saf.hour_day_id,
                    link_id: ctx.link_id,
                    pol_process_id: omppa.pol_process_id,
                    op_mode_id: omppa.op_mode_id,
                    op_mode_fraction,
                });
            }
        }
    }

 // @step 010, stage 3: operating mode 300 takes whatever fraction the
 // non-operating modes left, floored at 0. Group the stage-2 rows by
 // (sourceType, hourDay, polProcess) — linkID is constant in context.
    let mut operating: BTreeMap<(SourceTypeId, i16, PolProcessId), f64> = BTreeMap::new();
    for row in &rows {
 *operating
            .entry((row.source_type_id, row.hour_day_id, row.pol_process_id))
            .or_insert(0.0) += row.op_mode_fraction;
    }
    for ((source_type_id, hour_day_id, pol_process_id), non_operating_sum) in operating {
        rows.push(OpModeDistributionRow {
            source_type_id,
            hour_day_id,
            link_id: ctx.link_id,
            pol_process_id,
            op_mode_id: OPERATING_OP_MODE,
            op_mode_fraction: (1.0 - non_operating_sum).max(0.0),
        });
    }

    rows.sort_unstable_by_key(OpModeDistributionRow::key);
    rows
}

fn row_err(table: &'static str, row: usize, column: &'static str, msg: String) -> Error {
    Error::RowExtraction {
        table: table.into(),
        row,
        column: column.into(),
        message: msg,
    }
}

/// Wrapper for `PollutantProcessAssociation` so we can implement `TableRow`
/// (orphan rule prevents implementing a foreign trait for a foreign type).
struct EvapPollutantProcessAssocRow {
    pollutant_id: u16,
    process_id: u16,
}

impl TableRow for EvapPollutantProcessAssocRow {
    fn table_name() -> &'static str {
        "PollutantProcessAssoc"
    }
    fn polars_schema() -> polars::prelude::Schema {
        polars::prelude::Schema::from_iter([
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
                    rows.iter()
                        .map(|r| r.pollutant_id as i32)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "processID".into(),
                    rows.iter()
                        .map(|r| r.process_id as i32)
                        .collect::<Vec<i32>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "PollutantProcessAssoc";
        let pollutant_id_col = df
            .column("pollutantID")
            .map_err(|e| row_err(t, 0, "pollutantID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "pollutantID", e.to_string()))?;
        let process_id_col = df
            .column("processID")
            .map_err(|e| row_err(t, 0, "processID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "processID", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(EvapPollutantProcessAssocRow {
                    pollutant_id: pollutant_id_col.get(i).ok_or_else(|| null("pollutantID"))?
                        as u16,
                    process_id: process_id_col.get(i).ok_or_else(|| null("processID"))? as u16,
                })
            })
            .collect()
    }
}

impl TableRow for SourceHoursRow {
    fn table_name() -> &'static str {
        "sourceHours"
    }
    fn polars_schema() -> polars::prelude::Schema {
        polars::prelude::Schema::from_iter([
            ("hourDayID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("ageID".into(), DataType::Int32),
            ("linkID".into(), DataType::Int32),
            ("monthID".into(), DataType::Int32),
            ("yearID".into(), DataType::Int32),
            ("sourceHours".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "hourDayID".into(),
                    rows.iter()
                        .map(|r| r.hour_day_id as i32)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter()
                        .map(|r| r.source_type_id.0 as i32)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "ageID".into(),
                    rows.iter().map(|r| r.age_id as i32).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "linkID".into(),
                    rows.iter().map(|r| r.link_id as i32).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "monthID".into(),
                    rows.iter().map(|r| r.month_id as i32).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "yearID".into(),
                    rows.iter().map(|r| r.year_id as i32).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceHours".into(),
                    rows.iter().map(|r| r.source_hours).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "sourceHours";
        let hour_day_id_col = df
            .column("hourDayID")
            .map_err(|e| row_err(t, 0, "hourDayID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "hourDayID", e.to_string()))?;
        let source_type_id_col = df
            .column("sourceTypeID")
            .map_err(|e| row_err(t, 0, "sourceTypeID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "sourceTypeID", e.to_string()))?;
        let age_id_col = df
            .column("ageID")
            .map_err(|e| row_err(t, 0, "ageID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "ageID", e.to_string()))?;
        let link_id_col = df
            .column("linkID")
            .map_err(|e| row_err(t, 0, "linkID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "linkID", e.to_string()))?;
        let month_id_col = df
            .column("monthID")
            .map_err(|e| row_err(t, 0, "monthID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "monthID", e.to_string()))?;
        let year_id_col = df
            .column("yearID")
            .map_err(|e| row_err(t, 0, "yearID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "yearID", e.to_string()))?;
        let source_hours_col = df
            .column("sourceHours")
            .map_err(|e| row_err(t, 0, "sourceHours", e.to_string()))?
            .f64()
            .map_err(|e| row_err(t, 0, "sourceHours", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(SourceHoursRow {
                    hour_day_id: hour_day_id_col.get(i).ok_or_else(|| null("hourDayID"))? as i16,
                    source_type_id: SourceTypeId(
                        source_type_id_col
                            .get(i)
                            .ok_or_else(|| null("sourceTypeID"))? as u16,
                    ),
                    age_id: age_id_col.get(i).ok_or_else(|| null("ageID"))? as i16,
                    link_id: link_id_col.get(i).ok_or_else(|| null("linkID"))? as u32,
                    month_id: month_id_col.get(i).ok_or_else(|| null("monthID"))? as u8,
                    year_id: year_id_col.get(i).ok_or_else(|| null("yearID"))? as u16,
                    source_hours: source_hours_col.get(i).ok_or_else(|| null("sourceHours"))?,
                })
            })
            .collect()
    }
}

impl TableRow for ShoRow {
    fn table_name() -> &'static str {
        "sho"
    }
    fn polars_schema() -> polars::prelude::Schema {
        polars::prelude::Schema::from_iter([
            ("hourDayID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("ageID".into(), DataType::Int32),
            ("linkID".into(), DataType::Int32),
            ("monthID".into(), DataType::Int32),
            ("yearID".into(), DataType::Int32),
            ("SHO".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "hourDayID".into(),
                    rows.iter()
                        .map(|r| r.hour_day_id as i32)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter()
                        .map(|r| r.source_type_id.0 as i32)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "ageID".into(),
                    rows.iter().map(|r| r.age_id as i32).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "linkID".into(),
                    rows.iter().map(|r| r.link_id as i32).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "monthID".into(),
                    rows.iter().map(|r| r.month_id as i32).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "yearID".into(),
                    rows.iter().map(|r| r.year_id as i32).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "SHO".into(),
                    rows.iter().map(|r| r.sho).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "sho";
        let hour_day_id_col = df
            .column("hourDayID")
            .map_err(|e| row_err(t, 0, "hourDayID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "hourDayID", e.to_string()))?;
        let source_type_id_col = df
            .column("sourceTypeID")
            .map_err(|e| row_err(t, 0, "sourceTypeID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "sourceTypeID", e.to_string()))?;
        let age_id_col = df
            .column("ageID")
            .map_err(|e| row_err(t, 0, "ageID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "ageID", e.to_string()))?;
        let link_id_col = df
            .column("linkID")
            .map_err(|e| row_err(t, 0, "linkID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "linkID", e.to_string()))?;
        let month_id_col = df
            .column("monthID")
            .map_err(|e| row_err(t, 0, "monthID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "monthID", e.to_string()))?;
        let year_id_col = df
            .column("yearID")
            .map_err(|e| row_err(t, 0, "yearID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "yearID", e.to_string()))?;
        let sho_col = df
            .column("SHO")
            .map_err(|e| row_err(t, 0, "SHO", e.to_string()))?
            .f64()
            .map_err(|e| row_err(t, 0, "SHO", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(ShoRow {
                    hour_day_id: hour_day_id_col.get(i).ok_or_else(|| null("hourDayID"))? as i16,
                    source_type_id: SourceTypeId(
                        source_type_id_col
                            .get(i)
                            .ok_or_else(|| null("sourceTypeID"))? as u16,
                    ),
                    age_id: age_id_col.get(i).ok_or_else(|| null("ageID"))? as i16,
                    link_id: link_id_col.get(i).ok_or_else(|| null("linkID"))? as u32,
                    month_id: month_id_col.get(i).ok_or_else(|| null("monthID"))? as u8,
                    year_id: year_id_col.get(i).ok_or_else(|| null("yearID"))? as u16,
                    sho: sho_col.get(i).ok_or_else(|| null("SHO"))?,
                })
            })
            .collect()
    }
}

impl TableRow for SoakActivityFractionRow {
    fn table_name() -> &'static str {
        "SoakActivityFraction"
    }
    fn polars_schema() -> polars::prelude::Schema {
        polars::prelude::Schema::from_iter([
            ("sourceTypeID".into(), DataType::Int32),
            ("zoneID".into(), DataType::Int32),
            ("monthID".into(), DataType::Int32),
            ("hourDayID".into(), DataType::Int32),
            ("opModeID".into(), DataType::Int32),
            ("soakActivityFraction".into(), DataType::Float64),
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
                    "zoneID".into(),
                    rows.iter().map(|r| r.zone_id as i32).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "monthID".into(),
                    rows.iter().map(|r| r.month_id as i32).collect::<Vec<i32>>(),
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
                    "opModeID".into(),
                    rows.iter()
                        .map(|r| r.op_mode_id as i32)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "soakActivityFraction".into(),
                    rows.iter()
                        .map(|r| r.soak_activity_fraction)
                        .collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "SoakActivityFraction";
        let source_type_id_col = df
            .column("sourceTypeID")
            .map_err(|e| row_err(t, 0, "sourceTypeID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "sourceTypeID", e.to_string()))?;
        let zone_id_col = df
            .column("zoneID")
            .map_err(|e| row_err(t, 0, "zoneID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "zoneID", e.to_string()))?;
        let month_id_col = df
            .column("monthID")
            .map_err(|e| row_err(t, 0, "monthID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "monthID", e.to_string()))?;
        let hour_day_id_col = df
            .column("hourDayID")
            .map_err(|e| row_err(t, 0, "hourDayID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "hourDayID", e.to_string()))?;
        let op_mode_id_col = df
            .column("opModeID")
            .map_err(|e| row_err(t, 0, "opModeID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "opModeID", e.to_string()))?;
        let soak_activity_fraction_col = df
            .column("soakActivityFraction")
            .map_err(|e| row_err(t, 0, "soakActivityFraction", e.to_string()))?
            .f64()
            .map_err(|e| row_err(t, 0, "soakActivityFraction", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(SoakActivityFractionRow {
                    source_type_id: SourceTypeId(
                        source_type_id_col
                            .get(i)
                            .ok_or_else(|| null("sourceTypeID"))? as u16,
                    ),
                    zone_id: zone_id_col.get(i).ok_or_else(|| null("zoneID"))? as u32,
                    month_id: month_id_col.get(i).ok_or_else(|| null("monthID"))? as u8,
                    hour_day_id: hour_day_id_col.get(i).ok_or_else(|| null("hourDayID"))? as i16,
                    op_mode_id: op_mode_id_col.get(i).ok_or_else(|| null("opModeID"))? as i16,
                    soak_activity_fraction: soak_activity_fraction_col
                        .get(i)
                        .ok_or_else(|| null("soakActivityFraction"))?,
                })
            })
            .collect()
    }
}

impl TableRow for OpModePolProcAssoc {
    fn table_name() -> &'static str {
        "OpModePolProcAssoc"
    }
    fn polars_schema() -> polars::prelude::Schema {
        polars::prelude::Schema::from_iter([
            ("polProcessID".into(), DataType::Int32),
            ("opModeID".into(), DataType::Int32),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
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
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "OpModePolProcAssoc";
        let pol_process_id_col = df
            .column("polProcessID")
            .map_err(|e| row_err(t, 0, "polProcessID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "polProcessID", e.to_string()))?;
        let op_mode_id_col = df
            .column("opModeID")
            .map_err(|e| row_err(t, 0, "opModeID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "opModeID", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(OpModePolProcAssoc {
                    pol_process_id: PolProcessId(
                        pol_process_id_col
                            .get(i)
                            .ok_or_else(|| null("polProcessID"))? as u32,
                    ),
                    op_mode_id: op_mode_id_col.get(i).ok_or_else(|| null("opModeID"))? as i16,
                })
            })
            .collect()
    }
}

impl TableRow for OpModeDistributionRow {
    fn table_name() -> &'static str {
        "OpModeDistribution"
    }
    fn polars_schema() -> polars::prelude::Schema {
        polars::prelude::Schema::from_iter([
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
                    rows.iter()
                        .map(|r| r.source_type_id.0 as i32)
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
                    "linkID".into(),
                    rows.iter().map(|r| r.link_id as i32).collect::<Vec<i32>>(),
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
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "OpModeDistribution";
        let source_type_id_col = df
            .column("sourceTypeID")
            .map_err(|e| row_err(t, 0, "sourceTypeID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "sourceTypeID", e.to_string()))?;
        let hour_day_id_col = df
            .column("hourDayID")
            .map_err(|e| row_err(t, 0, "hourDayID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "hourDayID", e.to_string()))?;
        let link_id_col = df
            .column("linkID")
            .map_err(|e| row_err(t, 0, "linkID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "linkID", e.to_string()))?;
        let pol_process_id_col = df
            .column("polProcessID")
            .map_err(|e| row_err(t, 0, "polProcessID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "polProcessID", e.to_string()))?;
        let op_mode_id_col = df
            .column("opModeID")
            .map_err(|e| row_err(t, 0, "opModeID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "opModeID", e.to_string()))?;
        let op_mode_fraction_col = df
            .column("opModeFraction")
            .map_err(|e| row_err(t, 0, "opModeFraction", e.to_string()))?
            .f64()
            .map_err(|e| row_err(t, 0, "opModeFraction", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(OpModeDistributionRow {
                    source_type_id: SourceTypeId(
                        source_type_id_col
                            .get(i)
                            .ok_or_else(|| null("sourceTypeID"))? as u16,
                    ),
                    hour_day_id: hour_day_id_col.get(i).ok_or_else(|| null("hourDayID"))? as i16,
                    link_id: link_id_col.get(i).ok_or_else(|| null("linkID"))? as u32,
                    pol_process_id: PolProcessId(
                        pol_process_id_col
                            .get(i)
                            .ok_or_else(|| null("polProcessID"))? as u32,
                    ),
                    op_mode_id: op_mode_id_col.get(i).ok_or_else(|| null("opModeID"))? as i16,
                    op_mode_fraction: op_mode_fraction_col
                        .get(i)
                        .ok_or_else(|| null("opModeFraction"))?,
                })
            })
            .collect()
    }
}

/// `OpModeDistribution` generator for the evaporative-emission processes.
///
/// Ports `EvaporativeEmissionsOperatingModeDistributionGenerator.java`; see
/// the module documentation for the scope of the port.
#[derive(Debug, Clone)]
pub struct EvaporativeEmissionsOperatingModeDistributionGenerator {
 /// The master-loop subscriptions, built once in [`Self::new`] — one
 /// per evaporative process that resolves against the default DB.
    subscriptions: Vec<CalculatorSubscription>,
}

impl EvaporativeEmissionsOperatingModeDistributionGenerator {
 /// Chain-DAG name — matches the Java class name.
    pub const NAME: &'static str = "EvaporativeEmissionsOperatingModeDistributionGenerator";

 /// Construct the generator with its master-loop subscriptions.
 ///
 /// Mirrors `subscribeToMe`: it walks `EVAP_PROCESS_NAMES`, resolves
 /// each through `EmissionProcess::find_by_name`, and subscribes to
 /// every name that resolves — at `MONTH` granularity, `GENERATOR`
 /// priority. "Evap Non-Fuel Vapors" does not resolve against the
 /// MOVES5 default database, so the result is three subscriptions
 /// (processes 11, 12, 13).
    #[must_use]
    pub fn new() -> Self {
        let priority =
            Priority::parse("GENERATOR").expect("\"GENERATOR\" is a valid MasterLoop priority");
        let subscriptions = EVAP_PROCESS_NAMES
            .iter()
            .filter_map(|&name| EmissionProcess::find_by_name(name))
            .map(|process| CalculatorSubscription::new(process.id, Granularity::Month, priority))
            .collect();
        Self { subscriptions }
    }
}

impl Default for EvaporativeEmissionsOperatingModeDistributionGenerator {
    fn default() -> Self {
        Self::new()
    }
}

/// Default-/scratch-database tables Java `calculateOpModeDistribution`
/// reads. Names use the casing of the generator's own Java SQL; the
/// registry maps them onto Parquet snapshots.
static INPUT_TABLES: &[&str] = &[
    "sourceHours",
    "sho",
    "SoakActivityFraction",
    "OpModePolProcAssoc",
    "PollutantProcessAssoc",
];

/// Scratch-namespace table this generator writes.
static OUTPUT_TABLES: &[&str] = &["OpModeDistribution"];

/// Upstream generators: `TotalActivityGenerator` produces `SHO` /
/// `sourceHours`; `TankTemperatureGenerator` produces `SoakActivityFraction`.
static UPSTREAM: &[&str] = &["TotalActivityGenerator", "TankTemperatureGenerator"];

impl Generator for EvaporativeEmissionsOperatingModeDistributionGenerator {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn subscriptions(&self) -> &[CalculatorSubscription] {
        &self.subscriptions
    }

    fn upstream(&self) -> &[&'static str] {
        UPSTREAM
    }

    fn input_tables(&self) -> &[&'static str] {
        INPUT_TABLES
    }

    fn output_tables(&self) -> &[&'static str] {
        OUTPUT_TABLES
    }

    fn execute(&self, ctx: &mut CalculatorContext) -> Result<CalculatorOutput, Error> {
        let pos = ctx.position();
        let context = EvapOpModeContext {
            process_id: pos
                .process_id
                .ok_or_else(|| Error::Polars("no process_id in iteration position".into()))?,
            link_id: pos
                .location
                .link_id
                .ok_or_else(|| Error::Polars("no link_id in iteration position".into()))?,
            zone_id: pos
                .location
                .zone_id
                .ok_or_else(|| Error::Polars("no zone_id in iteration position".into()))?,
            month_id: pos
                .time
                .month
                .ok_or_else(|| Error::Polars("no month in iteration position".into()))?,
            year_id: pos
                .time
                .year
                .ok_or_else(|| Error::Polars("no year in iteration position".into()))?,
        };
        let source_hours: Vec<SourceHoursRow> = ctx.tables().iter_typed("sourceHours")?;
        let sho: Vec<ShoRow> = ctx.tables().iter_typed("sho")?;
        let soak_activity_fraction: Vec<SoakActivityFractionRow> =
            ctx.tables().iter_typed("SoakActivityFraction")?;
        let op_mode_pol_proc_assoc: Vec<OpModePolProcAssoc> =
            ctx.tables().iter_typed("OpModePolProcAssoc")?;
        let ppa_raw: Vec<EvapPollutantProcessAssocRow> =
            ctx.tables().iter_typed("PollutantProcessAssoc")?;
        let pollutant_process_assoc: Vec<PollutantProcessAssociation> = ppa_raw
            .into_iter()
            .map(|r| PollutantProcessAssociation {
                pollutant_id: PollutantId(r.pollutant_id),
                process_id: ProcessId(r.process_id),
            })
            .collect();
        let inputs = EvapOpModeInputs {
            source_hours: &source_hours,
            sho: &sho,
            soak_activity_fraction: &soak_activity_fraction,
            op_mode_pol_proc_assoc: &op_mode_pol_proc_assoc,
            pollutant_process_assoc: &pollutant_process_assoc,
        };
        let rows = op_mode_distribution(&context, &inputs);
 // Write to the slow store (not scratch) so that downstream calculators
 // can find the table via ctx.tables(), which they already use for all
 // other input tables. The slow store is per-chunk after Arc::make_mut
 // clones it, so this write is visible only within this chunk.
        let df = OpModeDistributionRow::into_dataframe(rows)
            .map_err(|e| Error::Polars(e.to_string()))?;
        ctx.tables_mut().insert(OUTPUT_TABLES[0], df);
        Ok(CalculatorOutput::empty())
    }
}

/// Generator factory — returns a boxed instance for registration with the
/// `CalculatorRegistry`.
#[must_use]
pub fn factory() -> Box<dyn moves_framework::Generator> {
    Box::new(EvaporativeEmissionsOperatingModeDistributionGenerator::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use moves_data::PollutantId;

 /// The fixed loop context the table helpers below populate against.
    const LINK: u32 = 101;
    const ZONE: u32 = 90_001;
    const MONTH: u8 = 7;
    const YEAR: u16 = 2020;

 /// Loop context for `process`, at the fixed `(link, zone, month, year)`.
    fn ctx(process: u16) -> EvapOpModeContext {
        EvapOpModeContext {
            process_id: ProcessId(process),
            link_id: LINK,
            zone_id: ZONE,
            month_id: MONTH,
            year_id: YEAR,
        }
    }

 /// `sourceHours` row at the fixed `(link, month+1, year)`.
 ///
 /// Activity generators write sourceHours for the NEXT month (MONTH+1),
 /// so `fraction_of_operating` filters on `month_id = ctx.month_id + 1`.
    fn sh(hour_day: i16, source_type: u16, age: i16, source_hours: f64) -> SourceHoursRow {
        SourceHoursRow {
            hour_day_id: hour_day,
            source_type_id: SourceTypeId(source_type),
            age_id: age,
            link_id: LINK,
            month_id: MONTH + 1,
            year_id: YEAR,
            source_hours,
        }
    }

 /// `SHO` row at the fixed `(link, month+1, year)`.
 ///
 /// Activity generators write SHO for the NEXT month (MONTH+1),
 /// so `fraction_of_operating` filters on `month_id = ctx.month_id + 1`.
    fn sho(hour_day: i16, source_type: u16, age: i16, sho: f64) -> ShoRow {
        ShoRow {
            hour_day_id: hour_day,
            source_type_id: SourceTypeId(source_type),
            age_id: age,
            link_id: LINK,
            month_id: MONTH + 1,
            year_id: YEAR,
            sho,
        }
    }

 /// `SoakActivityFraction` row at the fixed `(zone, month+1)`.
 ///
 /// `TankTemperatureGenerator` writes SAF for the NEXT month (MONTH+1),
 /// so `op_mode_distribution` joins on `saf.monthID = ctx.month_id + 1`.
    fn saf(
        source_type: u16,
        hour_day: i16,
        op_mode: i16,
        fraction: f64,
    ) -> SoakActivityFractionRow {
        SoakActivityFractionRow {
            source_type_id: SourceTypeId(source_type),
            zone_id: ZONE,
            month_id: MONTH + 1,
            hour_day_id: hour_day,
            op_mode_id: op_mode,
            soak_activity_fraction: fraction,
        }
    }

 /// `polProcessID` for a `(pollutant, process)` pair.
    fn polproc(pollutant: u16, process: u16) -> PolProcessId {
        PolProcessId::new(PollutantId(pollutant), ProcessId(process))
    }

 /// `OpModePolProcAssoc` row helper.
    fn omppa(op_mode: i16, pol_process: PolProcessId) -> OpModePolProcAssoc {
        OpModePolProcAssoc {
            pol_process_id: pol_process,
            op_mode_id: op_mode,
        }
    }

 /// `PollutantProcessAssoc` row helper.
    fn ppa(pollutant: u16, process: u16) -> PollutantProcessAssociation {
        PollutantProcessAssociation {
            pollutant_id: PollutantId(pollutant),
            process_id: ProcessId(process),
        }
    }

 /// Empty `EvapOpModeInputs` — tests override the fields they exercise.
    fn empty_inputs<'a>() -> EvapOpModeInputs<'a> {
        EvapOpModeInputs {
            source_hours: &[],
            sho: &[],
            soak_activity_fraction: &[],
            op_mode_pol_proc_assoc: &[],
            pollutant_process_assoc: &[],
        }
    }

    #[test]
    fn fraction_of_operating_is_sho_over_source_hours() {
 // One cell, one age: 25 operating hours of 100 total -> 0.25.
        let source_hours = [sh(51, 21, 0, 100.0)];
        let sho_rows = [sho(51, 21, 0, 25.0)];
        let inputs = EvapOpModeInputs {
            source_hours: &source_hours,
            sho: &sho_rows,
            ..empty_inputs()
        };
        let fo = fraction_of_operating(&ctx(11), &inputs);
        assert_eq!(fo.len(), 1);
        assert_eq!(fo[0].hour_day_id, 51);
        assert_eq!(fo[0].source_type_id, SourceTypeId(21));
        assert!((fo[0].fraction_of_operating - 0.25).abs() < 1e-12);
    }

    #[test]
    fn fraction_of_operating_sums_across_ages() {
 // SUM(SHO)/SUM(sourceHours) = (4+6)/(10+30) = 0.25, summed over age.
        let source_hours = [sh(51, 21, 3, 10.0), sh(51, 21, 4, 30.0)];
        let sho_rows = [sho(51, 21, 3, 4.0), sho(51, 21, 4, 6.0)];
        let inputs = EvapOpModeInputs {
            source_hours: &source_hours,
            sho: &sho_rows,
            ..empty_inputs()
        };
        let fo = fraction_of_operating(&ctx(11), &inputs);
        assert_eq!(fo.len(), 1);
        assert!((fo[0].fraction_of_operating - 0.25).abs() < 1e-12);
    }

    #[test]
    fn fraction_of_operating_left_join_miss_contributes_zero_sho() {
 // age 4 has source-hours but no matching SHO row: COALESCE -> 0,
 // so the cell's operating share is (4+0)/(10+30) = 0.1.
        let source_hours = [sh(51, 21, 3, 10.0), sh(51, 21, 4, 30.0)];
        let sho_rows = [sho(51, 21, 3, 4.0)];
        let inputs = EvapOpModeInputs {
            source_hours: &source_hours,
            sho: &sho_rows,
            ..empty_inputs()
        };
        let fo = fraction_of_operating(&ctx(11), &inputs);
        assert!((fo[0].fraction_of_operating - 0.1).abs() < 1e-12);
    }

    #[test]
    fn fraction_of_operating_clamps_to_one() {
 // More operating hours than total source-hours: least(1, …) caps it.
        let source_hours = [sh(51, 21, 0, 10.0)];
        let sho_rows = [sho(51, 21, 0, 999.0)];
        let inputs = EvapOpModeInputs {
            source_hours: &source_hours,
            sho: &sho_rows,
            ..empty_inputs()
        };
        let fo = fraction_of_operating(&ctx(11), &inputs);
        assert_eq!(fo[0].fraction_of_operating, 1.0);
    }

    #[test]
    fn fraction_of_operating_excludes_non_positive_source_hours() {
 // `WHERE sourceHours > 0` drops the zero-hours row; only the 20.0
 // row survives -> 5/20 = 0.25.
        let source_hours = [sh(51, 21, 0, 0.0), sh(51, 21, 1, 20.0)];
        let sho_rows = [sho(51, 21, 0, 7.0), sho(51, 21, 1, 5.0)];
        let inputs = EvapOpModeInputs {
            source_hours: &source_hours,
            sho: &sho_rows,
            ..empty_inputs()
        };
        let fo = fraction_of_operating(&ctx(11), &inputs);
        assert_eq!(fo.len(), 1);
        assert!((fo[0].fraction_of_operating - 0.25).abs() < 1e-12);
    }

    #[test]
    fn fraction_of_operating_filters_by_context() {
 // A sourceHours row on another link, and an SHO row in another
 // month, are both ignored: only the in-context pair counts.
        let source_hours = [
            sh(51, 21, 0, 100.0),
            SourceHoursRow {
                link_id: 999,
                ..sh(51, 21, 0, 100.0)
            },
        ];
        let sho_rows = [
            sho(51, 21, 0, 40.0),
            ShoRow {
                month_id: 1,
                ..sho(51, 21, 0, 40.0)
            },
        ];
        let inputs = EvapOpModeInputs {
            source_hours: &source_hours,
            sho: &sho_rows,
            ..empty_inputs()
        };
        let fo = fraction_of_operating(&ctx(11), &inputs);
 // In-context sourceHours = 100, in-context SHO = 40 -> 0.4.
        assert_eq!(fo.len(), 1);
        assert!((fo[0].fraction_of_operating - 0.4).abs() < 1e-12);
    }

    #[test]
    fn fraction_of_operating_orders_by_hour_day_then_source_type() {
 // Inputs deliberately out of order; output follows the SQL ORDER BY.
        let source_hours = [sh(52, 21, 0, 1.0), sh(51, 30, 0, 1.0), sh(51, 21, 0, 1.0)];
        let inputs = EvapOpModeInputs {
            source_hours: &source_hours,
            ..empty_inputs()
        };
        let fo = fraction_of_operating(&ctx(11), &inputs);
        let keys: Vec<(i16, SourceTypeId)> = fo
            .iter()
            .map(|r| (r.hour_day_id, r.source_type_id))
            .collect();
        assert_eq!(
            keys,
            vec![
                (51, SourceTypeId(21)),
                (51, SourceTypeId(30)),
                (52, SourceTypeId(21)),
            ],
        );
    }

    #[test]
    fn op_mode_distribution_splits_operating_and_soak() {
 // fractionOfOperating(51,21) = 25/100 = 0.25.
 // Soak op mode 151 fraction 0.6 -> opModeFraction = 0.6 * 0.75 = 0.45.
 // Operating mode 300 -> 1 - 0.45 = 0.55.
        let source_hours = [sh(51, 21, 0, 100.0)];
        let sho_rows = [sho(51, 21, 0, 25.0)];
        let soak = [saf(21, 51, 151, 0.6)];
        let pp = polproc(31, 11);
        let omppa_rows = [omppa(151, pp)];
        let ppa_rows = [ppa(31, 11)];
        let inputs = EvapOpModeInputs {
            source_hours: &source_hours,
            sho: &sho_rows,
            soak_activity_fraction: &soak,
            op_mode_pol_proc_assoc: &omppa_rows,
            pollutant_process_assoc: &ppa_rows,
        };
        let rows = op_mode_distribution(&ctx(11), &inputs);
        assert_eq!(rows.len(), 2);
 // Sorted by primary key: soak op mode 151 precedes operating 300.
        assert_eq!(rows[0].op_mode_id, 151);
        assert!((rows[0].op_mode_fraction - 0.45).abs() < 1e-12);
        assert_eq!(rows[1].op_mode_id, OPERATING_OP_MODE);
        assert!((rows[1].op_mode_fraction - 0.55).abs() < 1e-12);
        for r in &rows {
            assert_eq!(r.source_type_id, SourceTypeId(21));
            assert_eq!(r.hour_day_id, 51);
            assert_eq!(r.link_id, LINK);
            assert_eq!(r.pol_process_id, pp);
        }
    }

    #[test]
    fn op_mode_distribution_operating_mode_is_one_minus_soak_sum() {
 // Two soak modes; fractionOfOperating = 0 (no SHO).
 // opModeFractions: 0.3 and 0.2 -> operating 300 = 1 - 0.5 = 0.5.
        let source_hours = [sh(51, 21, 0, 100.0)];
        let soak = [saf(21, 51, 150, 0.3), saf(21, 51, 151, 0.2)];
        let pp = polproc(31, 12);
        let omppa_rows = [omppa(150, pp), omppa(151, pp)];
        let ppa_rows = [ppa(31, 12)];
        let inputs = EvapOpModeInputs {
            source_hours: &source_hours,
            soak_activity_fraction: &soak,
            op_mode_pol_proc_assoc: &omppa_rows,
            pollutant_process_assoc: &ppa_rows,
            ..empty_inputs()
        };
        let rows = op_mode_distribution(&ctx(12), &inputs);
        let op300 = rows
            .iter()
            .find(|r| r.op_mode_id == OPERATING_OP_MODE)
            .expect("operating-mode row present");
        assert!((op300.op_mode_fraction - 0.5).abs() < 1e-12);
    }

    #[test]
    fn op_mode_distribution_operating_mode_floored_at_zero() {
 // Soak fractions sum past 1 (degenerate input): greatest(0, …)
 // floors the operating-mode fraction at 0 rather than going
 // negative.
        let source_hours = [sh(51, 21, 0, 100.0)];
        let soak = [saf(21, 51, 150, 0.8), saf(21, 51, 151, 0.7)];
        let pp = polproc(31, 13);
        let omppa_rows = [omppa(150, pp), omppa(151, pp)];
        let ppa_rows = [ppa(31, 13)];
        let inputs = EvapOpModeInputs {
            source_hours: &source_hours,
            soak_activity_fraction: &soak,
            op_mode_pol_proc_assoc: &omppa_rows,
            pollutant_process_assoc: &ppa_rows,
            ..empty_inputs()
        };
        let rows = op_mode_distribution(&ctx(13), &inputs);
        let op300 = rows
            .iter()
            .find(|r| r.op_mode_id == OPERATING_OP_MODE)
            .expect("operating-mode row present");
        assert_eq!(op300.op_mode_fraction, 0.0);
    }

    #[test]
    fn op_mode_distribution_filters_by_process() {
 // The op mode's only polProcess is process 12; running for
 // process 11 yields nothing.
        let source_hours = [sh(51, 21, 0, 100.0)];
        let soak = [saf(21, 51, 151, 0.6)];
        let omppa_rows = [omppa(151, polproc(31, 12))];
        let ppa_rows = [ppa(31, 11), ppa(31, 12)];
        let inputs = EvapOpModeInputs {
            source_hours: &source_hours,
            soak_activity_fraction: &soak,
            op_mode_pol_proc_assoc: &omppa_rows,
            pollutant_process_assoc: &ppa_rows,
            ..empty_inputs()
        };
        assert!(op_mode_distribution(&ctx(11), &inputs).is_empty());
 // …but running for process 12 produces the soak + operating rows.
        assert_eq!(op_mode_distribution(&ctx(12), &inputs).len(), 2);
    }

    #[test]
    fn op_mode_distribution_filters_soak_by_zone_and_month() {
 // SoakActivityFraction rows outside the loop's zone or month are
 // dropped by the step-2 WHERE clause.
        let source_hours = [sh(51, 21, 0, 100.0)];
        let soak = [
            SoakActivityFractionRow {
                zone_id: 7,
                ..saf(21, 51, 151, 0.6)
            },
            SoakActivityFractionRow {
                month_id: 1,
                ..saf(21, 51, 152, 0.6)
            },
        ];
        let pp = polproc(31, 11);
        let omppa_rows = [omppa(151, pp), omppa(152, pp)];
        let ppa_rows = [ppa(31, 11)];
        let inputs = EvapOpModeInputs {
            source_hours: &source_hours,
            soak_activity_fraction: &soak,
            op_mode_pol_proc_assoc: &omppa_rows,
            pollutant_process_assoc: &ppa_rows,
            ..empty_inputs()
        };
        assert!(op_mode_distribution(&ctx(11), &inputs).is_empty());
    }

    #[test]
    fn op_mode_distribution_emits_one_row_per_associated_polprocess() {
 // One soak op mode associated with two polProcesses of process 11:
 // step 2 emits a row for each, and step 3 a 300 row for each.
        let source_hours = [sh(51, 21, 0, 100.0)];
        let soak = [saf(21, 51, 151, 0.6)];
        let pp_a = polproc(31, 11);
        let pp_b = polproc(32, 11);
        let omppa_rows = [omppa(151, pp_a), omppa(151, pp_b)];
        let ppa_rows = [ppa(31, 11), ppa(32, 11)];
        let inputs = EvapOpModeInputs {
            source_hours: &source_hours,
            soak_activity_fraction: &soak,
            op_mode_pol_proc_assoc: &omppa_rows,
            pollutant_process_assoc: &ppa_rows,
            ..empty_inputs()
        };
        let rows = op_mode_distribution(&ctx(11), &inputs);
        assert_eq!(rows.len(), 4);
        let polprocs: HashSet<PolProcessId> = rows.iter().map(|r| r.pol_process_id).collect();
        assert_eq!(polprocs, HashSet::from([pp_a, pp_b]));
 // Each polProcess gets exactly one operating-mode row.
        assert_eq!(
            rows.iter()
                .filter(|r| r.op_mode_id == OPERATING_OP_MODE)
                .count(),
            2,
        );
    }

    #[test]
    fn op_mode_distribution_skips_cell_without_fraction_of_operating() {
 // No sourceHours -> no FractionOfOperating entry -> the INNER JOIN
 // drops the SoakActivityFraction row; nothing (not even a 300 row)
 // is emitted.
        let soak = [saf(21, 51, 151, 0.6)];
        let omppa_rows = [omppa(151, polproc(31, 11))];
        let ppa_rows = [ppa(31, 11)];
        let inputs = EvapOpModeInputs {
            soak_activity_fraction: &soak,
            op_mode_pol_proc_assoc: &omppa_rows,
            pollutant_process_assoc: &ppa_rows,
            ..empty_inputs()
        };
        assert!(op_mode_distribution(&ctx(11), &inputs).is_empty());
    }

    #[test]
    fn op_mode_distribution_skips_soak_op_mode_without_association() {
 // A soak op mode with no OpModePolProcAssoc row contributes no
 // non-operating row, and so no operating-mode-300 row either.
        let source_hours = [sh(51, 21, 0, 100.0)];
        let soak = [saf(21, 51, 199, 0.6)];
        let omppa_rows = [omppa(151, polproc(31, 11))];
        let ppa_rows = [ppa(31, 11)];
        let inputs = EvapOpModeInputs {
            source_hours: &source_hours,
            soak_activity_fraction: &soak,
            op_mode_pol_proc_assoc: &omppa_rows,
            pollutant_process_assoc: &ppa_rows,
            ..empty_inputs()
        };
        assert!(op_mode_distribution(&ctx(11), &inputs).is_empty());
    }

    #[test]
    fn op_mode_distribution_output_is_sorted_by_primary_key() {
 // Two source types, two hour/days, two op modes — deliberately
 // unsorted inputs; the output must follow the primary key.
        let source_hours = [sh(52, 30, 0, 100.0), sh(51, 21, 0, 100.0)];
        let soak = [
            saf(30, 52, 152, 0.1),
            saf(21, 51, 151, 0.2),
            saf(21, 51, 150, 0.3),
        ];
        let pp = polproc(31, 11);
        let omppa_rows = [omppa(150, pp), omppa(151, pp), omppa(152, pp)];
        let ppa_rows = [ppa(31, 11)];
        let inputs = EvapOpModeInputs {
            source_hours: &source_hours,
            soak_activity_fraction: &soak,
            op_mode_pol_proc_assoc: &omppa_rows,
            pollutant_process_assoc: &ppa_rows,
            ..empty_inputs()
        };
        let rows = op_mode_distribution(&ctx(11), &inputs);
        let keys: Vec<RowKey> = rows.iter().map(OpModeDistributionRow::key).collect();
        let mut sorted = keys.clone();
        sorted.sort_unstable();
        assert_eq!(keys, sorted);
 // All keys distinct — steps 2 and 3 never collide.
        let unique: HashSet<RowKey> = keys.iter().copied().collect();
        assert_eq!(unique.len(), keys.len());
    }

    #[test]
    fn op_mode_distribution_empty_without_inputs() {
        assert!(op_mode_distribution(&ctx(11), &empty_inputs()).is_empty());
    }

    #[test]
    fn generator_metadata_matches_java_subscribe_to_me() {
        let generator = EvaporativeEmissionsOperatingModeDistributionGenerator::new();
        assert_eq!(
            generator.name(),
            "EvaporativeEmissionsOperatingModeDistributionGenerator",
        );
        assert_eq!(generator.output_tables(), &["OpModeDistribution"]);
        assert_eq!(
            generator.upstream(),
            &["TotalActivityGenerator", "TankTemperatureGenerator"],
        );
        assert!(generator.input_tables().contains(&"SoakActivityFraction"));
        assert!(generator.input_tables().contains(&"sourceHours"));
    }

    #[test]
    fn subscribe_to_me_drops_evap_non_fuel_vapors() {
 // The fourth process name does not resolve against the default DB,
 // so the null-guard leaves exactly three subscriptions: 11, 12, 13,
 // all at MONTH granularity / GENERATOR priority.
        let generator = EvaporativeEmissionsOperatingModeDistributionGenerator::new();
        let subs = generator.subscriptions();
        assert_eq!(subs.len(), 3);
        let processes: Vec<ProcessId> = subs.iter().map(|s| s.process_id).collect();
        assert_eq!(processes, vec![ProcessId(11), ProcessId(12), ProcessId(13)],);
        for s in subs {
            assert_eq!(s.granularity, Granularity::Month);
            assert_eq!(s.priority.display(), "GENERATOR");
        }
    }

    #[test]
    fn execute_writes_op_mode_distribution_to_tables() {
        use moves_framework::{
            DataFrameStore, DataFrameStoreTyped, ExecutionLocation, ExecutionTime, InMemoryStore,
            IterationPosition,
        };

 // Build the five input tables using raw insert to bypass registry
 // schema validation (the registry sho/PollutantProcessAssoc schemas
 // describe partial column sets from other calculators).
        let mut store = InMemoryStore::default();
        store.insert(
            "sourceHours",
            SourceHoursRow::into_dataframe(vec![sh(51, 21, 0, 100.0)]).unwrap(),
        );
        store.insert(
            "sho",
            ShoRow::into_dataframe(vec![sho(51, 21, 0, 25.0)]).unwrap(),
        );
        store.insert(
            "SoakActivityFraction",
            SoakActivityFractionRow::into_dataframe(vec![saf(21, 51, 151, 0.6)]).unwrap(),
        );
        store.insert(
            "OpModePolProcAssoc",
            OpModePolProcAssoc::into_dataframe(vec![omppa(151, polproc(79, 11))]).unwrap(),
        );
        store.insert(
            "PollutantProcessAssoc",
            EvapPollutantProcessAssocRow::into_dataframe(vec![EvapPollutantProcessAssocRow {
                pollutant_id: 79,
                process_id: 11,
            }])
            .unwrap(),
        );

        let position = IterationPosition {
            iteration: 0,
            process_id: Some(ProcessId(11)),
            location: ExecutionLocation::link(1, 1, ZONE, LINK),
            time: ExecutionTime {
                year: Some(YEAR),
                month: Some(MONTH),
                day_id: None,
                hour: None,
            },
        };

        let generator = EvaporativeEmissionsOperatingModeDistributionGenerator::new();
        let mut ctx = CalculatorContext::with_position_and_tables(position, store);
        generator.execute(&mut ctx).unwrap();

        let out: Vec<OpModeDistributionRow> =
            ctx.tables().iter_typed("OpModeDistribution").unwrap();
 // fractionOfOperating = 25/100 = 0.25; soak 151 fraction = 0.6 * 0.75 = 0.45;
 // operating 300 = 1 - 0.45 = 0.55. Expect two rows sorted by opModeID.
        assert_eq!(out.len(), 2, "expected soak + operating rows");
        let soak = out.iter().find(|r| r.op_mode_id == 151).unwrap();
        assert!((soak.op_mode_fraction - 0.45).abs() < 1e-12);
        let operating = out.iter().find(|r| r.op_mode_id == 300).unwrap();
        assert!((operating.op_mode_fraction - 0.55).abs() < 1e-12);
    }

    #[test]
    fn generator_is_object_safe() {
 // The registry stores generators as Box<dyn Generator>.
        let generator: Box<dyn Generator> =
            Box::new(EvaporativeEmissionsOperatingModeDistributionGenerator::new());
        assert_eq!(
            generator.name(),
            "EvaporativeEmissionsOperatingModeDistributionGenerator",
        );
    }
}
