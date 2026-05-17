//! Port of `generators/sourcetypephysics/sourcetypephysics.go` — the
//! `SourceTypePhysics` external-generator step that corrects the rates-mode
//! `RatesOpModeDistribution` operating-mode-distribution table.
//!
//! Migration plan: Phase 3, Task 37.
//!
//! # What this step does
//!
//! MOVES models some `(model-year range, regulatory class)` combinations
//! with a *temporary* source type that stands in for the *real* source type
//! while the running-exhaust operating modes are computed. Once the external
//! drive-cycle generator has produced `RatesOpModeDistribution`, the
//! `SourceTypePhysics` step rewrites that table:
//!
//! * temporary source types are replaced with their real source type;
//! * the normal operating modes (`0..100`) of a temporary source type are
//!   shifted by a per-mapping `opModeIDOffset` into a range unique to that
//!   temporary source type, so a real source type can carry several
//!   model-year-specific operating-mode sets at once;
//! * wildcard placeholders and superseded real-source-type rows are dropped.
//!
//! The `sourceUseTypePhysicsMapping` table drives the rewrite: each record
//! is a `(realSourceTypeID, tempSourceTypeID, opModeIDOffset)` triple,
//! modeled here by [`SourceUseTypePhysicsMappingDetail`].
//!
//! # Scope of this port
//!
//! The pinned Go file is the whole `sourcetypephysics` package: the
//! in-memory mapping load (`setupTables`) and the one row-rewriting pass
//! (`coreUpdateOperatingModeDistribution_RatesOpModeDistribution`). Both are
//! ported here in full — [`SourceUseTypePhysicsMapping::build`] and
//! [`SourceUseTypePhysicsMapping::correct_table`] respectively.
//!
//! The wider `SourceTypePhysics.java` class (1 053 lines) also offsets
//! emission-rate tables and builds the expanded-operating-modes table — the
//! "tractive power / vehicle-specific power" work the migration-plan summary
//! alludes to. None of that is in the pinned Go file, so none of it is in
//! this task; the Go file is exactly the `RatesOpModeDistribution`
//! external-generator fast path and nothing else.
//!
//! The Go read its input from a MariaDB execution database and streamed the
//! result through temporary files `LOAD DATA INFILE`'d back into MariaDB.
//! This port keeps the **computation** — the temp→real source-type swap, the
//! operating-mode offsetting, the row-drop rules, and the `INSERT IGNORE`
//! collision resolution — and replaces the I/O boundary with plain values:
//! [`SourceUseTypePhysicsMappingDetail`] records in,
//! [`OpModeDistributionRow`] values in and out.
//!
//! # `INSERT IGNORE` and row order
//!
//! The Go reads `RatesOpModeDistribution` ordered by its full primary key
//! descending and `LOAD DATA INFILE IGNORE`s the rewritten rows into a fresh
//! copy of the table. Both halves matter together: rule 4 below shifts a
//! normal operating mode by `opModeIDOffset`, which can land it on the
//! primary key of a row that *already* carried an offset operating mode. The
//! descending order writes the already-offset row first, and `INSERT IGNORE`
//! keeps the first-written row — so the already-offset row wins, matching
//! the Java comment that a promoted row "must be ignored" when an extended
//! operating-mode row already exists. [`SourceUseTypePhysicsMapping::correct_table`]
//! reproduces both: it processes rows in descending primary-key order and
//! keeps the first row written for each output primary key.
//!
//! # Data plane (Task 50)
//!
//! [`Generator::execute`] receives a [`CalculatorContext`] whose
//! `ExecutionTables` / `ScratchNamespace` are Phase-2 placeholders until the
//! `DataFrameStore` lands (migration-plan Task 50), so `execute` cannot yet
//! read `sourceUseTypePhysicsMapping` / `RatesOpModeDistribution` nor write
//! the corrected table back. The numerically faithful algorithm is fully
//! ported and unit-tested on [`SourceUseTypePhysicsMapping`]; once the data
//! plane exists, `execute` builds the mapping from `ctx.tables()`, applies
//! [`correct_table`](SourceUseTypePhysicsMapping::correct_table) to the
//! scratch `RatesOpModeDistribution`, and writes the result back.

use std::cmp::Reverse;
use std::collections::{HashMap, HashSet};

use moves_data::SourceTypeId;
use moves_framework::{
    CalculatorContext, CalculatorOutput, CalculatorSubscription, Error, Generator,
};

/// `polProcessID` of brakewear — `SourceTypePhysics` special-cases it (the
/// Go "Change source types for brakewear" rule).
const BRAKEWEAR_POL_PROCESS_ID: i32 = 11609;

/// Running Exhaust process id. `polProcessID` encodes
/// `pollutantID * 100 + processID`, so `polProcessID % 100` recovers the
/// process id; `SourceTypePhysics` only rewrites running-exhaust rows.
const RUNNING_EXHAUST_PROCESS_ID: i32 = 1;

/// Count of "normal" operating modes — operating-mode IDs `0..100` are the
/// regular modes. `opModeIDOffset` shifts a temporary source type's normal
/// modes out of this range.
const NORMAL_OP_MODE_COUNT: i32 = 100;

/// One `sourceUseTypePhysicsMapping` record — the Go
/// `SourceUseTypePhysicsMappingDetail`.
///
/// `real_source_type_id` is the source type traditionally used.
/// `temp_source_type_id` is a temporary source type that stands in for one
/// model-year-range / regulatory-class combination. `op_mode_id_offset`
/// shifts that temporary source type's normal operating modes into a range
/// unique to it, so the real source type can host several at once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceUseTypePhysicsMappingDetail {
    /// `realSourceTypeID`.
    pub real_source_type_id: SourceTypeId,
    /// `tempSourceTypeID`.
    pub temp_source_type_id: SourceTypeId,
    /// `opModeIDOffset`.
    pub op_mode_id_offset: i32,
}

/// One row of the `RatesOpModeDistribution` operating-mode-distribution
/// table, as the `SourceTypePhysics` correction reads and rewrites it.
///
/// Ten columns, matching the Go `select` / `Scan` in
/// `coreUpdateOperatingModeDistribution_RatesOpModeDistribution`.
///
/// Integer columns the correction does not interpret (`road_type_id`,
/// `avg_speed_bin_id`, `hour_day_id`) are carried through as raw `i32` — the
/// type the Go `Scan` reads them into. `source_type_id` is given the typed
/// [`SourceTypeId`] because the correction looks it up in the mapping and
/// rewrites it. `pol_process_id` stays a raw signed `i32`: the table stores
/// negative `polProcessID` values as wildcard placeholders, which the
/// unsigned `PolProcessId` newtype cannot represent.
///
/// `op_mode_fraction_cv` is the post-`COALESCE(opModeFractionCV, 0)` value —
/// the Go query coalesces a `NULL` to `0`.
///
/// `moves_calculators::generators::rates_op_mode_distribution` defines a
/// narrower, eight-column `RatesOpModeDistributionRow` for the rows *that*
/// generator emits; this struct models the full ten-column table row the
/// `SourceTypePhysics` pass reads back. The two are intentionally distinct
/// types and are deliberately not re-exported at the crate root.
///
/// Primary key (`database/CreateExecutionRates.sql`):
/// `(sourceTypeID, polProcessID, roadTypeID, hourDayID, opModeID,
/// avgSpeedBinID)`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OpModeDistributionRow {
    /// `sourceTypeID` — the correction's lookup key; rewritten temp→real.
    pub source_type_id: SourceTypeId,
    /// `roadTypeID` — carried through unchanged.
    pub road_type_id: i32,
    /// `avgSpeedBinID` — carried through unchanged.
    pub avg_speed_bin_id: i32,
    /// `hourDayID` — carried through unchanged.
    pub hour_day_id: i32,
    /// `polProcessID` — `pollutantID * 100 + processID`; raw signed (a
    /// negative value is a wildcard placeholder).
    pub pol_process_id: i32,
    /// `opModeID` — possibly shifted by `opModeIDOffset` (rule 4).
    pub op_mode_id: i32,
    /// `opModeFraction` — carried through unchanged.
    pub op_mode_fraction: f64,
    /// `opModeFractionCV` — carried through unchanged (post-`COALESCE`).
    pub op_mode_fraction_cv: f64,
    /// `avgBinSpeed` — carried through unchanged.
    pub avg_bin_speed: f64,
    /// `avgSpeedFraction` — carried through unchanged.
    pub avg_speed_fraction: f64,
}

/// Primary-key tuple of `RatesOpModeDistribution`, in primary-key order —
/// the columns the Go `ORDER BY` sorts on and the `INSERT IGNORE`
/// de-duplicates on.
type RowKey = (SourceTypeId, i32, i32, i32, i32, i32);

impl OpModeDistributionRow {
    /// The primary-key projection: `(sourceTypeID, polProcessID, roadTypeID,
    /// hourDayID, opModeID, avgSpeedBinID)`.
    fn primary_key(&self) -> RowKey {
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

/// The outcome of applying the `SourceTypePhysics` correction to one
/// `RatesOpModeDistribution` row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowCorrection {
    /// The row is dropped from the corrected table.
    Drop,
    /// The row is kept, with its source type and operating mode possibly
    /// rewritten.
    Keep {
        /// The (possibly temp→real swapped) source type.
        source_type_id: SourceTypeId,
        /// The (possibly offset-shifted) operating mode.
        op_mode_id: i32,
    },
}

/// The in-memory `sourceUseTypePhysicsMapping` — the Go module-level
/// `SourceUseTypePhysicsMapping` slice plus its two by-source-type indexes.
///
/// Ports the in-memory half of the Go `setupTables`; see
/// [`build`](Self::build).
#[derive(Debug, Clone, Default)]
pub struct SourceUseTypePhysicsMapping {
    /// Every mapping record, in build order — the Go exported
    /// `SourceUseTypePhysicsMapping` slice.
    details: Vec<SourceUseTypePhysicsMappingDetail>,
    /// Records keyed by `tempSourceTypeID` (each is unique) — the Go
    /// `SourceUseTypePhysicsMappingByTempSourceType`.
    by_temp_source_type: HashMap<SourceTypeId, SourceUseTypePhysicsMappingDetail>,
    /// Records keyed by `realSourceTypeID` — the Go
    /// `SourceUseTypePhysicsMappingByRealSourceType`. A `realSourceTypeID`
    /// can be shared, so on collision the last record in build order wins
    /// (the Go comment: "ok to overwrite something else").
    by_real_source_type: HashMap<SourceTypeId, SourceUseTypePhysicsMappingDetail>,
}

impl SourceUseTypePhysicsMapping {
    /// Build the mapping from `sourceUseTypePhysicsMapping` records — the
    /// in-memory half of the Go `setupTables`.
    ///
    /// `details` is expected to be the result of the Go query
    /// `select distinct realSourceTypeID, tempSourceTypeID, opModeIDOffset
    /// from sourceUseTypePhysicsMapping where realSourceTypeID <>
    /// tempSourceTypeID order by realSourceTypeID, beginModelYearID`.
    /// Records with `real_source_type_id == temp_source_type_id` are skipped
    /// here too, mirroring the query's `WHERE` clause defensively.
    ///
    /// The build order matters for [`by_real_source_type`](Self) only: when
    /// two records share a `realSourceTypeID`, the last one wins, so callers
    /// must preserve the query's `ORDER BY realSourceTypeID,
    /// beginModelYearID` — the same contract the Go has with its SQL layer.
    #[must_use]
    pub fn build(details: impl IntoIterator<Item = SourceUseTypePhysicsMappingDetail>) -> Self {
        let mut all = Vec::new();
        let mut by_temp_source_type = HashMap::new();
        let mut by_real_source_type = HashMap::new();
        for detail in details {
            // Mirror the Go query's `where realSourceTypeID <> tempSourceTypeID`.
            if detail.real_source_type_id == detail.temp_source_type_id {
                continue;
            }
            all.push(detail);
            by_temp_source_type.insert(detail.temp_source_type_id, detail);
            // Last record in build order wins, as in the Go `map[real] = d`.
            by_real_source_type.insert(detail.real_source_type_id, detail);
        }
        Self {
            details: all,
            by_temp_source_type,
            by_real_source_type,
        }
    }

    /// Every mapping record, in build order.
    #[must_use]
    pub fn details(&self) -> &[SourceUseTypePhysicsMappingDetail] {
        &self.details
    }

    /// The record whose `tempSourceTypeID` is `id`, if any.
    #[must_use]
    pub fn temp_source_type_detail(
        &self,
        id: SourceTypeId,
    ) -> Option<&SourceUseTypePhysicsMappingDetail> {
        self.by_temp_source_type.get(&id)
    }

    /// The record whose `realSourceTypeID` is `id`, if any (the
    /// last-build-order record when several share that real source type).
    #[must_use]
    pub fn real_source_type_detail(
        &self,
        id: SourceTypeId,
    ) -> Option<&SourceUseTypePhysicsMappingDetail> {
        self.by_real_source_type.get(&id)
    }

    /// Apply the `SourceTypePhysics` correction to one row, identified by
    /// its source type, operating mode, and pollutant/process.
    ///
    /// Direct port of the per-row rule cascade in the Go
    /// `coreUpdateOperatingModeDistribution_RatesOpModeDistribution`. The Go
    /// walks a fixed sequence of rules guarded by a `didHandle` flag, so the
    /// first rule that matches decides the row; this port `return`s on the
    /// first match, which is equivalent. The rules, in order:
    ///
    /// 1. negative `polProcessID` (a wildcard placeholder) → drop;
    /// 2. a temporary source type whose operating mode is already in its
    ///    offset range, on a running-exhaust row → swap to the real source
    ///    type, keep the mode;
    /// 3. brakewear → swap a temporary source type's row to its real source
    ///    type; drop a non-mapped source type's brakewear row;
    /// 4. a temporary source type's *normal* operating mode, on a
    ///    running-exhaust row → swap to the real source type and shift the
    ///    mode by `opModeIDOffset`;
    /// 5. (unreachable — see the inline comment) drop a temporary source
    ///    type's already-promoted normal mode;
    /// 6. a real source type that now has a mapping → drop its leftover
    ///    normal operating modes;
    /// 7. otherwise → keep the row unchanged.
    #[must_use]
    pub fn correct_row(
        &self,
        source_type_id: SourceTypeId,
        op_mode_id: i32,
        pol_process_id: i32,
    ) -> RowCorrection {
        let temp_detail = self.temp_source_type_detail(source_type_id);
        let real_detail = self.real_source_type_detail(source_type_id);

        // The Go's "polProcessID < 0 || mod(polProcessID,100) = 1" guard,
        // shared by rules 2, 4, 5 and 6. After rule 1 the `< 0` half is
        // always false (negatives are dropped); it is kept here for a
        // literal correspondence to the Go expression.
        let running_or_wildcard =
            pol_process_id < 0 || pol_process_id % 100 == RUNNING_EXHAUST_PROCESS_ID;

        // Rule 1 — drop wildcard placeholders (negative polProcessID).
        if pol_process_id < 0 {
            return RowCorrection::Drop;
        }

        // Rule 2 — a temporary source type whose operating mode is already
        // in its offset range: swap to the real source type, keep the mode.
        if let Some(detail) = temp_detail.filter(|d| {
            running_or_wildcard
                && (d.op_mode_id_offset..d.op_mode_id_offset + NORMAL_OP_MODE_COUNT)
                    .contains(&op_mode_id)
        }) {
            return RowCorrection::Keep {
                source_type_id: detail.real_source_type_id,
                op_mode_id,
            };
        }

        // Rule 3 — brakewear: a temporary source type's brakewear row swaps
        // to the real source type; a non-mapped source type's brakewear row
        // is dropped. Brakewear is not a running-exhaust process, so rules
        // 2/4/5/6 never reach a brakewear row.
        if pol_process_id == BRAKEWEAR_POL_PROCESS_ID {
            return match temp_detail {
                Some(detail) => RowCorrection::Keep {
                    source_type_id: detail.real_source_type_id,
                    op_mode_id,
                },
                None => RowCorrection::Drop,
            };
        }

        // Rule 4 — promote a temporary source type's normal operating mode:
        // swap to the real source type and shift the mode by the offset.
        if let Some(detail) = temp_detail
            .filter(|_| running_or_wildcard && (0..NORMAL_OP_MODE_COUNT).contains(&op_mode_id))
        {
            return RowCorrection::Keep {
                source_type_id: detail.real_source_type_id,
                op_mode_id: op_mode_id + detail.op_mode_id_offset,
            };
        }

        // Rule 5 — drop a temporary source type's already-promoted normal
        // operating mode. Unreachable: rule 4's condition is a superset of
        // this one (rule 5 only adds `opModeIDOffset > 0`), so any row that
        // reaches here already failed rule 4's guard. Ported for a faithful,
        // line-by-line correspondence to the Go.
        if temp_detail.is_some_and(|d| {
            d.op_mode_id_offset > 0
                && running_or_wildcard
                && (0..NORMAL_OP_MODE_COUNT).contains(&op_mode_id)
        }) {
            return RowCorrection::Drop;
        }

        // Rule 6 — a real source type that now has a mapping (offset > 0) no
        // longer owns the normal operating modes: drop them.
        if real_detail.is_some_and(|d| {
            temp_detail.is_none()
                && d.op_mode_id_offset > 0
                && running_or_wildcard
                && (0..NORMAL_OP_MODE_COUNT).contains(&op_mode_id)
        }) {
            return RowCorrection::Drop;
        }

        // Rule 7 — default: keep the row unchanged.
        RowCorrection::Keep {
            source_type_id,
            op_mode_id,
        }
    }

    /// Apply the correction to a full [`OpModeDistributionRow`].
    ///
    /// Returns `None` when the row is dropped, or `Some(row)` with the
    /// source type and operating mode rewritten per [`correct_row`] and
    /// every other column carried through unchanged.
    ///
    /// [`correct_row`]: Self::correct_row
    #[must_use]
    pub fn correct(&self, row: OpModeDistributionRow) -> Option<OpModeDistributionRow> {
        match self.correct_row(row.source_type_id, row.op_mode_id, row.pol_process_id) {
            RowCorrection::Drop => None,
            RowCorrection::Keep {
                source_type_id,
                op_mode_id,
            } => Some(OpModeDistributionRow {
                source_type_id,
                op_mode_id,
                ..row
            }),
        }
    }

    /// Apply the correction to a whole `RatesOpModeDistribution` table.
    ///
    /// Port of the Go
    /// `coreUpdateOperatingModeDistribution_RatesOpModeDistribution` row
    /// loop. Rows are processed in the Go `ORDER BY` order — descending by
    /// primary key — and the rewritten rows are de-duplicated with
    /// `INSERT IGNORE` semantics (first-written wins). The two together
    /// resolve the rule-4 collision described in the module docs: when a
    /// promoted row's shifted operating mode lands on an already-offset
    /// row's primary key, the already-offset row, processed first, survives.
    ///
    /// The result is returned in ascending primary-key order — a
    /// `RatesOpModeDistribution` table is a set, so the order is for
    /// deterministic output only.
    #[must_use]
    pub fn correct_table(
        &self,
        rows: impl IntoIterator<Item = OpModeDistributionRow>,
    ) -> Vec<OpModeDistributionRow> {
        // The Go reads `RatesOpModeDistribution` ordered by its full primary
        // key descending; process the rows in that order so the
        // `INSERT IGNORE` collision resolution below keeps the right row.
        let mut input: Vec<OpModeDistributionRow> = rows.into_iter().collect();
        input.sort_unstable_by_key(|row| Reverse(row.primary_key()));

        // `LOAD DATA INFILE ... IGNORE`: when two rewritten rows share an
        // output primary key, the first one written wins.
        let mut seen: HashSet<RowKey> = HashSet::new();
        let mut out: Vec<OpModeDistributionRow> = Vec::new();
        for row in input {
            let Some(corrected) = self.correct(row) else {
                continue;
            };
            if seen.insert(corrected.primary_key()) {
                out.push(corrected);
            }
        }

        out.sort_unstable_by_key(OpModeDistributionRow::primary_key);
        out
    }
}

/// Master-loop subscriptions of [`SourceTypePhysics`] — none; see
/// [`Generator::subscriptions`] on the impl below.
static NO_SUBSCRIPTIONS: &[CalculatorSubscription] = &[];

/// Default-DB and scratch tables the `SourceTypePhysics` step reads:
/// `sourceUseTypePhysicsMapping` (the mapping) and `RatesOpModeDistribution`
/// (the table it rewrites in place).
static INPUT_TABLES: &[&str] = &["sourceUseTypePhysicsMapping", "RatesOpModeDistribution"];

/// Scratch table the step writes — it rewrites `RatesOpModeDistribution` in
/// place (the Go renames `genRatesOpModeDistribution` over it).
static OUTPUT_TABLES: &[&str] = &["RatesOpModeDistribution"];

/// The `SourceTypePhysics` operating-mode-distribution correction as a
/// chain-DAG [`Generator`].
///
/// The numerically faithful work lives on [`SourceUseTypePhysicsMapping`];
/// this zero-sized type exists so the step is a named module the calculator
/// chain can refer to — `RatesOperatingModeDistributionGenerator` lists
/// `"SourceTypePhysics"` among its `upstream` modules.
#[derive(Debug, Clone, Copy, Default)]
pub struct SourceTypePhysics;

impl SourceTypePhysics {
    /// Chain-DAG name — matches the Java class / Go package name.
    pub const NAME: &'static str = "SourceTypePhysics";

    /// Construct the generator.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl Generator for SourceTypePhysics {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    /// `SourceTypePhysics` carries no master-loop subscription of its own:
    /// the pinned `SourceTypePhysics.java` is a helper class, not a
    /// `MasterLoopable`, and the step is not a node in the reconstructed
    /// calculator chain DAG. Its correction runs when the rates-mode
    /// operating-mode pipeline invokes it. The registry still registers the
    /// module by [`name`](Generator::name) so chain `upstream` references to
    /// `"SourceTypePhysics"` resolve.
    fn subscriptions(&self) -> &[CalculatorSubscription] {
        NO_SUBSCRIPTIONS
    }

    fn input_tables(&self) -> &[&'static str] {
        INPUT_TABLES
    }

    fn output_tables(&self) -> &[&'static str] {
        OUTPUT_TABLES
    }

    /// Run the correction for the current master-loop iteration.
    ///
    /// **Data plane pending (Task 50).** [`CalculatorContext`] exposes only
    /// placeholder `ExecutionTables` / `ScratchNamespace` today, so this
    /// body cannot read `sourceUseTypePhysicsMapping` /
    /// `RatesOpModeDistribution` nor write the corrected table back. The
    /// faithful algorithm is ported and tested on
    /// [`SourceUseTypePhysicsMapping`]; once the `DataFrameStore` lands,
    /// `execute` builds a [`SourceUseTypePhysicsMapping`] from
    /// `ctx.tables()`, applies
    /// [`correct_table`](SourceUseTypePhysicsMapping::correct_table) to the
    /// scratch `RatesOpModeDistribution`, and stores the result.
    fn execute(&self, _ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
        Ok(CalculatorOutput::empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A `sourceUseTypePhysicsMapping` record helper.
    fn detail(real: u16, temp: u16, offset: i32) -> SourceUseTypePhysicsMappingDetail {
        SourceUseTypePhysicsMappingDetail {
            real_source_type_id: SourceTypeId(real),
            temp_source_type_id: SourceTypeId(temp),
            op_mode_id_offset: offset,
        }
    }

    /// A `RatesOpModeDistribution` row helper with fixed carry-through
    /// columns — only source type, operating mode and pollutant/process,
    /// the fields the correction inspects, are parameterised.
    fn row(source_type: u16, op_mode: i32, pol_process: i32) -> OpModeDistributionRow {
        OpModeDistributionRow {
            source_type_id: SourceTypeId(source_type),
            road_type_id: 5,
            avg_speed_bin_id: 16,
            hour_day_id: 51,
            pol_process_id: pol_process,
            op_mode_id: op_mode,
            op_mode_fraction: 0.4,
            op_mode_fraction_cv: 0.0,
            avg_bin_speed: 32.5,
            avg_speed_fraction: 0.6,
        }
    }

    /// A running-exhaust `polProcessID` (`mod 100 == 1`).
    const RUNNING: i32 = 101;
    /// A non-running `polProcessID` — start exhaust, process 2.
    const START: i32 = 102;

    #[test]
    fn build_indexes_details_by_temp_and_real_source_type() {
        let mapping =
            SourceUseTypePhysicsMapping::build([detail(20, 120, 1000), detail(30, 130, 2000)]);
        assert_eq!(mapping.details().len(), 2);
        assert_eq!(
            mapping.temp_source_type_detail(SourceTypeId(120)),
            Some(&detail(20, 120, 1000)),
        );
        assert_eq!(
            mapping.real_source_type_detail(SourceTypeId(30)),
            Some(&detail(30, 130, 2000)),
        );
        // A real source type is not indexed as a temporary one.
        assert!(mapping.temp_source_type_detail(SourceTypeId(20)).is_none());
        assert!(mapping.real_source_type_detail(SourceTypeId(120)).is_none());
    }

    #[test]
    fn build_skips_records_with_equal_real_and_temp_source_type() {
        // Mirrors the Go query's `where realSourceTypeID <> tempSourceTypeID`.
        let mapping =
            SourceUseTypePhysicsMapping::build([detail(20, 120, 1000), detail(30, 30, 0)]);
        assert_eq!(mapping.details().len(), 1);
        assert!(mapping.temp_source_type_detail(SourceTypeId(30)).is_none());
        assert!(mapping.real_source_type_detail(SourceTypeId(30)).is_none());
    }

    #[test]
    fn build_last_record_wins_for_duplicate_real_source_type() {
        // Two records share realSourceTypeID 20; the Go `map[real] = d`
        // keeps the last in build order.
        let mapping =
            SourceUseTypePhysicsMapping::build([detail(20, 120, 1000), detail(20, 121, 2000)]);
        assert_eq!(mapping.details().len(), 2);
        assert_eq!(
            mapping.real_source_type_detail(SourceTypeId(20)),
            Some(&detail(20, 121, 2000)),
        );
        // Both temporary source types remain individually indexed.
        assert!(mapping.temp_source_type_detail(SourceTypeId(120)).is_some());
        assert!(mapping.temp_source_type_detail(SourceTypeId(121)).is_some());
    }

    #[test]
    fn rule1_negative_pol_process_id_is_dropped() {
        let mapping = SourceUseTypePhysicsMapping::build([detail(20, 120, 1000)]);
        // Dropped for a mapped temporary source type ...
        assert_eq!(
            mapping.correct_row(SourceTypeId(120), 5, -1),
            RowCorrection::Drop,
        );
        // ... and for a source type absent from the mapping.
        assert_eq!(
            mapping.correct_row(SourceTypeId(99), 5, -7),
            RowCorrection::Drop,
        );
    }

    #[test]
    fn rule2_offset_range_mode_swaps_source_type_keeps_mode() {
        let mapping = SourceUseTypePhysicsMapping::build([detail(20, 120, 1000)]);
        // op mode 1005 is inside the offset range [1000, 1100).
        assert_eq!(
            mapping.correct_row(SourceTypeId(120), 1005, RUNNING),
            RowCorrection::Keep {
                source_type_id: SourceTypeId(20),
                op_mode_id: 1005,
            },
        );
    }

    #[test]
    fn rule2_requires_running_or_wildcard_pol_process() {
        let mapping = SourceUseTypePhysicsMapping::build([detail(20, 120, 1000)]);
        // Same row on a non-running process: rule 2 does not fire, and no
        // later rule matches, so the row is kept unchanged (rule 7).
        assert_eq!(
            mapping.correct_row(SourceTypeId(120), 1005, START),
            RowCorrection::Keep {
                source_type_id: SourceTypeId(120),
                op_mode_id: 1005,
            },
        );
    }

    #[test]
    fn rule2_offset_range_boundaries() {
        let mapping = SourceUseTypePhysicsMapping::build([detail(20, 120, 1000)]);
        let keep_swapped = RowCorrection::Keep {
            source_type_id: SourceTypeId(20),
            op_mode_id: 1000,
        };
        // op mode == offset is inside the range ...
        assert_eq!(
            mapping.correct_row(SourceTypeId(120), 1000, RUNNING),
            keep_swapped
        );
        // ... offset + 99 is the last mode inside it ...
        assert_eq!(
            mapping.correct_row(SourceTypeId(120), 1099, RUNNING),
            RowCorrection::Keep {
                source_type_id: SourceTypeId(20),
                op_mode_id: 1099,
            },
        );
        // ... offset + 100 is past it, and op mode 1100 is not a normal
        // mode either, so rule 7 keeps it unchanged.
        assert_eq!(
            mapping.correct_row(SourceTypeId(120), 1100, RUNNING),
            RowCorrection::Keep {
                source_type_id: SourceTypeId(120),
                op_mode_id: 1100,
            },
        );
    }

    #[test]
    fn rule3_brakewear_with_temp_detail_swaps_source_type() {
        let mapping = SourceUseTypePhysicsMapping::build([detail(20, 120, 1000)]);
        assert_eq!(
            mapping.correct_row(SourceTypeId(120), 7, BRAKEWEAR_POL_PROCESS_ID),
            RowCorrection::Keep {
                source_type_id: SourceTypeId(20),
                op_mode_id: 7,
            },
        );
    }

    #[test]
    fn rule3_brakewear_without_mapping_is_dropped() {
        let mapping = SourceUseTypePhysicsMapping::build([detail(20, 120, 1000)]);
        // Source type 999 is not a temporary source type in the mapping.
        assert_eq!(
            mapping.correct_row(SourceTypeId(999), 7, BRAKEWEAR_POL_PROCESS_ID),
            RowCorrection::Drop,
        );
    }

    #[test]
    fn rule4_promotes_normal_mode_by_offset() {
        // This row also satisfies rule 5's literal condition (temp detail,
        // offset > 0, normal mode, running) — rule 4 reaches it first and
        // promotes it, demonstrating rule 5 is shadowed.
        let mapping = SourceUseTypePhysicsMapping::build([detail(20, 120, 1000)]);
        assert_eq!(
            mapping.correct_row(SourceTypeId(120), 7, RUNNING),
            RowCorrection::Keep {
                source_type_id: SourceTypeId(20),
                op_mode_id: 1007,
            },
        );
    }

    #[test]
    fn rule4_requires_running_or_wildcard_pol_process() {
        let mapping = SourceUseTypePhysicsMapping::build([detail(20, 120, 1000)]);
        // Normal mode on a non-running process: no rule fires, kept as-is.
        assert_eq!(
            mapping.correct_row(SourceTypeId(120), 7, START),
            RowCorrection::Keep {
                source_type_id: SourceTypeId(120),
                op_mode_id: 7,
            },
        );
    }

    #[test]
    fn rule2_precedes_rule4_when_offset_below_100() {
        // offset 50 makes rule 2's range [50, 150) overlap rule 4's [0, 100)
        // on [50, 100). op mode 70 is in the overlap: rule 2 wins, so the
        // mode is kept (70), not promoted (70 + 50 = 120).
        let mapping = SourceUseTypePhysicsMapping::build([detail(20, 120, 50)]);
        assert_eq!(
            mapping.correct_row(SourceTypeId(120), 70, RUNNING),
            RowCorrection::Keep {
                source_type_id: SourceTypeId(20),
                op_mode_id: 70,
            },
        );
    }

    #[test]
    fn rule6_drops_leftover_real_source_type_normal_modes() {
        let mapping = SourceUseTypePhysicsMapping::build([detail(20, 120, 1000)]);
        // Source type 20 is the real source type; its normal modes on a
        // running process are now owned by the temporary source type.
        assert_eq!(
            mapping.correct_row(SourceTypeId(20), 7, RUNNING),
            RowCorrection::Drop,
        );
    }

    #[test]
    fn rule6_keeps_real_source_type_offset_modes() {
        let mapping = SourceUseTypePhysicsMapping::build([detail(20, 120, 1000)]);
        // op mode 1500 is not a normal mode, so rule 6 does not fire.
        assert_eq!(
            mapping.correct_row(SourceTypeId(20), 1500, RUNNING),
            RowCorrection::Keep {
                source_type_id: SourceTypeId(20),
                op_mode_id: 1500,
            },
        );
    }

    #[test]
    fn rule6_skipped_when_real_source_type_has_zero_offset() {
        // realSourceTypeDetail.opModeIDOffset must be > 0 for rule 6.
        let mapping = SourceUseTypePhysicsMapping::build([detail(20, 120, 0)]);
        assert_eq!(
            mapping.correct_row(SourceTypeId(20), 7, RUNNING),
            RowCorrection::Keep {
                source_type_id: SourceTypeId(20),
                op_mode_id: 7,
            },
        );
    }

    #[test]
    fn rule7_unmapped_source_type_kept_unchanged() {
        let mapping = SourceUseTypePhysicsMapping::build([detail(20, 120, 1000)]);
        // Source type 55 appears nowhere in the mapping.
        assert_eq!(
            mapping.correct_row(SourceTypeId(55), 8, RUNNING),
            RowCorrection::Keep {
                source_type_id: SourceTypeId(55),
                op_mode_id: 8,
            },
        );
    }

    #[test]
    fn empty_mapping_keeps_every_row_unchanged() {
        let mapping = SourceUseTypePhysicsMapping::default();
        assert!(mapping.details().is_empty());
        assert_eq!(
            mapping.correct_row(SourceTypeId(120), 7, RUNNING),
            RowCorrection::Keep {
                source_type_id: SourceTypeId(120),
                op_mode_id: 7,
            },
        );
    }

    #[test]
    fn correct_carries_through_unmodified_columns() {
        let mapping = SourceUseTypePhysicsMapping::build([detail(20, 120, 1000)]);
        let input = row(120, 7, RUNNING);
        // Rule 4 swaps the source type and promotes the mode; every other
        // column — road type, speed bin, hour/day, all four fractions —
        // is carried through unchanged.
        let expected = OpModeDistributionRow {
            source_type_id: SourceTypeId(20),
            op_mode_id: 1007,
            ..input
        };
        assert_eq!(mapping.correct(input), Some(expected));
    }

    #[test]
    fn correct_returns_none_for_dropped_row() {
        let mapping = SourceUseTypePhysicsMapping::build([detail(20, 120, 1000)]);
        assert!(mapping.correct(row(120, 5, -1)).is_none());
    }

    #[test]
    fn correct_table_offset_collision_keeps_already_offset_row() {
        // The module-doc collision: with offset 1000, an already-offset row
        // (op mode 1005, rule 2) and a normal row (op mode 5, rule 4 →
        // 5 + 1000 = 1005) both rewrite to the same primary key. The Go
        // `ORDER BY` processes the larger original op mode first, and
        // `INSERT IGNORE` keeps it — so the already-offset row survives.
        let mapping = SourceUseTypePhysicsMapping::build([detail(20, 120, 1000)]);
        let already_offset = OpModeDistributionRow {
            op_mode_fraction: 0.9,
            ..row(120, 1005, RUNNING)
        };
        let normal = OpModeDistributionRow {
            op_mode_fraction: 0.1,
            ..row(120, 5, RUNNING)
        };
        // Pass the loser first to prove `correct_table` sorts internally.
        let result = mapping.correct_table([normal, already_offset]);
        let expected = OpModeDistributionRow {
            source_type_id: SourceTypeId(20),
            op_mode_id: 1005,
            ..already_offset
        };
        assert_eq!(result, vec![expected]);
    }

    #[test]
    fn correct_table_excludes_dropped_rows() {
        let mapping = SourceUseTypePhysicsMapping::build([detail(20, 120, 1000)]);
        let dropped = row(120, 5, -1); // rule 1
        let kept = row(55, 8, RUNNING); // rule 7
        assert_eq!(mapping.correct_table([dropped, kept]), vec![kept]);
    }

    #[test]
    fn correct_table_output_is_sorted_by_primary_key() {
        // Unmapped source types: every row is kept unchanged (rule 7), so
        // the output rows equal the inputs and must come back primary-key
        // sorted regardless of input order.
        let mapping = SourceUseTypePhysicsMapping::default();
        let rows = [
            row(55, 9, RUNNING),
            row(11, 3, RUNNING),
            row(55, 2, RUNNING),
            row(11, 7, START),
        ];
        let result = mapping.correct_table(rows);
        let keys: Vec<RowKey> = result
            .iter()
            .map(OpModeDistributionRow::primary_key)
            .collect();
        let mut sorted = keys.clone();
        sorted.sort_unstable();
        assert_eq!(keys, sorted);
        assert_eq!(result.len(), 4);
    }

    #[test]
    fn generator_metadata() {
        let generator = SourceTypePhysics::new();
        assert_eq!(generator.name(), "SourceTypePhysics");
        // No master-loop subscription — see the trait-impl doc comment.
        assert!(generator.subscriptions().is_empty());
        // No declared upstream module (trait default).
        assert!(generator.upstream().is_empty());
        assert!(generator
            .input_tables()
            .contains(&"sourceUseTypePhysicsMapping"));
        assert!(generator
            .input_tables()
            .contains(&"RatesOpModeDistribution"));
        assert_eq!(generator.output_tables(), &["RatesOpModeDistribution"]);
    }

    #[test]
    fn generator_execute_returns_placeholder_until_data_plane() {
        // execute is a documented placeholder until Task 50; it must still
        // honour the trait contract and return Ok.
        let generator = SourceTypePhysics::new();
        let ctx = CalculatorContext::new();
        assert!(generator.execute(&ctx).is_ok());
    }

    #[test]
    fn generator_is_object_safe() {
        // The registry stores generators as Box<dyn Generator>.
        let generator: Box<dyn Generator> = Box::new(SourceTypePhysics::new());
        assert_eq!(generator.name(), "SourceTypePhysics");
    }
}
