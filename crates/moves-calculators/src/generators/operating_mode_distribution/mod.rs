//! Port of `OperatingModeDistributionGenerator.java`
//! (`gov.epa.otaq.moves.master.implementation.ghg`) — the generator that
//! builds the running-exhaust and brakewear `OpModeDistribution` table.
//!
//! Migration plan: Phase 3, Task 30.
//!
//! # What this generator produces
//!
//! An *operating-mode distribution* is the fraction of a vehicle's operating
//! time spent in each operating mode — idle, the cruise / acceleration VSP
//! bins, braking — broken down by source type, road type, hour/day and
//! pollutant/process. The running-emission calculators multiply an emission
//! *rate per operating mode* by these fractions, so the distribution is the
//! bridge between drive-cycle physics and the inventory.
//!
//! The Java class subscribes to two emission processes at `YEAR` granularity
//! / `GENERATOR` priority — Running Exhaust (process 1) and Brakewear
//! (process 9) — and shares one set of computed fractions across both.
//!
//! # The OMDG pipeline
//!
//! `executeLoop` runs a seven-stage SQL pipeline once per run; the pure
//! computational core is ported in [`pipeline`]:
//!
//! 1. **OMDG-1** ([`bracket_average_speed_bins`]) — bracket each average-speed
//!    bin between the two driving cycles whose average speeds straddle it,
//!    clamping bins outside the cycle range.
//! 2. **OMDG-2/-3** — split each bin's average-speed-distribution fraction
//!    between its bracketing cycles and sum per driving cycle, giving a
//!    per-`(sourceType, roadType, hourDay, driveSchedule)` drive-schedule
//!    fraction.
//! 3. **OMDG-4** — compute second-by-second vehicle-specific power (VSP) from
//!    the road-load polynomial in `sourceUseTypePhysicsMapping`.
//! 4. **OMDG-5** — bin every second into an operating mode from its VSP,
//!    speed and acceleration (braking, idle and the VSP/speed bins).
//! 5. **OMDG-6** — divide the per-mode second counts by the total, giving an
//!    operating-mode fraction per driving cycle.
//! 6. **OMDG-7** ([`op_mode_distribution`]) — weight the per-cycle fractions
//!    by the drive-schedule fractions and sum, then copy each represented
//!    pollutant/process its representative's distribution.
//!
//! # Scope of the port
//!
//! [`op_mode_distribution`] ports stages OMDG-1 … OMDG-7 in full and yields
//! every `OpModeFraction2` row — the operating-mode fraction per
//! `(sourceType, roadType, hourDay, polProcess, opMode)`. The Java's final
//! `calculateOpModeFractions` step is a thin data-plane projection: it
//! cross-joins `OpModeFraction2` with `Link` (`roadTypeID → linkID`) to fill
//! the link-keyed `OpModeDistribution` table, skipping link rows a user
//! supplied directly. That projection belongs to [`Generator::execute`] once
//! the Task 50 data plane lands; see [`OpModeFractionRow`].
//!
//! **Deferred to `SourceTypePhysics` (migration-plan Task 37).** `executeLoop`
//! also calls `modelYearPhysics.setup`, `modelYearPhysics.updateEmissionRateTables`
//! and `modelYearPhysics.updateOperatingModeDistribution`. Those belong to the
//! `SourceTypePhysics` port; this generator *consumes*
//! `sourceUseTypePhysicsMapping` (which `SourceTypePhysics.setup` populates)
//! as an input — the road-load polynomial terms it carries drive the VSP
//! calculation, and its real/temporary source-type rows let one source type
//! split by model-year range. Output rows therefore key on a *temporary*
//! source type wherever such a split applies.
//!
//! # Numeric fidelity
//!
//! The port computes in `f64` throughout, matching the
//! `baserategenerator::drivecycle` sibling. The canonical MariaDB path is
//! coarser: `OpModeFractionBySchedule` and `OpModeFraction2` are `FLOAT`
//! (`f32`) columns, and the OMDG-6 `count(*) / secondSum` is integer division,
//! which MariaDB evaluates as `DECIMAL` rounded to `div_precision_increment`
//! places. The resulting divergence is systematic, bounded and tracked by the
//! Task 44 generator-validation tolerance budget.
//!
//! # Data plane (Task 50)
//!
//! [`Generator::execute`] receives a [`CalculatorContext`] whose
//! `ExecutionTables` / `ScratchNamespace` are Phase-2 placeholders until the
//! `DataFrameStore` lands (migration-plan Task 50), so `execute` cannot yet
//! read the input tables nor write `OpModeDistribution`. The numerically
//! faithful algorithm is fully ported and unit-tested in [`op_mode_distribution`];
//! once the data plane exists, `execute` projects an [`OmdgInputs`] out of
//! `ctx.tables()`, runs [`op_mode_distribution`], cross-joins the result with
//! `Link`, and writes `OpModeDistribution` to the scratch namespace.

pub mod inputs;
pub mod pipeline;

use moves_calculator_info::{Granularity, Priority};
use moves_data::ProcessId;
use moves_framework::{
    CalculatorContext, CalculatorOutput, CalculatorSubscription, Error, Generator,
};

pub use inputs::{
    AvgSpeedBinRow, AvgSpeedDistributionRow, DriveScheduleAssocRow, DriveScheduleRow,
    DriveScheduleSecondRow, OmdgInputs, OpModePolProcAssocRow, OperatingModeRow, PhysicsMappingRow,
    PolProcessRepresentedRow,
};
pub use pipeline::{
    bracket_average_speed_bins, op_mode_distribution, validate_drive_schedule_distribution, BinKey,
    BracketBin, OpModeFractionRow,
};

/// Running Exhaust — process id 1. The Java `subscribeToMe` subscribes it
/// (`MasterLoopGranularity.YEAR`, year level for source bins from the
/// `SourceBinDistributionGenerator`).
const RUNNING_EXHAUST: ProcessId = ProcessId(1);
/// Brakewear — process id 9. The Java `subscribeToMe` subscribes it at the
/// same granularity and priority as Running Exhaust.
const BRAKEWEAR: ProcessId = ProcessId(9);

/// `OpModeDistribution` generator for running-exhaust and brakewear runs.
///
/// Ports `OperatingModeDistributionGenerator.java`; see the module
/// documentation for the scope of the port.
#[derive(Debug, Clone)]
pub struct OperatingModeDistributionGenerator {
    /// The two master-loop subscriptions, built once in [`Self::new`].
    subscriptions: [CalculatorSubscription; 2],
}

impl OperatingModeDistributionGenerator {
    /// Chain-DAG name — matches the Java class name.
    pub const NAME: &'static str = "OperatingModeDistributionGenerator";

    /// Construct the generator with its master-loop subscriptions.
    ///
    /// Mirrors `subscribeToMe`: Running Exhaust and Brakewear, both at `YEAR`
    /// granularity (year level for source bins from the
    /// `SourceBinDistributionGenerator`), `GENERATOR` priority.
    #[must_use]
    pub fn new() -> Self {
        let priority =
            Priority::parse("GENERATOR").expect("\"GENERATOR\" is a valid MasterLoop priority");
        let at_year = |process| CalculatorSubscription::new(process, Granularity::Year, priority);
        Self {
            subscriptions: [at_year(RUNNING_EXHAUST), at_year(BRAKEWEAR)],
        }
    }
}

impl Default for OperatingModeDistributionGenerator {
    fn default() -> Self {
        Self::new()
    }
}

/// Default-database tables the OMDG pipeline reads. Names are the canonical
/// MOVES table names; the registry maps them onto Parquet snapshots. `Link`
/// is read only by the `calculateOpModeFractions` link projection.
static INPUT_TABLES: &[&str] = &[
    "DriveSchedule",
    "DriveScheduleAssoc",
    "DriveScheduleSecond",
    "AvgSpeedBin",
    "AvgSpeedDistribution",
    "OperatingMode",
    "OpModePolProcAssoc",
    "sourceUseTypePhysicsMapping",
    "OMDGPolProcessRepresented",
    "RunSpecSourceType",
    "RunSpecRoadType",
    "RunSpecHour",
    "RunSpecDay",
    "HourDay",
    "Link",
];

/// Scratch-namespace table this generator writes.
static OUTPUT_TABLES: &[&str] = &["OpModeDistribution"];

/// Upstream module: `SourceTypePhysics` builds `sourceUseTypePhysicsMapping` —
/// the road-load polynomial terms and real/temporary source-type rows the VSP
/// calculation reads.
static UPSTREAM: &[&str] = &["SourceTypePhysics"];

impl Generator for OperatingModeDistributionGenerator {
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

    /// Run the generator for the current master-loop iteration.
    ///
    /// **Data plane pending (Task 50).** [`CalculatorContext`] exposes only
    /// placeholder `ExecutionTables` / `ScratchNamespace` today, so this body
    /// cannot read the [`input_tables`](Generator::input_tables) nor write
    /// `OpModeDistribution`. The numerically faithful algorithm is fully
    /// ported and tested in [`op_mode_distribution`]; once the `DataFrameStore`
    /// lands, `execute` will project an [`OmdgInputs`] from `ctx.tables()`,
    /// run [`op_mode_distribution`], cross-join the result with `Link` on
    /// `roadTypeID`, and store the link-keyed `OpModeDistribution` rows.
    fn execute(&self, _ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
        Ok(CalculatorOutput::empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generator_metadata_matches_java_subscribe_to_me() {
        let generator = OperatingModeDistributionGenerator::new();
        assert_eq!(generator.name(), "OperatingModeDistributionGenerator");
        assert_eq!(generator.output_tables(), &["OpModeDistribution"]);
        assert_eq!(generator.upstream(), &["SourceTypePhysics"]);
        assert!(generator.input_tables().contains(&"AvgSpeedDistribution"));
        assert!(generator.input_tables().contains(&"DriveScheduleSecond"));

        let subscriptions = generator.subscriptions();
        assert_eq!(subscriptions.len(), 2);
        let processes: Vec<ProcessId> = subscriptions.iter().map(|s| s.process_id).collect();
        // Running Exhaust (1) and Brakewear (9).
        assert_eq!(processes, vec![ProcessId(1), ProcessId(9)]);
        for subscription in subscriptions {
            assert_eq!(subscription.granularity, Granularity::Year);
            assert_eq!(subscription.priority.display(), "GENERATOR");
        }
    }

    #[test]
    fn generator_execute_returns_placeholder_until_data_plane() {
        // execute is a documented placeholder until Task 50; it must still
        // honour the trait contract and return Ok.
        let generator = OperatingModeDistributionGenerator::new();
        let ctx = CalculatorContext::new();
        assert!(generator.execute(&ctx).is_ok());
    }

    #[test]
    fn generator_is_object_safe() {
        // The registry stores generators as Box<dyn Generator>.
        let generator: Box<dyn Generator> = Box::new(OperatingModeDistributionGenerator::new());
        assert_eq!(generator.name(), "OperatingModeDistributionGenerator");
    }

    #[test]
    fn generator_default_matches_new() {
        let generator = OperatingModeDistributionGenerator::default();
        assert_eq!(generator.subscriptions().len(), 2);
    }
}
