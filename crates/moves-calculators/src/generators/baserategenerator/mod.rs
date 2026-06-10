//! Base Rate Generator —.
//!
//! Pure-Rust port of `generators/baserategenerator/baserategenerator.go`
//! (2,391 lines), one of the two largest pieces of Go in the MOVES worker.
//! The generator builds the `BaseRate` and `BaseRateByAge` tables that the
//! `BaseRateCalculator` consumes — its output is the input to
//! every running-emission calculation, so numerical fidelity is critical.
//!
//! # Module map
//!
//! | Module | Ports |
//! |--------|-------|
//! | [`model`] | the Go struct declarations and `readExternalFlags` |
//! | [`inputs`] | `setupTables` and the thirteen `read*` table loaders |
//! | [`aggregate`] | `coreBaseRateGeneratorFromRatesOpModeDistribution` and the three `makeBaseRate*` aggregators |
//! | [`drivecycle`] | `findDriveCycles`, `calculateDriveCycleOpModeDistribution`, `processDriveCycles` |
//!
//! # What the Go did, and what this port keeps
//!
//! The Go worker read its inputs from a MariaDB execution database, ran the
//! computation across goroutines connected by channels, and streamed the
//! result into temporary files `LOAD DATA INFILE`'d back into MariaDB. The
//! port keeps the **computation** verbatim — the physics-mapping source-type
//! swaps, the weighted-rate aggregation, the vehicle-specific-power
//! operating-mode binning — and replaces the I/O boundary with plain values:
//! a [`BaseRateInputs`] in, a [`BaseRateOutput`] out. The goroutine pipeline
//! collapses to sequential calls because every stage is pure; the producer
//! yields its [`RomdBlock`](model::RomdBlock) stream in exactly the order the
//! Go SQL `ORDER BY` / nested-loop enumeration produced, so the streaming
//! aggregators accumulate identical sums.
//!
//! # Data-plane status
//!
//! [`BaseRateGenerator::run`] is the numerical entry point and is fully
//! exercised by the crate's tests. The [`Generator`] trait's
//! [`execute`](Generator::execute) method is a shell: the
//! [`CalculatorContext`] it receives exposes only the placeholder
//! [`ExecutionTables`] / [`ScratchNamespace`], which have no row storage
//! yet. (`DataFrameStore`) lands that storage; the `execute` body
//! then materialises a [`BaseRateInputs`] from the context, calls
//! [`BaseRateGenerator::run`], and writes the [`BaseRateOutput`] back into
//! the scratch namespace. Until then `execute` returns an empty
//! [`CalculatorOutput`] and the metadata methods carry the real wiring
//! information the registry needs.
//!
//! [`ExecutionTables`]: moves_framework::ExecutionTables
//! [`ScratchNamespace`]: moves_framework::ScratchNamespace

pub mod aggregate;
pub mod drivecycle;
pub mod inputs;
pub mod model;
pub mod sbweighted;

use std::sync::OnceLock;

use moves_calculator_info::{Granularity, Priority};
use moves_data::ProcessId;
use moves_framework::{
    CalculatorContext, CalculatorOutput, CalculatorSubscription, DataFrameStore,
    DataFrameStoreTyped, Error, Generator, InMemoryStore, IntoDataFrame, ModelScale, TableRow,
};
use polars::prelude::{DataFrame, DataType, NamedFrom, PolarsResult, Schema, Series};

pub use inputs::{BaseRateInputs, PreparedTables};
pub use model::{
    BaseRateOutputRecord, DrivingIdleFractionRow, ExternalFlags, ALWAYS_USE_ROMD_TABLE,
};

use aggregate::{
    core_base_rate_generator_from_romd, make_base_rate_by_age_from_source_bin_rates,
    make_base_rate_from_distance_rates, make_base_rate_from_source_bin_rates,
};
use drivecycle::process_drive_cycles;
use inputs::{
    AvgSpeedBinRow, AvgSpeedDistributionRow, DriveScheduleAssocRow, DriveScheduleRow,
    DriveScheduleSecondRow, OpModePolProcRow, RatesOpModeDistributionRow, SbWeightedDistanceRow,
};
use model::{OperatingMode, SbWeightedRateDetail, SourceUseTypePhysicsMappingDetail};

/// Stable module name in the calculator-chain DAG.
const GENERATOR_NAME: &str = "BaseRateGenerator";

/// Default-DB and scratch tables the generator reads. Names match the
/// casing used in the MOVES default database.
static INPUT_TABLES: &[&str] = &[
    "RatesOpModeDistribution",
    // Source-bin-weighted rates are computed in-process (see `sbweighted`), so
    // depend on the runtime SourceBin / SourceBinDistribution the
    // SourceBinDistributionGenerator produces (and the raw rate + lookup tables)
    // rather than the never-materialised SBWeighted* tables. Declaring these
    // co-locates this generator with the SourceBinDistributionGenerator in the
    // same execution chunk so its scratch output is visible here.
    "SourceBin",
    "SourceBinDistribution",
    "EmissionRateByAge",
    "EmissionRate",
    "PollutantProcessModelYear",
    "SourceTypeModelYear",
    "PollutantProcessAssoc",
    "fullACAdjustment",
    "AgeCategory",
    "avgSpeedBin",
    "avgSpeedDistribution",
    "driveSchedule",
    "driveScheduleAssoc",
    "driveScheduleSecond",
    "operatingMode",
    "sourceUseTypePhysicsMapping",
    "runSpecRoadType",
    "runSpecHourDay",
    "runSpecSourceType",
    "runSpecPollutantProcess",
    "opModePolProcAssoc",
];

/// Scratch tables the generator writes for downstream calculators.
static OUTPUT_TABLES: &[&str] = &["BaseRate", "BaseRateByAge", "DrivingIdleFraction"];

/// The tables the Base Rate Generator produces in one run.
///
/// `base_rate` is the union of the source-bin-weighted and distance-weighted
/// contributions — the Go loaded both into the same `baseRate_<p>_<y>`
/// table. `driving_idle_fraction` is populated only on the drive-cycle path.
#[derive(Debug, Clone, Default)]
pub struct BaseRateOutput {
    /// Rows of the `BaseRate` table.
    pub base_rate: Vec<BaseRateOutputRecord>,
    /// Rows of the `BaseRateByAge` table.
    pub base_rate_by_age: Vec<BaseRateOutputRecord>,
    /// Rows of the `DrivingIdleFraction` table (drive-cycle path only).
    pub driving_idle_fraction: Vec<DrivingIdleFractionRow>,
}

/// The Base Rate Generator.
///
/// A zero-sized value type: the generator owns no per-run state, exactly as
/// the [`Generator`] trait contract requires. All run-varying input flows
/// through [`BaseRateGenerator::run`]'s arguments.
#[derive(Debug, Clone, Copy, Default)]
pub struct BaseRateGenerator;

impl BaseRateGenerator {
    /// Run the generator over a fully materialised set of input tables.
    ///
    /// Ports `BaseRateGeneratorFromRatesOpModeDistribution`. The drive-cycle
    /// fast path is taken for non-Project Running Exhaust (`processID == 1`)
    /// and Brakewear (`processID == 9`); every other process, and every
    /// Project-domain run, takes the `RatesOpModeDistribution` core path.
    /// Distance-based rates are always folded into the `BaseRate` output.
    #[must_use]
    pub fn run(inputs: &BaseRateInputs, flags: &ExternalFlags) -> BaseRateOutput {
        let prepared = PreparedTables::from_inputs(inputs, flags);

        // shouldProcessDriveCycles — EMT-633 added processID 9 (Brakewear)
        // alongside processID 1 (Running Exhaust). ALWAYS_USE_ROMD_TABLE
        // forces the core path; it is `false` in normal operation.
        let should_process_drive_cycles = !inputs.is_project
            && (flags.process_id == 1 || flags.process_id == 9)
            && !ALWAYS_USE_ROMD_TABLE;

        let (romd_blocks, driving_idle_fraction) = if should_process_drive_cycles {
            let drive = process_drive_cycles(inputs, &prepared, flags);
            (drive.romd_blocks, drive.driving_idle_fraction)
        } else {
            (
                core_base_rate_generator_from_romd(inputs, &prepared, flags),
                Vec::new(),
            )
        };

        let mut base_rate = make_base_rate_from_source_bin_rates(&romd_blocks, &prepared, flags);
        base_rate.extend(make_base_rate_from_distance_rates(inputs, &prepared, flags));
        let base_rate_by_age =
            make_base_rate_by_age_from_source_bin_rates(&romd_blocks, &prepared, flags);

        BaseRateOutput {
            base_rate,
            base_rate_by_age,
            driving_idle_fraction,
        }
    }
}

impl Generator for BaseRateGenerator {
    fn name(&self) -> &'static str {
        GENERATOR_NAME
    }

    fn subscriptions(&self) -> &[CalculatorSubscription] {
        // Built once: `Priority::parse` is not a `const fn`, so the slice
        // cannot be a plain `static`. The Base Rate Generator subscribes for
        // every exhaust process at YEAR granularity, priority GENERATOR-2 // the rows recorded in `CalculatorInfo.txt`.
        static SUBS: OnceLock<Vec<CalculatorSubscription>> = OnceLock::new();
        SUBS.get_or_init(|| {
            let priority = Priority::parse("GENERATOR-2").expect("GENERATOR-2 is a valid priority");
            // Running, Start, Brakewear, Tirewear, Extended Idle, Aux Power.
            [1_u16, 2, 9, 10, 90, 91]
                .into_iter()
                .map(|process| {
                    CalculatorSubscription::new(ProcessId(process), Granularity::Year, priority)
                })
                .collect()
        })
    }

    fn input_tables(&self) -> &[&'static str] {
        INPUT_TABLES
    }

    fn output_tables(&self) -> &[&'static str] {
        OUTPUT_TABLES
    }

    fn execute(&self, ctx: &mut CalculatorContext) -> Result<CalculatorOutput, Error> {
        let pos = ctx.position();
        let process_id = pos
            .process_id
            .ok_or_else(|| Error::Polars("no process_id in iteration position".into()))
            .map(|p| p.0 as i32)?;
        let year_id = pos
            .time
            .year
            .ok_or_else(|| Error::Polars("no year in iteration position".into()))
            .map(|y| y as i32)?;

        let is_project = ctx.is_project();
        let scale = ctx.model_scale();

        // Derive behavioral flags — mirrors Java BaseRateGenerator.generateBaseRates().
        //
        // Scale mapping (Java ModelScale → Rust ModelScale):
        //   Java MACROSCALE (description="Inv")     → Rust ModelScale::Inventory
        //   Java MESOSCALE_LOOKUP (description="Rates") → Rust ModelScale::Rates
        //
        // applyAvgSpeedDistribution: Inventory (Java MACROSCALE) scale for processes 1/9/10.
        // Project domain overrides it to false later (handled via the !is_project gate on
        // use_avg_speed_bin rather than mutating the intermediate).
        let apply_avg_speed_distribution =
            matches!(scale, Some(ModelScale::Inventory)) && matches!(process_id, 1 | 9 | 10);
        // Net after Project-domain override (Java: if isProjectDomain && proc∈{1,9,10} then false):
        let apply_avg_speed_distribution = apply_avg_speed_distribution && !is_project;

        // keepOpModeID: process 2 (Start Exhaust). The Java Project-domain override only
        // fires for processes 1/9/10, so process 2 keeps keepOpModeID=true even in Project domain.
        let keep_op_mode_id = process_id == 2;

        // useAvgSpeedBin: Rates-path processes 1/9/10 (non-Project). Collapsed to 0 when
        // applyAvgSpeedDistribution is active or in Project domain.
        let use_avg_speed_bin =
            !apply_avg_speed_distribution && !is_project && matches!(process_id, 1 | 9 | 10);

        // useAvgSpeedFraction: active on the applyAvgSpeedDistribution (Inventory) path.
        let use_avg_speed_fraction = apply_avg_speed_distribution;

        // useSumSBD (applySourceBinDistribution in Java): Inventory scale or processes 2/90/91.
        // Java: applySourceBinDistribution = (scale == MACROSCALE); if process ∈ {2,90,91} →
        // applySourceBinDistribution = true. No Project-domain override for this flag.
        let use_sum_sbd =
            matches!(scale, Some(ModelScale::Inventory)) || matches!(process_id, 2 | 90 | 91);

        // useSumSBDRaw: Rates (Java MESOSCALE_LOOKUP) scale AND processes 2/90/91.
        let use_sum_sbd_raw =
            matches!(scale, Some(ModelScale::Rates)) && matches!(process_id, 2 | 90 | 91);

        // Build the 8-token parameter list that ExternalFlags::from_parameters expects,
        // mirroring brbaFlags.getCSVForExternalGenerator() + ",processID,yearID,roadTypeID".
        // roadTypeID = 0: no per-road-type filter for non-Project; the input tables carry
        // the full runSpecRoadType set which the generator iterates internally.
        let process_str = process_id.to_string();
        let year_str = year_id.to_string();
        let params: Vec<&str> = [
            if keep_op_mode_id { "yOp" } else { "nOp" },
            if use_avg_speed_bin { "yASB" } else { "nASB" },
            if use_avg_speed_fraction {
                "yASF"
            } else {
                "nASF"
            },
            if use_sum_sbd { "ySBD" } else { "nSBD" },
            if use_sum_sbd_raw { "yRaw" } else { "nRaw" },
            &process_str,
            &year_str,
            "0",
        ]
        .to_vec();

        let flags =
            ExternalFlags::from_parameters(&params).map_err(|e| Error::Polars(e.to_string()))?;

        // Source-bin-weight the raw emission rates — the port of canonical
        // `generateSBWeightedEmissionRates`. A run that already provides the
        // weighted tables (a captured snapshot, or a unit-test fixture that
        // pre-seeds them) uses them as-is; the default-DB path has no producer
        // for `SBWeighted*`, so compute them here from the runtime
        // `SourceBinDistribution` + raw rate tables (otherwise every base rate —
        // and the whole onroad inventory — is zero).
        let provided_by_age: Vec<model::SbWeightedRateDetail> =
            iter_optional(ctx.tables(), "SBWeightedEmissionRateByAge")?;
        let provided_non_age: Vec<model::SbWeightedRateDetail> =
            iter_optional(ctx.tables(), "SBWeightedEmissionRate")?;
        let (sb_weighted_by_age, sb_weighted_non_age) =
            if !provided_by_age.is_empty() || !provided_non_age.is_empty() {
                (provided_by_age, provided_non_age)
            } else {
                let computed = sbweighted::compute_sb_weighted_rates(
                    ctx.tables(),
                    i64::from(process_id),
                    i64::from(year_id),
                )?;
                (computed.by_age, computed.non_age)
            };

        let inputs = BaseRateInputs {
            avg_speed_bin: ctx.tables().iter_typed("avgSpeedBin")?,
            drive_schedule: iter_optional(ctx.tables(), "driveSchedule")?,
            avg_speed_distribution: ctx.tables().iter_typed("avgSpeedDistribution")?,
            source_use_type_physics_mapping: iter_optional(
                ctx.tables(),
                "sourceUseTypePhysicsMapping",
            )?,
            sb_weighted_emission_rate_by_age: sb_weighted_by_age,
            sb_weighted_emission_rate: sb_weighted_non_age,
            sb_weighted_distance_rate: iter_optional(ctx.tables(), "SBWeightedDistanceRate")?,
            run_spec_road_type: ctx
                .tables()
                .iter_typed::<RunSpecRoadTypeRow>("runSpecRoadType")?
                .into_iter()
                .map(|r| r.road_type_id)
                .collect(),
            run_spec_hour_day: ctx
                .tables()
                .iter_typed::<RunSpecHourDayRow>("runSpecHourDay")?
                .into_iter()
                .map(|r| r.hour_day_id)
                .collect(),
            run_spec_source_type: ctx
                .tables()
                .iter_typed::<RunSpecSourceTypeRow>("runSpecSourceType")?
                .into_iter()
                .map(|r| r.source_type_id)
                .collect(),
            run_spec_pollutant_process: ctx
                .tables()
                .iter_typed::<RunSpecPollutantProcessRow>("runSpecPollutantProcess")?
                .into_iter()
                .map(|r| r.pol_process_id)
                .collect(),
            op_mode_pol_proc_assoc: ctx.tables().iter_typed("opModePolProcAssoc")?,
            drive_schedule_assoc: iter_optional(ctx.tables(), "driveScheduleAssoc")?,
            operating_mode: iter_optional(ctx.tables(), "operatingMode")?,
            rates_op_mode_distribution: iter_optional(ctx.tables(), "RatesOpModeDistribution")?,
            drive_schedule_second: iter_optional(ctx.tables(), "driveScheduleSecond")?,
            is_project,
        };

        let output = BaseRateGenerator::run(&inputs, &flags);

        // Write output tables to scratch.
        let base_rate_df = output
            .base_rate
            .into_dataframe()
            .map_err(|e| Error::Polars(e.to_string()))?;
        let base_rate_by_age_df = output
            .base_rate_by_age
            .into_dataframe()
            .map_err(|e| Error::Polars(e.to_string()))?;
        let driving_idle_df = output
            .driving_idle_fraction
            .into_dataframe()
            .map_err(|e| Error::Polars(e.to_string()))?;

        ctx.scratch_mut().insert("BaseRate", base_rate_df);
        ctx.scratch_mut()
            .insert("BaseRateByAge", base_rate_by_age_df);
        ctx.scratch_mut()
            .insert("DrivingIdleFraction", driving_idle_df);

        Ok(CalculatorOutput::empty())
    }
}

// ── Helper: load optional table ───────────────────────────────────────────────

/// Load rows from an optional table. Returns an empty `Vec` when the table is
/// absent from the store rather than an error.
fn iter_optional<R: TableRow>(store: &InMemoryStore, name: &str) -> Result<Vec<R>, Error> {
    if store.contains(name) {
        store.iter_typed(name)
    } else {
        Ok(Vec::new())
    }
}

// ── Helper: RowExtraction error ───────────────────────────────────────────────

fn row_err(table: &'static str, row: usize, col: &'static str, msg: String) -> Error {
    Error::RowExtraction {
        table: table.into(),
        row,
        column: col.into(),
        message: msg,
    }
}

// ── Wrapper structs for Vec<i32> runSpec tables ───────────────────────────────

struct RunSpecRoadTypeRow {
    road_type_id: i32,
}

struct RunSpecHourDayRow {
    hour_day_id: i32,
}

struct RunSpecSourceTypeRow {
    source_type_id: i32,
}

struct RunSpecPollutantProcessRow {
    pol_process_id: i32,
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
        let col = df
            .column("roadTypeID")
            .map_err(|e| row_err(t, 0, "roadTypeID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "roadTypeID", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |c| row_err(t, i, c, "null value".into());
                Ok(RunSpecRoadTypeRow {
                    road_type_id: col.get(i).ok_or_else(|| null("roadTypeID"))?,
                })
            })
            .collect()
    }
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
        let col = df
            .column("hourDayID")
            .map_err(|e| row_err(t, 0, "hourDayID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "hourDayID", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |c| row_err(t, i, c, "null value".into());
                Ok(RunSpecHourDayRow {
                    hour_day_id: col.get(i).ok_or_else(|| null("hourDayID"))?,
                })
            })
            .collect()
    }
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
        let col = df
            .column("sourceTypeID")
            .map_err(|e| row_err(t, 0, "sourceTypeID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "sourceTypeID", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |c| row_err(t, i, c, "null value".into());
                Ok(RunSpecSourceTypeRow {
                    source_type_id: col.get(i).ok_or_else(|| null("sourceTypeID"))?,
                })
            })
            .collect()
    }
}

impl TableRow for RunSpecPollutantProcessRow {
    fn table_name() -> &'static str {
        "runSpecPollutantProcess"
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
        let t = "runSpecPollutantProcess";
        let col = df
            .column("polProcessID")
            .map_err(|e| row_err(t, 0, "polProcessID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "polProcessID", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |c| row_err(t, i, c, "null value".into());
                Ok(RunSpecPollutantProcessRow {
                    pol_process_id: col.get(i).ok_or_else(|| null("polProcessID"))?,
                })
            })
            .collect()
    }
}

// ── TableRow for input row types ──────────────────────────────────────────────

impl TableRow for AvgSpeedBinRow {
    fn table_name() -> &'static str {
        "avgSpeedBin"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("avgSpeedBinID".into(), DataType::Int32),
            ("avgBinSpeed".into(), DataType::Float64),
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
                        .map(|r| r.avg_speed_bin_id)
                        .collect::<Vec<i32>>(),
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
        let bin_id = get_i32("avgSpeedBinID")?;
        let bin_speed = get_f64("avgBinSpeed")?;
        (0..df.height())
            .map(|i| {
                let null = |c| row_err(t, i, c, "null value".into());
                Ok(AvgSpeedBinRow {
                    avg_speed_bin_id: bin_id.get(i).ok_or_else(|| null("avgSpeedBinID"))?,
                    avg_bin_speed: bin_speed.get(i).ok_or_else(|| null("avgBinSpeed"))?,
                })
            })
            .collect()
    }
}

impl TableRow for DriveScheduleRow {
    fn table_name() -> &'static str {
        "driveSchedule"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("driveScheduleID".into(), DataType::Int32),
            ("averageSpeed".into(), DataType::Float64),
        ])
    }

    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "driveScheduleID".into(),
                    rows.iter()
                        .map(|r| r.drive_schedule_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "averageSpeed".into(),
                    rows.iter().map(|r| r.average_speed).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "driveSchedule";
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
        let ds_id = get_i32("driveScheduleID")?;
        let avg_speed = get_f64("averageSpeed")?;
        (0..df.height())
            .map(|i| {
                let null = |c| row_err(t, i, c, "null value".into());
                Ok(DriveScheduleRow {
                    drive_schedule_id: ds_id.get(i).ok_or_else(|| null("driveScheduleID"))?,
                    average_speed: avg_speed.get(i).ok_or_else(|| null("averageSpeed"))?,
                })
            })
            .collect()
    }
}

impl TableRow for AvgSpeedDistributionRow {
    fn table_name() -> &'static str {
        "avgSpeedDistribution"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("sourceTypeID".into(), DataType::Int32),
            ("roadTypeID".into(), DataType::Int32),
            ("hourDayID".into(), DataType::Int32),
            ("avgSpeedBinID".into(), DataType::Int32),
            ("avgSpeedFraction".into(), DataType::Float64),
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
                    "roadTypeID".into(),
                    rows.iter().map(|r| r.road_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "hourDayID".into(),
                    rows.iter().map(|r| r.hour_day_id).collect::<Vec<i32>>(),
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
                    "avgSpeedFraction".into(),
                    rows.iter()
                        .map(|r| r.avg_speed_fraction)
                        .collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "avgSpeedDistribution";
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
        let src = get_i32("sourceTypeID")?;
        let road = get_i32("roadTypeID")?;
        let hd = get_i32("hourDayID")?;
        let bin = get_i32("avgSpeedBinID")?;
        let frac = get_f64("avgSpeedFraction")?;
        (0..df.height())
            .map(|i| {
                let null = |c| row_err(t, i, c, "null value".into());
                Ok(AvgSpeedDistributionRow {
                    source_type_id: src.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    road_type_id: road.get(i).ok_or_else(|| null("roadTypeID"))?,
                    hour_day_id: hd.get(i).ok_or_else(|| null("hourDayID"))?,
                    avg_speed_bin_id: bin.get(i).ok_or_else(|| null("avgSpeedBinID"))?,
                    avg_speed_fraction: frac.get(i).ok_or_else(|| null("avgSpeedFraction"))?,
                })
            })
            .collect()
    }
}

impl TableRow for SbWeightedDistanceRow {
    fn table_name() -> &'static str {
        "SBWeightedDistanceRate"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("sourceTypeID".into(), DataType::Int32),
            ("avgSpeedBinID".into(), DataType::Int32),
            ("polProcessID".into(), DataType::Int32),
            ("modelYearID".into(), DataType::Int32),
            ("fuelTypeID".into(), DataType::Int32),
            ("regClassID".into(), DataType::Int32),
            ("meanBaseRate".into(), DataType::Float64),
            ("meanBaseRateIM".into(), DataType::Float64),
            ("meanBaseRateACAdj".into(), DataType::Float64),
            ("meanBaseRateIMACAdj".into(), DataType::Float64),
            ("sumSBD".into(), DataType::Float64),
            ("sumSBDRaw".into(), DataType::Float64),
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
                    "avgSpeedBinID".into(),
                    rows.iter()
                        .map(|r| r.avg_speed_bin_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "polProcessID".into(),
                    rows.iter().map(|r| r.pol_process_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "modelYearID".into(),
                    rows.iter().map(|r| r.model_year_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "fuelTypeID".into(),
                    rows.iter().map(|r| r.fuel_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "regClassID".into(),
                    rows.iter().map(|r| r.reg_class_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "meanBaseRate".into(),
                    rows.iter().map(|r| r.mean_base_rate).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "meanBaseRateIM".into(),
                    rows.iter()
                        .map(|r| r.mean_base_rate_im)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "meanBaseRateACAdj".into(),
                    rows.iter()
                        .map(|r| r.mean_base_rate_ac_adj)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "meanBaseRateIMACAdj".into(),
                    rows.iter()
                        .map(|r| r.mean_base_rate_im_ac_adj)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "sumSBD".into(),
                    rows.iter().map(|r| r.sum_sbd).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "sumSBDRaw".into(),
                    rows.iter().map(|r| r.sum_sbd_raw).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "SBWeightedDistanceRate";
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
        let src = get_i32("sourceTypeID")?;
        let bin = get_i32("avgSpeedBinID")?;
        let pol = get_i32("polProcessID")?;
        let my = get_i32("modelYearID")?;
        let ft = get_i32("fuelTypeID")?;
        let rc = get_i32("regClassID")?;
        let mbr = get_f64("meanBaseRate")?;
        let mbri = get_f64("meanBaseRateIM")?;
        let mbra = get_f64("meanBaseRateACAdj")?;
        let mbria = get_f64("meanBaseRateIMACAdj")?;
        let sbd = get_f64("sumSBD")?;
        let sbdr = get_f64("sumSBDRaw")?;
        (0..df.height())
            .map(|i| {
                let null = |c| row_err(t, i, c, "null value".into());
                Ok(SbWeightedDistanceRow {
                    source_type_id: src.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    avg_speed_bin_id: bin.get(i).ok_or_else(|| null("avgSpeedBinID"))?,
                    pol_process_id: pol.get(i).ok_or_else(|| null("polProcessID"))?,
                    model_year_id: my.get(i).ok_or_else(|| null("modelYearID"))?,
                    fuel_type_id: ft.get(i).ok_or_else(|| null("fuelTypeID"))?,
                    reg_class_id: rc.get(i).ok_or_else(|| null("regClassID"))?,
                    mean_base_rate: mbr.get(i).ok_or_else(|| null("meanBaseRate"))?,
                    mean_base_rate_im: mbri.get(i).ok_or_else(|| null("meanBaseRateIM"))?,
                    mean_base_rate_ac_adj: mbra.get(i).ok_or_else(|| null("meanBaseRateACAdj"))?,
                    mean_base_rate_im_ac_adj: mbria
                        .get(i)
                        .ok_or_else(|| null("meanBaseRateIMACAdj"))?,
                    sum_sbd: sbd.get(i).ok_or_else(|| null("sumSBD"))?,
                    sum_sbd_raw: sbdr.get(i).ok_or_else(|| null("sumSBDRaw"))?,
                })
            })
            .collect()
    }
}

impl TableRow for OpModePolProcRow {
    fn table_name() -> &'static str {
        "opModePolProcAssoc"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
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
                    rows.iter().map(|r| r.pol_process_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "opModeID".into(),
                    rows.iter().map(|r| r.op_mode_id).collect::<Vec<i32>>(),
                )
                .into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "opModePolProcAssoc";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let pol = get_i32("polProcessID")?;
        let op = get_i32("opModeID")?;
        (0..df.height())
            .map(|i| {
                let null = |c| row_err(t, i, c, "null value".into());
                Ok(OpModePolProcRow {
                    pol_process_id: pol.get(i).ok_or_else(|| null("polProcessID"))?,
                    op_mode_id: op.get(i).ok_or_else(|| null("opModeID"))?,
                })
            })
            .collect()
    }
}

impl TableRow for DriveScheduleAssocRow {
    fn table_name() -> &'static str {
        "driveScheduleAssoc"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("sourceTypeID".into(), DataType::Int32),
            ("roadTypeID".into(), DataType::Int32),
            ("driveScheduleID".into(), DataType::Int32),
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
                    "roadTypeID".into(),
                    rows.iter().map(|r| r.road_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "driveScheduleID".into(),
                    rows.iter()
                        .map(|r| r.drive_schedule_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "driveScheduleAssoc";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let src = get_i32("sourceTypeID")?;
        let road = get_i32("roadTypeID")?;
        let ds = get_i32("driveScheduleID")?;
        (0..df.height())
            .map(|i| {
                let null = |c| row_err(t, i, c, "null value".into());
                Ok(DriveScheduleAssocRow {
                    source_type_id: src.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    road_type_id: road.get(i).ok_or_else(|| null("roadTypeID"))?,
                    drive_schedule_id: ds.get(i).ok_or_else(|| null("driveScheduleID"))?,
                })
            })
            .collect()
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
            ("avgBinSpeed".into(), DataType::Float64),
            ("avgSpeedFraction".into(), DataType::Float64),
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
                Series::new(
                    "avgBinSpeed".into(),
                    rows.iter().map(|r| r.avg_bin_speed).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "avgSpeedFraction".into(),
                    rows.iter()
                        .map(|r| r.avg_speed_fraction)
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
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .f64()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let src = get_i32("sourceTypeID")?;
        let road = get_i32("roadTypeID")?;
        let bin = get_i32("avgSpeedBinID")?;
        let hd = get_i32("hourDayID")?;
        let pol = get_i32("polProcessID")?;
        let op = get_i32("opModeID")?;
        let omf = get_f64("opModeFraction")?;
        let abs = get_f64("avgBinSpeed")?;
        // `avgSpeedFraction` is part of the canonical RatesOpModeDistribution
        // schema, but the rates-path producer
        // (RatesOperatingModeDistributionGenerator) does not populate it. Its
        // value is only consulted under the `yASF` flag; the engine never sets
        // that flag (BaseRateGenerator::execute leaves ExternalFlags at default,
        // so use_avg_speed_fraction = false and the aggregator substitutes 1.0).
        // A missing column or NULL therefore lowers to 0.0 rather than failing
        // extraction — the value is never read for computation.
        let asf = df
            .column("avgSpeedFraction")
            .ok()
            .and_then(|c| c.f64().ok());
        (0..df.height())
            .map(|i| {
                let null = |c| row_err(t, i, c, "null value".into());
                Ok(RatesOpModeDistributionRow {
                    source_type_id: src.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    road_type_id: road.get(i).ok_or_else(|| null("roadTypeID"))?,
                    avg_speed_bin_id: bin.get(i).ok_or_else(|| null("avgSpeedBinID"))?,
                    hour_day_id: hd.get(i).ok_or_else(|| null("hourDayID"))?,
                    pol_process_id: pol.get(i).ok_or_else(|| null("polProcessID"))?,
                    op_mode_id: op.get(i).ok_or_else(|| null("opModeID"))?,
                    op_mode_fraction: omf.get(i).ok_or_else(|| null("opModeFraction"))?,
                    // MOVES leaves avgBinSpeed NULL in RatesOpModeDistribution
                    // (default 0.0); treat a NULL as 0.0 rather than erroring.
                    avg_bin_speed: abs.get(i).unwrap_or(0.0),
                    avg_speed_fraction: asf.and_then(|c| c.get(i)).unwrap_or(0.0),
                })
            })
            .collect()
    }
}

impl TableRow for DriveScheduleSecondRow {
    fn table_name() -> &'static str {
        "driveScheduleSecond"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("driveScheduleID".into(), DataType::Int32),
            ("second".into(), DataType::Int32),
            ("speed".into(), DataType::Float64),
        ])
    }

    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "driveScheduleID".into(),
                    rows.iter()
                        .map(|r| r.drive_schedule_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "second".into(),
                    rows.iter().map(|r| r.second).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "speed".into(),
                    rows.iter().map(|r| r.speed).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "driveScheduleSecond";
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
        let ds_id = get_i32("driveScheduleID")?;
        let sec = get_i32("second")?;
        let spd = get_f64("speed")?;
        (0..df.height())
            .map(|i| {
                let null = |c| row_err(t, i, c, "null value".into());
                Ok(DriveScheduleSecondRow {
                    drive_schedule_id: ds_id.get(i).ok_or_else(|| null("driveScheduleID"))?,
                    second: sec.get(i).ok_or_else(|| null("second"))?,
                    speed: spd.get(i).ok_or_else(|| null("speed"))?,
                })
            })
            .collect()
    }
}

// ── TableRow for model types ──────────────────────────────────────────────────

impl TableRow for SourceUseTypePhysicsMappingDetail {
    fn table_name() -> &'static str {
        "sourceUseTypePhysicsMapping"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("realSourceTypeID".into(), DataType::Int32),
            ("tempSourceTypeID".into(), DataType::Int32),
            ("opModeIDOffset".into(), DataType::Int32),
            ("regClassID".into(), DataType::Int32),
            ("beginModelYearID".into(), DataType::Int32),
            ("endModelYearID".into(), DataType::Int32),
            ("rollingTermA".into(), DataType::Float64),
            ("rotatingTermB".into(), DataType::Float64),
            ("dragTermC".into(), DataType::Float64),
            ("sourceMass".into(), DataType::Float64),
            ("fixedMassFactor".into(), DataType::Float64),
        ])
    }

    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "realSourceTypeID".into(),
                    rows.iter()
                        .map(|r| r.real_source_type_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "tempSourceTypeID".into(),
                    rows.iter()
                        .map(|r| r.temp_source_type_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "opModeIDOffset".into(),
                    rows.iter()
                        .map(|r| r.op_mode_id_offset)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "regClassID".into(),
                    rows.iter().map(|r| r.reg_class_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "beginModelYearID".into(),
                    rows.iter()
                        .map(|r| r.begin_model_year_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "endModelYearID".into(),
                    rows.iter()
                        .map(|r| r.end_model_year_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "rollingTermA".into(),
                    rows.iter().map(|r| r.rolling_term_a).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "rotatingTermB".into(),
                    rows.iter().map(|r| r.rotating_term_b).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "dragTermC".into(),
                    rows.iter().map(|r| r.drag_term_c).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "sourceMass".into(),
                    rows.iter().map(|r| r.source_mass).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "fixedMassFactor".into(),
                    rows.iter()
                        .map(|r| r.fixed_mass_factor)
                        .collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "sourceUseTypePhysicsMapping";
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
        let rsti = get_i32("realSourceTypeID")?;
        let tsti = get_i32("tempSourceTypeID")?;
        let omio = get_i32("opModeIDOffset")?;
        let rc = get_i32("regClassID")?;
        let bmy = get_i32("beginModelYearID")?;
        let emy = get_i32("endModelYearID")?;
        let rta = get_f64("rollingTermA")?;
        let rtb = get_f64("rotatingTermB")?;
        let dtc = get_f64("dragTermC")?;
        let sm = get_f64("sourceMass")?;
        let fmf = get_f64("fixedMassFactor")?;
        (0..df.height())
            .map(|i| {
                let null = |c| row_err(t, i, c, "null value".into());
                Ok(SourceUseTypePhysicsMappingDetail {
                    real_source_type_id: rsti.get(i).ok_or_else(|| null("realSourceTypeID"))?,
                    temp_source_type_id: tsti.get(i).ok_or_else(|| null("tempSourceTypeID"))?,
                    op_mode_id_offset: omio.get(i).ok_or_else(|| null("opModeIDOffset"))?,
                    reg_class_id: rc.get(i).ok_or_else(|| null("regClassID"))?,
                    begin_model_year_id: bmy.get(i).ok_or_else(|| null("beginModelYearID"))?,
                    end_model_year_id: emy.get(i).ok_or_else(|| null("endModelYearID"))?,
                    rolling_term_a: rta.get(i).ok_or_else(|| null("rollingTermA"))?,
                    rotating_term_b: rtb.get(i).ok_or_else(|| null("rotatingTermB"))?,
                    drag_term_c: dtc.get(i).ok_or_else(|| null("dragTermC"))?,
                    source_mass: sm.get(i).ok_or_else(|| null("sourceMass"))?,
                    fixed_mass_factor: fmf.get(i).ok_or_else(|| null("fixedMassFactor"))?,
                })
            })
            .collect()
    }
}

impl TableRow for SbWeightedRateDetail {
    fn table_name() -> &'static str {
        "SBWeightedEmissionRateByAge"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("sourceTypeID".into(), DataType::Int32),
            ("polProcessID".into(), DataType::Int32),
            ("opModeID".into(), DataType::Int32),
            ("modelYearID".into(), DataType::Int32),
            ("fuelTypeID".into(), DataType::Int32),
            ("ageGroupID".into(), DataType::Int32),
            ("regClassID".into(), DataType::Int32),
            ("sumSBD".into(), DataType::Float64),
            ("sumSBDRaw".into(), DataType::Float64),
            ("meanBaseRate".into(), DataType::Float64),
            ("meanBaseRateIM".into(), DataType::Float64),
            ("meanBaseRateACAdj".into(), DataType::Float64),
            ("meanBaseRateIMACAdj".into(), DataType::Float64),
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
                    "modelYearID".into(),
                    rows.iter().map(|r| r.model_year_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "fuelTypeID".into(),
                    rows.iter().map(|r| r.fuel_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "ageGroupID".into(),
                    rows.iter().map(|r| r.age_group_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "regClassID".into(),
                    rows.iter().map(|r| r.reg_class_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sumSBD".into(),
                    rows.iter().map(|r| r.sum_sbd).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "sumSBDRaw".into(),
                    rows.iter().map(|r| r.sum_sbd_raw).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "meanBaseRate".into(),
                    rows.iter().map(|r| r.mean_base_rate).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "meanBaseRateIM".into(),
                    rows.iter()
                        .map(|r| r.mean_base_rate_im)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "meanBaseRateACAdj".into(),
                    rows.iter()
                        .map(|r| r.mean_base_rate_ac_adj)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "meanBaseRateIMACAdj".into(),
                    rows.iter()
                        .map(|r| r.mean_base_rate_im_ac_adj)
                        .collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "SBWeightedEmissionRateByAge";
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
        let src = get_i32("sourceTypeID")?;
        let pol = get_i32("polProcessID")?;
        let op = get_i32("opModeID")?;
        let my = get_i32("modelYearID")?;
        let ft = get_i32("fuelTypeID")?;
        // The non-age `SBWeightedEmissionRate` table is read with this same
        // struct but has no `ageGroupID` column; its rows carry age group 0
        // (see [`SbWeightedRateDetail`]). Treat an absent column — and any NULL
        // within it — as age group 0 rather than erroring.
        let ag = df
            .column("ageGroupID")
            .ok()
            .and_then(|c| c.i32().ok().cloned());
        let rc = get_i32("regClassID")?;
        let sbd = get_f64("sumSBD")?;
        let sbdr = get_f64("sumSBDRaw")?;
        let mbr = get_f64("meanBaseRate")?;
        let mbri = get_f64("meanBaseRateIM")?;
        let mbra = get_f64("meanBaseRateACAdj")?;
        let mbria = get_f64("meanBaseRateIMACAdj")?;
        (0..df.height())
            .map(|i| {
                let null = |c| row_err(t, i, c, "null value".into());
                Ok(SbWeightedRateDetail {
                    source_type_id: src.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    pol_process_id: pol.get(i).ok_or_else(|| null("polProcessID"))?,
                    op_mode_id: op.get(i).ok_or_else(|| null("opModeID"))?,
                    model_year_id: my.get(i).ok_or_else(|| null("modelYearID"))?,
                    fuel_type_id: ft.get(i).ok_or_else(|| null("fuelTypeID"))?,
                    age_group_id: ag.as_ref().and_then(|c| c.get(i)).unwrap_or(0),
                    reg_class_id: rc.get(i).ok_or_else(|| null("regClassID"))?,
                    sum_sbd: sbd.get(i).ok_or_else(|| null("sumSBD"))?,
                    sum_sbd_raw: sbdr.get(i).ok_or_else(|| null("sumSBDRaw"))?,
                    mean_base_rate: mbr.get(i).ok_or_else(|| null("meanBaseRate"))?,
                    mean_base_rate_im: mbri.get(i).ok_or_else(|| null("meanBaseRateIM"))?,
                    mean_base_rate_ac_adj: mbra.get(i).ok_or_else(|| null("meanBaseRateACAdj"))?,
                    mean_base_rate_im_ac_adj: mbria
                        .get(i)
                        .ok_or_else(|| null("meanBaseRateIMACAdj"))?,
                })
            })
            .collect()
    }
}

impl TableRow for OperatingMode {
    fn table_name() -> &'static str {
        "operatingMode"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("opModeID".into(), DataType::Int32),
            ("VSPLower".into(), DataType::Float64),
            ("VSPUpper".into(), DataType::Float64),
            ("speedLower".into(), DataType::Float64),
            ("speedUpper".into(), DataType::Float64),
        ])
    }

    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        let vsp_lower: Vec<Option<f64>> = rows.iter().map(|r| r.vsp_lower).collect();
        let vsp_upper: Vec<Option<f64>> = rows.iter().map(|r| r.vsp_upper).collect();
        let speed_lower: Vec<Option<f64>> = rows.iter().map(|r| r.speed_lower).collect();
        let speed_upper: Vec<Option<f64>> = rows.iter().map(|r| r.speed_upper).collect();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "opModeID".into(),
                    rows.iter().map(|r| r.op_mode_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new("VSPLower".into(), vsp_lower).into(),
                Series::new("VSPUpper".into(), vsp_upper).into(),
                Series::new("speedLower".into(), speed_lower).into(),
                Series::new("speedUpper".into(), speed_upper).into(),
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
        let get_f64_opt = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .f64()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let op_mode_id = get_i32("opModeID")?;
        let vsp_lower = get_f64_opt("VSPLower")?;
        let vsp_upper = get_f64_opt("VSPUpper")?;
        let speed_lower = get_f64_opt("speedLower")?;
        let speed_upper = get_f64_opt("speedUpper")?;
        (0..df.height())
            .map(|i| {
                let null = |c| row_err(t, i, c, "null value".into());
                Ok(OperatingMode {
                    op_mode_id: op_mode_id.get(i).ok_or_else(|| null("opModeID"))?,
                    vsp_lower: vsp_lower.get(i),
                    vsp_upper: vsp_upper.get(i),
                    speed_lower: speed_lower.get(i),
                    speed_upper: speed_upper.get(i),
                })
            })
            .collect()
    }
}

// ── TableRow for output types ─────────────────────────────────────────────────

impl TableRow for BaseRateOutputRecord {
    fn table_name() -> &'static str {
        "BaseRate"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("sourceTypeID".into(), DataType::Int32),
            ("roadTypeID".into(), DataType::Int32),
            ("avgSpeedBinID".into(), DataType::Int32),
            ("hourDayID".into(), DataType::Int32),
            ("polProcessID".into(), DataType::Int32),
            ("pollutantID".into(), DataType::Int32),
            ("processID".into(), DataType::Int32),
            ("modelYearID".into(), DataType::Int32),
            ("fuelTypeID".into(), DataType::Int32),
            ("ageGroupID".into(), DataType::Int32),
            ("regClassID".into(), DataType::Int32),
            ("opModeID".into(), DataType::Int32),
            ("meanBaseRate".into(), DataType::Float64),
            ("meanBaseRateIM".into(), DataType::Float64),
            ("meanBaseRateACAdj".into(), DataType::Float64),
            ("meanBaseRateIMACAdj".into(), DataType::Float64),
            ("emissionRate".into(), DataType::Float64),
            ("emissionRateIM".into(), DataType::Float64),
            ("emissionRateACAdj".into(), DataType::Float64),
            ("emissionRateIMACAdj".into(), DataType::Float64),
            ("opModeFraction".into(), DataType::Float64),
            ("opModeFractionRate".into(), DataType::Float64),
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
                    "pollutantID".into(),
                    rows.iter().map(|r| r.pollutant_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "processID".into(),
                    rows.iter().map(|r| r.process_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "modelYearID".into(),
                    rows.iter().map(|r| r.model_year_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "fuelTypeID".into(),
                    rows.iter().map(|r| r.fuel_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "ageGroupID".into(),
                    rows.iter().map(|r| r.age_group_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "regClassID".into(),
                    rows.iter().map(|r| r.reg_class_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "opModeID".into(),
                    rows.iter().map(|r| r.op_mode_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "meanBaseRate".into(),
                    rows.iter().map(|r| r.mean_base_rate).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "meanBaseRateIM".into(),
                    rows.iter()
                        .map(|r| r.mean_base_rate_im)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "meanBaseRateACAdj".into(),
                    rows.iter()
                        .map(|r| r.mean_base_rate_ac_adj)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "meanBaseRateIMACAdj".into(),
                    rows.iter()
                        .map(|r| r.mean_base_rate_im_ac_adj)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "emissionRate".into(),
                    rows.iter().map(|r| r.emission_rate).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "emissionRateIM".into(),
                    rows.iter()
                        .map(|r| r.emission_rate_im)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "emissionRateACAdj".into(),
                    rows.iter()
                        .map(|r| r.emission_rate_ac_adj)
                        .collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "emissionRateIMACAdj".into(),
                    rows.iter()
                        .map(|r| r.emission_rate_im_ac_adj)
                        .collect::<Vec<f64>>(),
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
                    "opModeFractionRate".into(),
                    rows.iter()
                        .map(|r| r.op_mode_fraction_rate)
                        .collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "BaseRate";
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
        let src = get_i32("sourceTypeID")?;
        let road = get_i32("roadTypeID")?;
        let bin = get_i32("avgSpeedBinID")?;
        let hd = get_i32("hourDayID")?;
        let pol = get_i32("polProcessID")?;
        let pollutant = get_i32("pollutantID")?;
        let process = get_i32("processID")?;
        let my = get_i32("modelYearID")?;
        let ft = get_i32("fuelTypeID")?;
        let ag = get_i32("ageGroupID")?;
        let rc = get_i32("regClassID")?;
        let op = get_i32("opModeID")?;
        let mbr = get_f64("meanBaseRate")?;
        let mbri = get_f64("meanBaseRateIM")?;
        let mbra = get_f64("meanBaseRateACAdj")?;
        let mbria = get_f64("meanBaseRateIMACAdj")?;
        let er = get_f64("emissionRate")?;
        let eri = get_f64("emissionRateIM")?;
        let era = get_f64("emissionRateACAdj")?;
        let eria = get_f64("emissionRateIMACAdj")?;
        let omf = get_f64("opModeFraction")?;
        let omfr = get_f64("opModeFractionRate")?;
        (0..df.height())
            .map(|i| {
                let null = |c| row_err(t, i, c, "null value".into());
                Ok(BaseRateOutputRecord {
                    source_type_id: src.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    road_type_id: road.get(i).ok_or_else(|| null("roadTypeID"))?,
                    avg_speed_bin_id: bin.get(i).ok_or_else(|| null("avgSpeedBinID"))?,
                    hour_day_id: hd.get(i).ok_or_else(|| null("hourDayID"))?,
                    pol_process_id: pol.get(i).ok_or_else(|| null("polProcessID"))?,
                    pollutant_id: pollutant.get(i).ok_or_else(|| null("pollutantID"))?,
                    process_id: process.get(i).ok_or_else(|| null("processID"))?,
                    model_year_id: my.get(i).ok_or_else(|| null("modelYearID"))?,
                    fuel_type_id: ft.get(i).ok_or_else(|| null("fuelTypeID"))?,
                    age_group_id: ag.get(i).ok_or_else(|| null("ageGroupID"))?,
                    reg_class_id: rc.get(i).ok_or_else(|| null("regClassID"))?,
                    op_mode_id: op.get(i).ok_or_else(|| null("opModeID"))?,
                    mean_base_rate: mbr.get(i).ok_or_else(|| null("meanBaseRate"))?,
                    mean_base_rate_im: mbri.get(i).ok_or_else(|| null("meanBaseRateIM"))?,
                    mean_base_rate_ac_adj: mbra.get(i).ok_or_else(|| null("meanBaseRateACAdj"))?,
                    mean_base_rate_im_ac_adj: mbria
                        .get(i)
                        .ok_or_else(|| null("meanBaseRateIMACAdj"))?,
                    emission_rate: er.get(i).ok_or_else(|| null("emissionRate"))?,
                    emission_rate_im: eri.get(i).ok_or_else(|| null("emissionRateIM"))?,
                    emission_rate_ac_adj: era.get(i).ok_or_else(|| null("emissionRateACAdj"))?,
                    emission_rate_im_ac_adj: eria
                        .get(i)
                        .ok_or_else(|| null("emissionRateIMACAdj"))?,
                    op_mode_fraction: omf.get(i).ok_or_else(|| null("opModeFraction"))?,
                    op_mode_fraction_rate: omfr.get(i).ok_or_else(|| null("opModeFractionRate"))?,
                })
            })
            .collect()
    }
}

impl TableRow for DrivingIdleFractionRow {
    fn table_name() -> &'static str {
        "DrivingIdleFraction"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("hourDayID".into(), DataType::Int32),
            ("yearID".into(), DataType::Int32),
            ("roadTypeID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("drivingIdleFraction".into(), DataType::Float64),
        ])
    }

    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "hourDayID".into(),
                    rows.iter().map(|r| r.hour_day_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "yearID".into(),
                    rows.iter().map(|r| r.year_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "roadTypeID".into(),
                    rows.iter().map(|r| r.road_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "drivingIdleFraction".into(),
                    rows.iter()
                        .map(|r| r.driving_idle_fraction)
                        .collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "DrivingIdleFraction";
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
        let hd = get_i32("hourDayID")?;
        let yr = get_i32("yearID")?;
        let road = get_i32("roadTypeID")?;
        let src = get_i32("sourceTypeID")?;
        let dif = get_f64("drivingIdleFraction")?;
        (0..df.height())
            .map(|i| {
                let null = |c| row_err(t, i, c, "null value".into());
                Ok(DrivingIdleFractionRow {
                    hour_day_id: hd.get(i).ok_or_else(|| null("hourDayID"))?,
                    year_id: yr.get(i).ok_or_else(|| null("yearID"))?,
                    road_type_id: road.get(i).ok_or_else(|| null("roadTypeID"))?,
                    source_type_id: src.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    driving_idle_fraction: dif.get(i).ok_or_else(|| null("drivingIdleFraction"))?,
                })
            })
            .collect()
    }
}

/// Generator-factory shim so the calculator registry can register it.
#[must_use]
pub fn factory() -> Box<dyn Generator> {
    Box::new(BaseRateGenerator)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generator_metadata_matches_calculator_info() {
        let generator = BaseRateGenerator;
        assert_eq!(generator.name(), "BaseRateGenerator");

        let subs = generator.subscriptions();
        assert_eq!(subs.len(), 6);
        let processes: Vec<u16> = subs.iter().map(|s| s.process_id.0).collect();
        assert_eq!(processes, vec![1, 2, 9, 10, 90, 91]);
        assert!(subs.iter().all(|s| s.granularity == Granularity::Year));
        assert!(subs.iter().all(|s| s.priority.display() == "GENERATOR-2"));

        // Generators register no (pollutant, process) output pairs.
        assert!(generator.upstream().is_empty());
        assert!(generator
            .input_tables()
            .contains(&"RatesOpModeDistribution"));
        assert_eq!(
            generator.output_tables(),
            &["BaseRate", "BaseRateByAge", "DrivingIdleFraction"]
        );
    }

    #[test]
    fn generator_is_object_safe() {
        // The registry stores generators as `Box<dyn Generator>`.
        let generators: Vec<Box<dyn Generator>> = vec![Box::new(BaseRateGenerator)];
        assert_eq!(generators[0].name(), "BaseRateGenerator");
    }

    #[test]
    fn run_on_empty_inputs_yields_empty_output() {
        let inputs = BaseRateInputs::default();
        let flags = ExternalFlags {
            process_id: 2,
            year_id: 2020,
            ..ExternalFlags::default()
        };
        let output = BaseRateGenerator::run(&inputs, &flags);
        assert!(output.base_rate.is_empty());
        assert!(output.base_rate_by_age.is_empty());
        assert!(output.driving_idle_fraction.is_empty());
    }

    #[test]
    fn execute_writes_base_rate_to_scratch() {
        use moves_framework::{
            DataFrameStore, ExecutionLocation, ExecutionTime, InMemoryStore, IterationPosition,
        };

        // Build a minimal InMemoryStore with the required non-optional tables.
        let mut store = InMemoryStore::new();

        // avgSpeedBin — one row so the inner join has a result.
        let avg_speed_bin = vec![AvgSpeedBinRow {
            avg_speed_bin_id: 1,
            avg_bin_speed: 25.0,
        }];
        store.insert(
            "avgSpeedBin",
            AvgSpeedBinRow::into_dataframe(avg_speed_bin).unwrap(),
        );

        // avgSpeedDistribution — empty (no rows needed for the test to pass).
        store.insert(
            "avgSpeedDistribution",
            AvgSpeedDistributionRow::into_dataframe(vec![]).unwrap(),
        );

        // runSpecRoadType — one entry (road type 3).
        store.insert(
            "runSpecRoadType",
            RunSpecRoadTypeRow::into_dataframe(vec![RunSpecRoadTypeRow { road_type_id: 3 }])
                .unwrap(),
        );

        // runSpecHourDay — one entry.
        store.insert(
            "runSpecHourDay",
            RunSpecHourDayRow::into_dataframe(vec![RunSpecHourDayRow { hour_day_id: 85 }]).unwrap(),
        );

        // runSpecSourceType — one entry.
        store.insert(
            "runSpecSourceType",
            RunSpecSourceTypeRow::into_dataframe(vec![RunSpecSourceTypeRow { source_type_id: 21 }])
                .unwrap(),
        );

        // runSpecPollutantProcess — one entry for process 2 (pol 101).
        store.insert(
            "runSpecPollutantProcess",
            RunSpecPollutantProcessRow::into_dataframe(vec![RunSpecPollutantProcessRow {
                pol_process_id: 202,
            }])
            .unwrap(),
        );

        // opModePolProcAssoc — one entry linking pol process to an op mode.
        store.insert(
            "opModePolProcAssoc",
            OpModePolProcRow::into_dataframe(vec![OpModePolProcRow {
                pol_process_id: 202,
                op_mode_id: 0,
            }])
            .unwrap(),
        );

        // Build a context with process 2 and year 2020.
        let pos = IterationPosition {
            iteration: 0,
            process_id: Some(ProcessId(2)),
            location: ExecutionLocation::none(),
            time: ExecutionTime::year(2020),
        };
        let mut ctx = CalculatorContext::with_position_and_tables(pos, store);

        // Execute must succeed.
        let generator = BaseRateGenerator;
        let result = generator.execute(&mut ctx);
        assert!(result.is_ok(), "execute failed: {:?}", result.err());

        // All three output tables must be present in scratch.
        assert!(
            ctx.scratch().store.contains("BaseRate"),
            "BaseRate not in scratch"
        );
        assert!(
            ctx.scratch().store.contains("BaseRateByAge"),
            "BaseRateByAge not in scratch"
        );
        assert!(
            ctx.scratch().store.contains("DrivingIdleFraction"),
            "DrivingIdleFraction not in scratch"
        );
    }

    /// Build a minimal InMemoryStore with the required non-optional tables for execute().
    fn minimal_store_for_process(process_id: i32) -> InMemoryStore {
        let mut store = InMemoryStore::new();
        store.insert(
            "avgSpeedBin",
            AvgSpeedBinRow::into_dataframe(vec![AvgSpeedBinRow {
                avg_speed_bin_id: 1,
                avg_bin_speed: 25.0,
            }])
            .unwrap(),
        );
        store.insert(
            "avgSpeedDistribution",
            AvgSpeedDistributionRow::into_dataframe(vec![]).unwrap(),
        );
        store.insert(
            "runSpecRoadType",
            RunSpecRoadTypeRow::into_dataframe(vec![RunSpecRoadTypeRow { road_type_id: 3 }])
                .unwrap(),
        );
        store.insert(
            "runSpecHourDay",
            RunSpecHourDayRow::into_dataframe(vec![RunSpecHourDayRow { hour_day_id: 85 }]).unwrap(),
        );
        store.insert(
            "runSpecSourceType",
            RunSpecSourceTypeRow::into_dataframe(vec![RunSpecSourceTypeRow { source_type_id: 21 }])
                .unwrap(),
        );
        let pol_process_id = process_id * 100 + process_id;
        store.insert(
            "runSpecPollutantProcess",
            RunSpecPollutantProcessRow::into_dataframe(vec![RunSpecPollutantProcessRow {
                pol_process_id,
            }])
            .unwrap(),
        );
        store.insert(
            "opModePolProcAssoc",
            OpModePolProcRow::into_dataframe(vec![OpModePolProcRow {
                pol_process_id,
                op_mode_id: 0,
            }])
            .unwrap(),
        );
        store
    }

    /// execute() sets ExternalFlags.keep_op_mode_id=true for process 2 (Start Exhaust)
    /// and useSumSBDRaw=true on a Rates run — mirrors Java BaseRateGenerator.generateBaseRates().
    /// keepOpModeID is process 2 regardless of domain (the Java Project-domain override only
    /// applies to processes 1/9/10).
    #[test]
    fn execute_flags_process2_rates_keep_op_mode_id_and_sum_sbd_raw() {
        use moves_framework::{ExecutionLocation, ExecutionTime, IterationPosition, ModelScale};

        let store = minimal_store_for_process(2);
        let pos = IterationPosition {
            iteration: 0,
            process_id: Some(ProcessId(2)),
            location: ExecutionLocation::none(),
            time: ExecutionTime::year(2020),
        };
        let mut ctx = CalculatorContext::with_position_and_tables(pos, store);
        ctx.set_model_scale(ModelScale::Rates);
        let generator = BaseRateGenerator;
        let result = generator.execute(&mut ctx);
        assert!(result.is_ok(), "execute failed: {:?}", result.err());
        // With process 2 on a Rates run, keepOpModeID=true and useSumSBDRaw=true.
        // Flag correctness is validated in the numerical baserategenerator.rs integration tests.
        assert!(ctx.scratch().store.contains("BaseRate"));
    }

    /// execute() propagates is_project=true from the context into BaseRateInputs,
    /// forcing the ROMD path even for process 1 (Running Exhaust).
    #[test]
    fn execute_project_domain_forces_romd_path_for_process1() {
        use moves_framework::{
            ExecutionLocation, ExecutionTime, IterationPosition, ModelDomain, ModelScale,
        };

        let mut store = minimal_store_for_process(1);
        // Add minimal RatesOpModeDistribution so the ROMD path produces output.
        store.insert(
            "RatesOpModeDistribution",
            RatesOpModeDistributionRow::into_dataframe(vec![RatesOpModeDistributionRow {
                source_type_id: 21,
                road_type_id: 3,
                avg_speed_bin_id: 1,
                hour_day_id: 85,
                pol_process_id: 101,
                op_mode_id: 0,
                op_mode_fraction: 1.0,
                avg_bin_speed: 25.0,
                avg_speed_fraction: 1.0,
            }])
            .unwrap(),
        );
        let pos = IterationPosition {
            iteration: 0,
            process_id: Some(ProcessId(1)),
            location: ExecutionLocation::none(),
            time: ExecutionTime::year(2020),
        };
        let mut ctx = CalculatorContext::with_position_and_tables(pos, store);
        ctx.set_model_scale(ModelScale::Rates);
        // Project domain: is_project=true → drive-cycle path is skipped even for
        // process 1 (Running Exhaust). execute() must complete without error.
        ctx.set_model_domain(Some(ModelDomain::Project));
        assert!(ctx.is_project(), "context must report is_project=true");
        let generator = BaseRateGenerator;
        let result = generator.execute(&mut ctx);
        assert!(
            result.is_ok(),
            "execute failed in Project domain: {:?}",
            result.err()
        );
        assert!(ctx.scratch().store.contains("BaseRate"));
    }

    /// execute() uses use_avg_speed_bin=true for process 1 on a Rates run (non-Project),
    /// meaning the drive-cycle fast path is taken.
    #[test]
    fn execute_flags_process1_rates_use_avg_speed_bin() {
        use moves_framework::{ExecutionLocation, ExecutionTime, IterationPosition, ModelScale};

        let store = minimal_store_for_process(1);
        let pos = IterationPosition {
            iteration: 0,
            process_id: Some(ProcessId(1)),
            location: ExecutionLocation::none(),
            time: ExecutionTime::year(2020),
        };
        let mut ctx = CalculatorContext::with_position_and_tables(pos, store);
        ctx.set_model_scale(ModelScale::Rates);
        let generator = BaseRateGenerator;
        // On Rates scale, process 1, non-Project: drive-cycle path is taken.
        // This exercises the branch where use_avg_speed_bin=true.
        let result = generator.execute(&mut ctx);
        assert!(result.is_ok(), "execute failed: {:?}", result.err());
        assert!(ctx.scratch().store.contains("BaseRate"));
    }

    /// context.is_project() reflects the ModelDomain set by the engine.
    #[test]
    fn context_is_project_reflects_domain() {
        use moves_framework::ModelDomain;
        let mut ctx = CalculatorContext::new();
        assert!(!ctx.is_project(), "default context is not project domain");
        ctx.set_model_domain(Some(ModelDomain::Project));
        assert!(
            ctx.is_project(),
            "Project domain → is_project() must be true"
        );
        ctx.set_model_domain(Some(ModelDomain::Default));
        assert!(
            !ctx.is_project(),
            "Default domain → is_project() must be false"
        );
        ctx.set_model_domain(None);
        assert!(
            !ctx.is_project(),
            "None domain → is_project() must be false"
        );
    }

    /// context.parameters() starts empty and round-trips through set_parameters().
    #[test]
    fn context_parameters_round_trip() {
        let mut ctx = CalculatorContext::new();
        assert!(
            ctx.parameters().is_empty(),
            "default parameters must be empty"
        );
        let tokens = vec!["yOp".to_string(), "nASB".to_string()];
        ctx.set_parameters(tokens.clone());
        assert_eq!(ctx.parameters(), tokens.as_slice());
    }
}
