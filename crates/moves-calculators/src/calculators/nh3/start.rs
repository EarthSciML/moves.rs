//! `NH3StartCalculator` — the start-exhaust ammonia calculator of Phase 3
//! Task 66 (the running-exhaust half is [`super::running`]).
//!
//! Pure-Rust port of `NH3StartCalculator.java` and the "Processing" section
//! of `database/NH3StartCalculator.sql`.
//!
//! # What this calculator does
//!
//! `NH3StartCalculator` produces ammonia (NH3, pollutant 30) emissions for
//! the **Start Exhaust** process (process 2). The Java class is a thin
//! `GenericCalculatorBase` subclass: its constructor passes the single
//! `polProcessID` `"3002"` (pollutant 30, process 2), a `YEAR` master-loop
//! granularity, a zero priority offset and the script
//! `database/NH3StartCalculator.sql`. It also implements
//! `MasterLoopContext.IContextFilter` — see
//! [`Nh3StartCalculator::processes_road_type`].
//!
//! It follows the *activity × I/M-blended base rate* methodology: an
//! age-resolved base emission rate is weighted by the source-bin distribution,
//! weighted by the operating-mode distribution, multiplied by engine-start
//! activity, and finally blended between its non-I/M and I/M variants by the
//! inspection-and-maintenance coverage fraction. The Ammonia calculator
//! carries **no** fuel-formulation, temperature, AC or humidity effects (the
//! SQL says so in its header comment).
//!
//! # The pipeline — `NH3StartCalculator.sql` "Processing" section
//!
//! The script's processing section runs four steps, labelled `NH3SEC` in the
//! SQL:
//!
//! 1. **`NH3SEC 1` — merge I/M coverage.** Build the per-`(polProcess,
//!    modelYear, fuelType, sourceType)` I/M adjustment fraction. Identical to
//!    `NH3REC 1`; ported by the shared [`merge_im_coverage`].
//! 2. **`NH3SEC-2` — weight by source bin.** Sum `meanBaseRate ×
//!    sourceBinActivityFraction` over source bins. Ported by the shared
//!    [`weight_by_source_bin`]; this calculator then cross-joins the RunSpec
//!    months and hours (the SQL's `SourceBinEmissionRates`).
//! 3. **`NH3SEC-3` — weight by operating mode.** Sum `meanBaseRate ×
//!    opModeFraction` over operating modes, resolving the `dayID` dimension
//!    by joining `HourDay` on `hourID`. Ported by `weight_by_op_mode`.
//! 4. **`NH3SEC-4` — multiply by activity.** Build `Starts2` (engine starts
//!    keyed by `modelYearID = yearID - ageID`), join it to the weighted rate
//!    and `PollutantProcessAssoc`, and form `emissionQuant = meanBaseRate ×
//!    starts` (and the parallel `emissionQuantIM`). Ported by
//!    `multiply_by_activity`.
//!
//! The closing `-- Apply IM` `UPDATE` blends `emissionQuant` with
//! `emissionQuantIM`; that is the shared [`finalize_with_im`].
//!
//! The script's "Create Remote Tables" / "Extract Data" / "Cleanup" sections
//! are MariaDB I/O boilerplate. The Rust port receives the tables already
//! materialised as [`StartInputs`]; only the computation is ported.
//!
//! # Iteration geography
//!
//! Engine starts are zone-level activity. The SQL stamps
//! `##context.iterLocation.…##` constants — the run year, state, county,
//! zone and link — onto the working tables; `ActivityWeightedEmissionRate`
//! carries the iteration `zoneID`, which threads through to the `Starts2`
//! join. The output `linkID` is the iteration link and the output
//! `roadTypeID` is a literal `1` (off-network); both come from
//! [`StartContext`].
//!
//! # Chain metadata — a superseded calculator
//!
//! `NH3StartCalculator` is a **legacy calculator superseded by
//! `BaseRateCalculator`** (migration-plan Task 45). `CalculatorInfo.txt`
//! registers `Ammonia (NH3)` on Start Exhaust to `BaseRateCalculator`, not to
//! this calculator; `characterization/calculator-chains/calculator-dag.json`
//! records `NH3StartCalculator` with `registrations_count: 0` and
//! `depends_on: []` to match.
//!
//! Consequently [`registrations`](Calculator::registrations) returns an empty
//! slice — registering `(30, 2)` here too would double-register the pair
//! against `BaseRateCalculator`. The DAG still records a single subscription
//! — `subscribes_directly: true`, granularity `YEAR`, priority
//! `EMISSION_CALCULATOR` — but with a placeholder `process_id` of `0`;
//! [`subscriptions`](Calculator::subscriptions) resolves it to process 2.
//!
//! # Fidelity notes
//!
//! The fidelity notes of [`super::running`] apply unchanged: the SQL stores
//! intermediate rates in `FLOAT` columns while MariaDB evaluates in `DOUBLE`,
//! and this port computes in [`f64`] end to end without reproducing the
//! inter-step truncation (a sub-`1e-7` relative drift left to the Task 73/74
//! fidelity gate). The SQL has no integer/integer literal division.
//!
//! # Data plane (Task 50)
//!
//! [`Calculator::execute`] is a shell: its [`CalculatorContext`] exposes only
//! the Phase 2 placeholder `ExecutionTables` / `ScratchNamespace`. The
//! faithful pipeline is [`Nh3StartCalculator::calculate`], fully unit-tested.
//! Once the `DataFrameStore` (migration-plan Task 50) lands, `execute`
//! materialises a [`StartInputs`] and a [`StartContext`] from the context,
//! calls `calculate`, and writes the [`EmissionRow`]s back.

use std::collections::HashMap;
use std::sync::OnceLock;

use moves_calculator_info::{Granularity, Priority};
use moves_data::{PollutantProcessAssociation, ProcessId};
use moves_framework::{
    Calculator, CalculatorContext, CalculatorOutput, CalculatorSubscription, DataFrameStoreTyped,
    Error, TableRow,
};
use polars::prelude::{DataFrame, DataType, NamedFrom, PolarsResult, Schema, Series};

use super::common::{
    finalize_with_im, merge_im_coverage, weight_by_source_bin, AgeCategoryRow,
    EmissionRateByAgeRow, EmissionRow, HourDayRow, ImCoverageRow, ImFactorRow,
    OpModeDistributionRow, PollutantProcessAssocRow, PollutantProcessMappedModelYearRow,
    RunSpecHourIdRow, RunSpecMonthIdRow, SourceBinDistributionRow, SourceBinEmissionRate,
    SourceBinRow, SourceTypeModelYearRow,
};

/// Chain-DAG name — matches the Java class and the `calculator-dag.json`
/// entry.
const CALCULATOR_NAME: &str = "NH3StartCalculator";

/// Start Exhaust — the one process this calculator covers. The Java
/// `polProcessID` string `"3002"` decodes to `pollutant 30 × 100 + process 2`.
const START_EXHAUST_PROCESS_ID: u16 = 2;

/// Off-network road type — `RoadType` row 1. Engine starts are modelled as
/// off-network activity, so the SQL writes a literal `1` for the output
/// `roadTypeID`, and the Java `doesProcessContext` runs the calculator only
/// here. See [`Nh3StartCalculator::processes_road_type`].
const OFF_NETWORK_ROAD_TYPE_ID: i32 = 1;

// ===========================================================================
// Input tables specific to the start calculator. The shared emission-rate,
// source-bin and I/M tables are defined in `super::common`.
// ===========================================================================

/// One `Starts` row — start-exhaust activity for a `(hourDay, month, year,
/// age, zone, sourceType)` cell. Engine starts are zone-level, not link-level.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct StartsRow {
    /// `hourDayID` — joins to [`HourDayRow::hour_day_id`].
    pub hour_day_id: i32,
    /// `monthID` — calendar month.
    pub month_id: i32,
    /// `yearID` — calendar year.
    pub year_id: i32,
    /// `ageID` — vehicle age in years; `modelYearID = yearID - ageID`.
    pub age_id: i32,
    /// `zoneID` — the zone the starts occur in.
    pub zone_id: i32,
    /// `sourceTypeID` — MOVES source (vehicle) type.
    pub source_type_id: i32,
    /// `starts` — number of engine starts.
    pub starts: f64,
}

/// The fully materialised inputs to [`Nh3StartCalculator::calculate`] — the
/// tables the SQL's "Extract Data" section produces, as plain row vectors.
///
/// A future Task 50 (`DataFrameStore`) wiring populates this from the per-run
/// filtered execution database; until then it is the explicit data-plane
/// contract the unit tests build directly.
#[derive(Debug, Clone, Default)]
pub struct StartInputs {
    /// `Starts` rows — the start-exhaust activity.
    pub starts: Vec<StartsRow>,
    /// `RunSpecMonth` — the calendar months the run covers.
    pub runspec_months: Vec<i32>,
    /// `RunSpecHour` — the hours of day the run covers.
    pub runspec_hours: Vec<i32>,
    /// `EmissionRateByAge` rows.
    pub emission_rate_by_age: Vec<EmissionRateByAgeRow>,
    /// `AgeCategory` rows.
    pub age_category: Vec<AgeCategoryRow>,
    /// `SourceTypeModelYear` rows.
    pub source_type_model_year: Vec<SourceTypeModelYearRow>,
    /// `SourceBinDistribution` rows.
    pub source_bin_distribution: Vec<SourceBinDistributionRow>,
    /// `SourceBin` rows.
    pub source_bin: Vec<SourceBinRow>,
    /// `OpModeDistribution` rows.
    pub op_mode_distribution: Vec<OpModeDistributionRow>,
    /// `HourDay` rows.
    pub hour_day: Vec<HourDayRow>,
    /// `PollutantProcessAssoc` rows.
    pub pollutant_process_assoc: Vec<PollutantProcessAssocRow>,
    /// `PollutantProcessMappedModelYear` rows (for the I/M merge).
    pub pollutant_process_mapped_model_year: Vec<PollutantProcessMappedModelYearRow>,
    /// `IMFactor` rows (for the I/M merge).
    pub im_factor: Vec<ImFactorRow>,
    /// `IMCoverage` rows (for the I/M merge), already filtered to
    /// `useIMyn = 'Y'` by the SQL's "Extract Data" section.
    pub im_coverage: Vec<ImCoverageRow>,
}

/// The iteration-geography constants the SQL stamps from
/// `##context.iterLocation.…##` and `##context.year##`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StartContext {
    /// `##context.year##` — the calendar year of the run.
    pub year_id: i32,
    /// `##context.iterLocation.stateRecordID##` — the output `stateID`.
    pub state_id: i32,
    /// `##context.iterLocation.countyRecordID##` — the output `countyID`, and
    /// the county the I/M merge filters `IMCoverage` to.
    pub county_id: i32,
    /// `##context.iterLocation.zoneRecordID##` — the iteration zone, joined
    /// against each `Starts` row's `zoneID`.
    pub zone_id: i32,
    /// `##context.iterLocation.linkRecordID##` — the output `linkID`.
    pub link_id: i32,
}

/// One `ActivityWeightedEmissionRate` row — a base rate weighted by both
/// source bin and operating mode, the result of `weight_by_op_mode`
/// (`NH3SEC-3`).
///
/// The SQL's working table also carries the constant `zoneID`/`yearID`; those
/// come from [`StartContext`] and are not stored per row.
#[derive(Debug, Clone, Copy, PartialEq)]
struct ActivityWeightedRate {
    month_id: i32,
    pol_process_id: i32,
    source_type_id: i32,
    model_year_id: i32,
    fuel_type_id: i32,
    day_id: i32,
    hour_id: i32,
    mean_base_rate: f64,
    mean_base_rate_im: f64,
}

/// `ActivityWeightedEmissionRate` content keyed for the `Starts2` join — the
/// `(pollutant, process, fuelType)` resolution plus the two weighted rates.
#[derive(Debug, Clone, Copy, PartialEq)]
struct ActivityWeightedEntry {
    pollutant_id: i32,
    process_id: i32,
    fuel_type_id: i32,
    mean_base_rate: f64,
    mean_base_rate_im: f64,
}

fn row_err(table: &'static str, row: usize, column: &'static str, msg: String) -> Error {
    Error::RowExtraction {
        table: table.into(),
        row,
        column: column.into(),
        message: msg,
    }
}

impl TableRow for StartsRow {
    fn table_name() -> &'static str {
        "Starts"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("hourDayID".into(), DataType::Int32),
            ("monthID".into(), DataType::Int32),
            ("yearID".into(), DataType::Int32),
            ("ageID".into(), DataType::Int32),
            ("zoneID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("starts".into(), DataType::Float64),
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
                    "monthID".into(),
                    rows.iter().map(|r| r.month_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "yearID".into(),
                    rows.iter().map(|r| r.year_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "ageID".into(),
                    rows.iter().map(|r| r.age_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "zoneID".into(),
                    rows.iter().map(|r| r.zone_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "starts".into(),
                    rows.iter().map(|r| r.starts).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "Starts";
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
        let hour_day_id = get_i32("hourDayID")?;
        let month_id = get_i32("monthID")?;
        let year_id = get_i32("yearID")?;
        let age_id = get_i32("ageID")?;
        let zone_id = get_i32("zoneID")?;
        let source_type_id = get_i32("sourceTypeID")?;
        let starts = get_f64("starts")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(StartsRow {
                    hour_day_id: hour_day_id.get(i).ok_or_else(|| null("hourDayID"))?,
                    month_id: month_id.get(i).ok_or_else(|| null("monthID"))?,
                    year_id: year_id.get(i).ok_or_else(|| null("yearID"))?,
                    age_id: age_id.get(i).ok_or_else(|| null("ageID"))?,
                    zone_id: zone_id.get(i).ok_or_else(|| null("zoneID"))?,
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    starts: starts.get(i).ok_or_else(|| null("starts"))?,
                })
            })
            .collect()
    }
}

/// The MOVES start-exhaust ammonia calculator.
///
/// A zero-sized value type: the calculator owns no per-run state, exactly as
/// the [`Calculator`] trait requires. All run-varying input flows through
/// [`Nh3StartCalculator::calculate`].
#[derive(Debug, Clone, Copy, Default)]
pub struct Nh3StartCalculator;

impl Nh3StartCalculator {
    /// Chain-DAG name — matches the Java class and the `calculator-dag.json`
    /// entry.
    pub const NAME: &'static str = CALCULATOR_NAME;

    /// Construct the calculator.
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    /// Whether the master loop should run this calculator for the given road
    /// type — the port of `NH3StartCalculator.doesProcessContext`.
    ///
    /// `NH3StartCalculator` implements `MasterLoopContext.IContextFilter`:
    /// start-exhaust emissions live on the off-network road type, so the Java
    /// predicate rejects a context whose `roadTypeRecordID` is a positive,
    /// non-off-network id. An absent road type (`<= 0`) still passes — the
    /// filter only excludes a *known* on-network road type.
    #[must_use]
    pub fn processes_road_type(road_type_id: i32) -> bool {
        !(road_type_id > 0 && road_type_id != OFF_NETWORK_ROAD_TYPE_ID)
    }

    /// Run the calculator over a fully materialised set of input tables.
    ///
    /// Chains the four `NH3SEC` processing steps of `NH3StartCalculator.sql`
    /// and returns the `MOVESWorkerOutput` rows the SQL would insert, sorted
    /// by their dimension columns for deterministic output.
    #[must_use]
    pub fn calculate(inputs: &StartInputs, ctx: &StartContext) -> Vec<EmissionRow> {
        // NH3SEC 1: merge I/M coverage.
        let merged = merge_im_coverage(
            ctx.year_id,
            ctx.county_id,
            &inputs.pollutant_process_mapped_model_year,
            &inputs.im_factor,
            &inputs.age_category,
            &inputs.im_coverage,
            &inputs.pollutant_process_assoc,
        );
        // NH3SEC-2: weight by source bin.
        let source_bin_rates = weight_by_source_bin(
            ctx.year_id,
            &inputs.emission_rate_by_age,
            &inputs.age_category,
            &inputs.source_type_model_year,
            &inputs.source_bin_distribution,
            &inputs.source_bin,
        );
        // NH3SEC-3: weight by operating mode (and cross-join months/hours).
        let activity_weighted = weight_by_op_mode(&source_bin_rates, inputs);
        // NH3SEC-4: multiply by Starts activity.
        let rows_with_im = multiply_by_activity(&activity_weighted, inputs, ctx);
        // -- Apply IM.
        finalize_with_im(rows_with_im, &merged)
    }
}

/// `weight_by_op_mode`'s `NH3SEC-3` `GROUP BY` key — `(polProcessID,
/// sourceTypeID, modelYearID, fuelTypeID, dayID, hourID)`.
type OpModeGroupKey = (i32, i32, i32, i32, i32, i32);

/// `NH3SEC-2`'s `SourceBinEmissionRates` cross-join with the RunSpec months
/// and hours, then `NH3SEC-3`'s operating-mode weighting.
///
/// `SourceBinEmissionRates` cross-joins each [`SourceBinEmissionRate`] with
/// every `(month, hour)` pair of the RunSpec; `NH3SEC-3` then sums
/// `meanBaseRate × opModeFraction` over operating mode, after joining
/// `HourDay` on `hourID` (which expands the row by `dayID`) and
/// `OpModeDistribution` on `(sourceTypeID, hourDayID, polProcessID,
/// opModeID)`. The `GROUP BY` keys on `(zoneID, yearID, monthID, dayID,
/// hourID, polProcessID, sourceTypeID, modelYearID, fuelTypeID)`.
///
/// The month is an independent dimension the `GROUP BY` carries unchanged, so
/// this port weights by operating mode first — iterating the RunSpec hours,
/// which the `HourDay`/`OpModeDistribution` joins do depend on — and expands
/// the months afterwards.
fn weight_by_op_mode(
    source_bin_rates: &[SourceBinEmissionRate],
    inputs: &StartInputs,
) -> Vec<ActivityWeightedRate> {
    // OpModeDistribution indexed by the join key. Unlike the running
    // calculator, the start `NH3SEC-3` join carries no `linkID`.
    let mut omd_by: HashMap<(i32, i32, i32, i32), Vec<&OpModeDistributionRow>> = HashMap::new();
    for omd in &inputs.op_mode_distribution {
        omd_by
            .entry((
                omd.source_type_id,
                omd.hour_day_id,
                omd.pol_process_id,
                omd.op_mode_id,
            ))
            .or_default()
            .push(omd);
    }
    // HourDay indexed by hourID — a single hour spans several day types.
    let mut hour_day_by_hour: HashMap<i32, Vec<&HourDayRow>> = HashMap::new();
    for hd in &inputs.hour_day {
        hour_day_by_hour.entry(hd.hour_id).or_default().push(hd);
    }

    // NH3SEC-3: sum opModeFraction × meanBaseRate over operating mode,
    // grouped by (polProcess, sourceType, modelYear, fuelType, dayID, hourID).
    let mut acc: HashMap<OpModeGroupKey, (f64, f64)> = HashMap::new();
    for sber in source_bin_rates {
        for &hour_id in &inputs.runspec_hours {
            // INNER JOIN HourDay ON hourID — expands the row by day type.
            let Some(hour_days) = hour_day_by_hour.get(&hour_id) else {
                continue;
            };
            for hd in hour_days {
                // INNER JOIN OpModeDistribution USING (sourceTypeID,
                // hourDayID, polProcessID, opModeID).
                let Some(omds) = omd_by.get(&(
                    sber.source_type_id,
                    hd.hour_day_id,
                    sber.pol_process_id,
                    sber.op_mode_id,
                )) else {
                    continue;
                };
                for omd in omds {
                    let entry = acc
                        .entry((
                            sber.pol_process_id,
                            sber.source_type_id,
                            sber.model_year_id,
                            sber.fuel_type_id,
                            hd.day_id,
                            hour_id,
                        ))
                        .or_insert((0.0, 0.0));
                    entry.0 += sber.mean_base_rate * omd.op_mode_fraction;
                    entry.1 += sber.mean_base_rate_im * omd.op_mode_fraction;
                }
            }
        }
    }

    // Cross-join the RunSpec months.
    let mut keys: Vec<&OpModeGroupKey> = acc.keys().collect();
    keys.sort_unstable();
    let mut out: Vec<ActivityWeightedRate> = Vec::new();
    for key in keys {
        let &(pol_process_id, source_type_id, model_year_id, fuel_type_id, day_id, hour_id) = key;
        let (mean_base_rate, mean_base_rate_im) = acc[key];
        for &month_id in &inputs.runspec_months {
            out.push(ActivityWeightedRate {
                month_id,
                pol_process_id,
                source_type_id,
                model_year_id,
                fuel_type_id,
                day_id,
                hour_id,
                mean_base_rate,
                mean_base_rate_im,
            });
        }
    }
    out
}

/// `NH3SEC-4` — multiply the operating-mode-weighted rates by `Starts`
/// activity.
///
/// Ports the SQL's `Starts2`/`MOVESWorkerOutput` chain:
///
/// * `Starts2` joins `Starts` to `HourDay` and derives `modelYearID =
///   yearID - ageID`;
/// * the final insert joins `Starts2`, `ActivityWeightedEmissionRate` (on
///   `zoneID, monthID, hourID, dayID, yearID, sourceTypeID, modelYearID`) and
///   `PollutantProcessAssoc` (on `polProcessID`), forming
///   `emissionQuant = meanBaseRate × starts` (and `emissionQuantIM`
///   likewise).
///
/// The weighted rate carries the constant iteration `zoneID`/`yearID`; the
/// join keeps only `Starts` rows in that zone and year. The output `linkID`
/// is the iteration link and `roadTypeID` is the literal off-network `1`.
/// Returns each row paired with its `emissionQuantIM` for the
/// [`finalize_with_im`] blend.
fn multiply_by_activity(
    activity_weighted: &[ActivityWeightedRate],
    inputs: &StartInputs,
    ctx: &StartContext,
) -> Vec<(EmissionRow, f64)> {
    // PollutantProcessAssoc lookup — resolves polProcessID.
    let ppa: HashMap<i32, &PollutantProcessAssocRow> = inputs
        .pollutant_process_assoc
        .iter()
        .map(|r| (r.pol_process_id, r))
        .collect();
    // ActivityWeightedEmissionRate indexed by the Starts2 join key. The
    // constant zoneID/yearID are checked per `Starts` row below, so the key
    // is (monthID, hourID, dayID, sourceTypeID, modelYearID).
    let mut awer_by: HashMap<(i32, i32, i32, i32, i32), Vec<ActivityWeightedEntry>> =
        HashMap::new();
    for awr in activity_weighted {
        let Some(assoc) = ppa.get(&awr.pol_process_id) else {
            continue;
        };
        awer_by
            .entry((
                awr.month_id,
                awr.hour_id,
                awr.day_id,
                awr.source_type_id,
                awr.model_year_id,
            ))
            .or_default()
            .push(ActivityWeightedEntry {
                pollutant_id: assoc.pollutant_id,
                process_id: assoc.process_id,
                fuel_type_id: awr.fuel_type_id,
                mean_base_rate: awr.mean_base_rate,
                mean_base_rate_im: awr.mean_base_rate_im,
            });
    }

    let hour_day: HashMap<i32, &HourDayRow> =
        inputs.hour_day.iter().map(|r| (r.hour_day_id, r)).collect();
    let mut out: Vec<(EmissionRow, f64)> = Vec::new();
    for st in &inputs.starts {
        // Starts2: INNER JOIN HourDay USING (hourDayID).
        let Some(hd) = hour_day.get(&st.hour_day_id) else {
            continue;
        };
        let model_year_id = st.year_id - st.age_id;
        // The Starts2 ⋈ ActivityWeightedEmissionRate join requires
        // s.zoneID = awer.zoneID (the constant iteration zone) and
        // s.yearID = awer.yearID (the constant run year).
        if st.zone_id != ctx.zone_id || st.year_id != ctx.year_id {
            continue;
        }
        let Some(entries) = awer_by.get(&(
            st.month_id,
            hd.hour_id,
            hd.day_id,
            st.source_type_id,
            model_year_id,
        )) else {
            continue;
        };
        for entry in entries {
            let row = EmissionRow {
                year_id: st.year_id,
                month_id: st.month_id,
                day_id: hd.day_id,
                hour_id: hd.hour_id,
                state_id: ctx.state_id,
                county_id: ctx.county_id,
                zone_id: st.zone_id,
                link_id: ctx.link_id,
                pollutant_id: entry.pollutant_id,
                process_id: entry.process_id,
                source_type_id: st.source_type_id,
                fuel_type_id: entry.fuel_type_id,
                model_year_id,
                road_type_id: OFF_NETWORK_ROAD_TYPE_ID,
                emission_quant: entry.mean_base_rate * st.starts,
            };
            out.push((row, entry.mean_base_rate_im * st.starts));
        }
    }
    out
}

/// The calculator's single master-loop subscription.
///
/// `GenericCalculatorBase.subscribeToMe` subscribes once per process the
/// calculator's `polProcessID`s span; `"3002"` is process 2 (Start Exhaust),
/// so there is exactly one subscription, at `YEAR` granularity and
/// `EMISSION_CALCULATOR` priority (the Java constructor passes a zero priority
/// offset). `calculator-dag.json` records the granularity and priority but a
/// placeholder `process_id` of `0`, because the static analyser cannot
/// resolve `GenericCalculatorBase`'s runtime `polProcessID` lookup — the true
/// process id (2) comes from the constructor.
fn subscriptions() -> &'static [CalculatorSubscription] {
    static SUBS: OnceLock<[CalculatorSubscription; 1]> = OnceLock::new();
    SUBS.get_or_init(|| {
        let priority = Priority::parse("EMISSION_CALCULATOR")
            .expect("\"EMISSION_CALCULATOR\" is a valid MasterLoop priority");
        [CalculatorSubscription::new(
            ProcessId(START_EXHAUST_PROCESS_ID),
            Granularity::Year,
            priority,
        )]
    })
}

/// The `(pollutant, process)` pairs the calculator registers — **none**.
///
/// `NH3StartCalculator` is superseded by `BaseRateCalculator` (see the
/// module-level supersession note): it is absent from `CalculatorInfo.txt`
/// and `calculator-dag.json` records `registrations_count: 0`. NH3 Start
/// Exhaust `(30, 2)` is registered to `BaseRateCalculator`, so registering it
/// here too would double-register the pair in the calculator registry.
static REGISTRATIONS: &[PollutantProcessAssociation] = &[];

/// Default-DB tables `NH3StartCalculator.sql`'s processing pass reads.
///
/// The script's extract section also pulls `County`, `criteriaRatio`,
/// `FuelFormulation`, `FuelSubType`, `FuelSupply`, `FuelType`,
/// `MonthOfAnyYear`, `StartTempAdjustment`, `Year`, `Zone` and
/// `ZoneMonthHour`, none of which the processing pass consumes (the Ammonia
/// calculator carries no fuel, temperature or humidity effects); they are
/// omitted here.
static INPUT_TABLES: &[&str] = &[
    "AgeCategory",
    "EmissionRateByAge",
    "HourDay",
    "IMCoverage",
    "IMFactor",
    "OpModeDistribution",
    "PollutantProcessAssoc",
    "PollutantProcessMappedModelYear",
    "RunSpecHour",
    "RunSpecMonth",
    "SourceBin",
    "SourceBinDistribution",
    "SourceTypeModelYear",
    "Starts",
];

impl Calculator for Nh3StartCalculator {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn subscriptions(&self) -> &[CalculatorSubscription] {
        subscriptions()
    }

    /// `NH3StartCalculator` registers **no** `(pollutant, process)` pairs —
    /// see `REGISTRATIONS` and the module-level supersession note.
    fn registrations(&self) -> &[PollutantProcessAssociation] {
        REGISTRATIONS
    }

    // `upstream` keeps the trait default (empty): `calculator-dag.json`
    // records no `depends_on` edges for `NH3StartCalculator`.

    fn input_tables(&self) -> &[&'static str] {
        INPUT_TABLES
    }

    fn execute(&self, ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
        let tables = ctx.tables();
        let pos = ctx.position();
        let start_ctx = StartContext {
            year_id: pos.time.year.map(|y| y as i32).unwrap_or(0),
            state_id: pos.location.state_id.map(|s| s as i32).unwrap_or(0),
            county_id: pos.location.county_id.map(|c| c as i32).unwrap_or(0),
            zone_id: pos.location.zone_id.map(|z| z as i32).unwrap_or(0),
            link_id: pos.location.link_id.map(|l| l as i32).unwrap_or(0),
        };
        let inputs = StartInputs {
            starts: tables.iter_typed::<StartsRow>("Starts")?,
            runspec_months: tables
                .iter_typed::<RunSpecMonthIdRow>("RunSpecMonth")?
                .into_iter()
                .map(|r| r.month_id)
                .collect(),
            runspec_hours: tables
                .iter_typed::<RunSpecHourIdRow>("RunSpecHour")?
                .into_iter()
                .map(|r| r.hour_id)
                .collect(),
            emission_rate_by_age: tables.iter_typed::<EmissionRateByAgeRow>("EmissionRateByAge")?,
            age_category: tables.iter_typed::<AgeCategoryRow>("AgeCategory")?,
            source_type_model_year: tables
                .iter_typed::<SourceTypeModelYearRow>("SourceTypeModelYear")?,
            source_bin_distribution: tables
                .iter_typed::<SourceBinDistributionRow>("SourceBinDistribution")?,
            source_bin: tables.iter_typed::<SourceBinRow>("SourceBin")?,
            op_mode_distribution: tables
                .iter_typed::<OpModeDistributionRow>("OpModeDistribution")?,
            hour_day: tables.iter_typed::<HourDayRow>("HourDay")?,
            pollutant_process_assoc: tables
                .iter_typed::<PollutantProcessAssocRow>("PollutantProcessAssoc")?,
            pollutant_process_mapped_model_year: tables
                .iter_typed::<PollutantProcessMappedModelYearRow>(
                    "PollutantProcessMappedModelYear",
                )?,
            im_factor: tables.iter_typed::<ImFactorRow>("IMFactor")?,
            im_coverage: tables.iter_typed::<ImCoverageRow>("IMCoverage")?,
        };
        let rows = Nh3StartCalculator::calculate(&inputs, &start_ctx);
        crate::wiring::emit_rows(rows)
    }
}

/// Construct the calculator as a boxed trait object — matches the engine's
/// calculator-factory signature so the registry can register it.
#[must_use]
pub fn factory() -> Box<dyn Calculator> {
    Box::new(Nh3StartCalculator::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::calculators::nh3::common::NH3_POLLUTANT_ID;

    /// NH3 Start Exhaust `polProcessID` — `pollutant 30 × 100 + process 2`.
    const NH3_START_POL_PROCESS: i32 = 3002;

    /// A one-`Starts`, one-bin, one-operating-mode start input with no I/M
    /// coverage. The single output row is
    /// `emissionQuant = starts 10 × (opModeFraction 1 × (sbaf 1 ×
    /// meanBaseRate 3)) = 30`.
    fn minimal_inputs() -> StartInputs {
        StartInputs {
            starts: vec![StartsRow {
                hour_day_id: 85,
                month_id: 7,
                year_id: 2020,
                age_id: 2, // modelYearID = 2020 - 2 = 2018
                zone_id: 261_610,
                source_type_id: 21,
                starts: 10.0,
            }],
            runspec_months: vec![7],
            runspec_hours: vec![8],
            emission_rate_by_age: vec![EmissionRateByAgeRow {
                source_bin_id: 1000,
                pol_process_id: NH3_START_POL_PROCESS,
                op_mode_id: 100,
                age_group_id: 3,
                mean_base_rate: 3.0,
                mean_base_rate_im: 1.0,
            }],
            age_category: vec![AgeCategoryRow {
                age_id: 2,
                age_group_id: 3,
            }],
            source_type_model_year: vec![SourceTypeModelYearRow {
                source_type_model_year_id: 212_018,
                model_year_id: 2018,
                source_type_id: 21,
            }],
            source_bin_distribution: vec![SourceBinDistributionRow {
                source_type_model_year_id: 212_018,
                pol_process_id: NH3_START_POL_PROCESS,
                source_bin_id: 1000,
                source_bin_activity_fraction: 1.0,
            }],
            source_bin: vec![SourceBinRow {
                source_bin_id: 1000,
                fuel_type_id: 1,
            }],
            op_mode_distribution: vec![OpModeDistributionRow {
                source_type_id: 21,
                hour_day_id: 85,
                link_id: 5001,
                pol_process_id: NH3_START_POL_PROCESS,
                op_mode_id: 100,
                op_mode_fraction: 1.0,
            }],
            hour_day: vec![HourDayRow {
                hour_day_id: 85,
                day_id: 5,
                hour_id: 8,
            }],
            pollutant_process_assoc: vec![PollutantProcessAssocRow {
                pol_process_id: NH3_START_POL_PROCESS,
                process_id: 2,
                pollutant_id: NH3_POLLUTANT_ID,
            }],
            pollutant_process_mapped_model_year: Vec::new(),
            im_factor: Vec::new(),
            im_coverage: Vec::new(),
        }
    }

    fn ctx() -> StartContext {
        StartContext {
            year_id: 2020,
            state_id: 26,
            county_id: 26_161,
            zone_id: 261_610,
            link_id: 5001,
        }
    }

    #[test]
    fn calculate_multiplies_starts_by_the_weighted_rate() {
        let out = Nh3StartCalculator::calculate(&minimal_inputs(), &ctx());
        assert_eq!(out.len(), 1);
        let row = out[0];
        assert_eq!(row.pollutant_id, NH3_POLLUTANT_ID);
        assert_eq!(row.process_id, 2);
        assert_eq!(row.source_type_id, 21);
        assert_eq!(row.fuel_type_id, 1);
        assert_eq!(row.model_year_id, 2018);
        assert_eq!(row.year_id, 2020);
        assert_eq!(row.month_id, 7);
        assert_eq!(row.day_id, 5);
        assert_eq!(row.hour_id, 8);
        assert_eq!(row.state_id, 26);
        assert_eq!(row.county_id, 26_161);
        assert_eq!(row.zone_id, 261_610);
        assert_eq!(row.link_id, 5001);
        // Start exhaust is reported on the off-network road type.
        assert_eq!(row.road_type_id, 1);
        // starts 10 × opModeFraction 1 × (sbaf 1 × meanBaseRate 3) = 30.
        assert!((row.emission_quant - 30.0).abs() < 1e-9);
    }

    #[test]
    fn calculate_blends_in_the_im_quantity_where_coverage_exists() {
        let mut inputs = minimal_inputs();
        // emissionQuant = 30 (meanBaseRate 3 × starts 10),
        // emissionQuantIM = 10 (meanBaseRateIM 1 × starts 10).
        inputs.pollutant_process_mapped_model_year = vec![PollutantProcessMappedModelYearRow {
            pol_process_id: NH3_START_POL_PROCESS,
            model_year_id: 2018,
            im_model_year_group_id: 7,
        }];
        inputs.im_factor = vec![ImFactorRow {
            pol_process_id: NH3_START_POL_PROCESS,
            inspect_freq: 1,
            test_standards_id: 11,
            source_type_id: 21,
            fuel_type_id: 1,
            im_model_year_group_id: 7,
            age_group_id: 3,
            im_factor: 50.0,
        }];
        inputs.im_coverage = vec![ImCoverageRow {
            pol_process_id: NH3_START_POL_PROCESS,
            county_id: 26_161,
            year_id: 2020,
            source_type_id: 21,
            fuel_type_id: 1,
            inspect_freq: 1,
            test_standards_id: 11,
            beg_model_year_id: 2000,
            end_model_year_id: 2025,
            compliance_factor: 80.0,
        }];

        let out = Nh3StartCalculator::calculate(&inputs, &ctx());
        assert_eq!(out.len(), 1);
        // IMAdjustFract = 50 × 80 × 0.01 = 40. Blend = 10 × 40 + 30 ×
        // (1 - 40) = 400 - 1170 = -770 → GREATEST(…, 0) = 0.
        assert!((out[0].emission_quant - 0.0).abs() < 1e-9);
    }

    #[test]
    fn calculate_drops_starts_in_another_zone() {
        let mut inputs = minimal_inputs();
        inputs.starts[0].zone_id = 999_999; // not the iteration zone
        assert!(Nh3StartCalculator::calculate(&inputs, &ctx()).is_empty());
    }

    #[test]
    fn calculate_drops_starts_whose_hour_day_is_unknown() {
        let mut inputs = minimal_inputs();
        inputs.starts[0].hour_day_id = 999; // no HourDay row
        assert!(Nh3StartCalculator::calculate(&inputs, &ctx()).is_empty());
    }

    #[test]
    fn calculate_expands_one_rate_across_every_runspec_month() {
        let mut inputs = minimal_inputs();
        inputs.runspec_months = vec![6, 7, 8];
        inputs.starts = vec![6, 7, 8]
            .into_iter()
            .map(|month_id| StartsRow {
                hour_day_id: 85,
                month_id,
                year_id: 2020,
                age_id: 2,
                zone_id: 261_610,
                source_type_id: 21,
                starts: 10.0,
            })
            .collect();

        let out = Nh3StartCalculator::calculate(&inputs, &ctx());
        assert_eq!(out.len(), 3);
        assert_eq!(
            out.iter().map(|r| r.month_id).collect::<Vec<_>>(),
            [6, 7, 8]
        );
        for row in &out {
            assert!((row.emission_quant - 30.0).abs() < 1e-9);
        }
    }

    #[test]
    fn processes_road_type_admits_off_network_and_absent_road_types() {
        // Off-network and "no road type" pass; a known on-network type fails.
        assert!(Nh3StartCalculator::processes_road_type(1));
        assert!(Nh3StartCalculator::processes_road_type(0));
        assert!(Nh3StartCalculator::processes_road_type(-1));
        assert!(!Nh3StartCalculator::processes_road_type(2));
        assert!(!Nh3StartCalculator::processes_road_type(5));
    }

    #[test]
    fn subscriptions_are_start_exhaust_at_year_granularity() {
        let calc = Nh3StartCalculator::new();
        let subs = calc.subscriptions();
        assert_eq!(subs.len(), 1);
        assert_eq!(subs[0].process_id, ProcessId(START_EXHAUST_PROCESS_ID));
        assert_eq!(subs[0].granularity, Granularity::Year);
    }

    #[test]
    fn registrations_are_empty_because_base_rate_calculator_supersedes_this() {
        assert!(Nh3StartCalculator::new().registrations().is_empty());
    }

    #[test]
    fn execute_wires_through_data_plane() {
        use moves_framework::execution::execution_db::{
            ExecutionLocation, ExecutionTime, IterationPosition,
        };
        use moves_framework::{DataFrameStore, InMemoryStore};
        let inputs = minimal_inputs();
        let ctx_vals = ctx();
        let mut store = InMemoryStore::new();
        store.insert("Starts", StartsRow::into_dataframe(inputs.starts).unwrap());
        store.insert(
            "RunSpecMonth",
            RunSpecMonthIdRow::into_dataframe(
                inputs
                    .runspec_months
                    .iter()
                    .map(|&month_id| RunSpecMonthIdRow { month_id })
                    .collect(),
            )
            .unwrap(),
        );
        store.insert(
            "RunSpecHour",
            RunSpecHourIdRow::into_dataframe(
                inputs
                    .runspec_hours
                    .iter()
                    .map(|&hour_id| RunSpecHourIdRow { hour_id })
                    .collect(),
            )
            .unwrap(),
        );
        store.insert(
            "EmissionRateByAge",
            EmissionRateByAgeRow::into_dataframe(inputs.emission_rate_by_age).unwrap(),
        );
        store.insert(
            "AgeCategory",
            AgeCategoryRow::into_dataframe(inputs.age_category).unwrap(),
        );
        store.insert(
            "SourceTypeModelYear",
            SourceTypeModelYearRow::into_dataframe(inputs.source_type_model_year).unwrap(),
        );
        store.insert(
            "SourceBinDistribution",
            SourceBinDistributionRow::into_dataframe(inputs.source_bin_distribution).unwrap(),
        );
        store.insert(
            "SourceBin",
            SourceBinRow::into_dataframe(inputs.source_bin).unwrap(),
        );
        store.insert(
            "OpModeDistribution",
            OpModeDistributionRow::into_dataframe(inputs.op_mode_distribution).unwrap(),
        );
        store.insert(
            "HourDay",
            HourDayRow::into_dataframe(inputs.hour_day).unwrap(),
        );
        store.insert(
            "PollutantProcessAssoc",
            PollutantProcessAssocRow::into_dataframe(inputs.pollutant_process_assoc).unwrap(),
        );
        store.insert(
            "PollutantProcessMappedModelYear",
            PollutantProcessMappedModelYearRow::into_dataframe(
                inputs.pollutant_process_mapped_model_year,
            )
            .unwrap(),
        );
        store.insert(
            "IMFactor",
            ImFactorRow::into_dataframe(inputs.im_factor).unwrap(),
        );
        store.insert(
            "IMCoverage",
            ImCoverageRow::into_dataframe(inputs.im_coverage).unwrap(),
        );
        let position = IterationPosition {
            iteration: 0,
            process_id: None,
            location: ExecutionLocation::link(
                ctx_vals.state_id as u32,
                ctx_vals.county_id as u32,
                ctx_vals.zone_id as u32,
                ctx_vals.link_id as u32,
            ),
            time: ExecutionTime::year(ctx_vals.year_id as u16),
        };
        let ctx = CalculatorContext::with_position_and_tables(position, store);
        let out = Nh3StartCalculator::new().execute(&ctx).expect("execute ok");
        let df = out.dataframe().expect("expected non-empty DataFrame");
        assert_eq!(
            df.height(),
            1,
            "minimal inputs produce exactly one NH3 start row"
        );
        let quant = df
            .column("emissionQuant")
            .unwrap()
            .f64()
            .unwrap()
            .get(0)
            .unwrap();
        // starts 10 × opModeFraction 1 × (sbaf 1 × meanBaseRate 3) = 30.
        assert!((quant - 30.0).abs() < 1e-9, "emissionQuant {quant} != 30.0");
    }

    #[test]
    fn factory_builds_a_calculator_named_for_the_java_class() {
        assert_eq!(factory().name(), "NH3StartCalculator");
    }
}
