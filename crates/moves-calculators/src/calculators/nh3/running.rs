//! `NH3RunningCalculator` — the running-exhaust ammonia calculator of
//! (the start-exhaust half is [`super::start`]).
//!
//! Pure-Rust port of `NH3RunningCalculator.java` and the "Processing" section
//! of `database/NH3RunningCalculator.sql`.
//!
//! # What this calculator does
//!
//! `NH3RunningCalculator` produces ammonia (NH3, pollutant 30) emissions for
//! the **Running Exhaust** process (process 1). The Java class is a thin
//! `GenericCalculatorBase` subclass: its whole body is the constructor, which
//! passes the single `polProcessID` `"3001"` (pollutant 30, process 1), a
//! `YEAR` master-loop granularity, a zero priority offset and the script
//! `database/NH3RunningCalculator.sql`.
//!
//! It follows the *activity × I/M-blended base rate* methodology: an
//! age-resolved base emission rate is weighted by the source-bin distribution,
//! weighted by the operating-mode distribution, multiplied by source-hours-
//! operating activity, and finally blended between its non-I/M and I/M
//! variants by the inspection-and-maintenance coverage fraction. The Ammonia
//! calculator carries **no** fuel-formulation, temperature, AC or humidity
//! effects (the SQL says so in its header comment).
//!
//! # The pipeline — `NH3RunningCalculator.sql` "Processing" section
//!
//! The script's processing section runs four steps, labelled `NH3REC` in the
//! SQL:
//!
//! 1. **`NH3REC 1` — merge I/M coverage.** Build the per-`(polProcess,
//! modelYear, fuelType, sourceType)` I/M adjustment fraction. Ported by the
//! shared [`merge_im_coverage`].
//! 2. **`NH3REC-2` — weight by source bin.** Sum `meanBaseRate ×
//! sourceBinActivityFraction` over source bins. Ported by the shared
//! [`weight_by_source_bin`]; this calculator then cross-joins the RunSpec
//! months (the SQL's `SBWeightedEmissionRate`).
//! 3. **`NH3REC-3` — weight by operating mode.** Sum `meanBaseRate ×
//! opModeFraction` over operating modes, resolving the `hourDay` dimension
//! from `OpModeDistribution`. Ported by `fully_weight`.
//! 4. **`NH3REC 4` — multiply by activity.** Multiply the fully weighted rate
//! by `SHO` (source hours operating) to give `emissionQuant` (and the
//! parallel `emissionQuantIM`), then join `Link` for the output geography.
//! Ported by `multiply_by_activity`.
//!
//! The closing `-- Apply IM` `UPDATE` blends `emissionQuant` with
//! `emissionQuantIM`; that is the shared [`finalize_with_im`].
//!
//! The script's "Create Remote Tables" / "Extract Data" / "Cleanup" sections
//! are MariaDB I/O boilerplate — they load the per-iteration filtered tables
//! and drop the temporaries. The Rust port receives those tables already
//! materialised as [`RunningInputs`]; only the computation is ported.
//!
//! # Iteration geography
//!
//! The SQL stamps `##context.iterLocation.…##` constants onto the working
//! tables: the run year, state, county and link. `SourceBinEmissionRates0`
//! carries the iteration `linkID`, which threads through to the
//! `OpModeDistribution` join (`USING (…, linkID, …)`) and the final `Link`
//! join. These constants are supplied by [`RunningContext`]; the SQL also
//! stamps a `zoneID` that no later step reads, so it is not modelled. The
//! output `zoneID`/`roadTypeID` come from the `Link` table row for the
//! iteration link.
//!
//! # Chain metadata — a superseded calculator
//!
//! `NH3RunningCalculator` is a **legacy calculator superseded by
//! `BaseRateCalculator`** (). `CalculatorInfo.txt` — the
//! pinned runtime registration file — registers `Ammonia (NH3)` on Running
//! Exhaust to `BaseRateCalculator`, not to this calculator;
//! `characterization/calculator-chains/calculator-dag.json` records
//! `NH3RunningCalculator` with `registrations_count: 0` and `depends_on: []`
//! to match.
//!
//! Consequently [`registrations`](Calculator::registrations) returns an empty
//! slice — registering `(30, 1)` here too would double-register the pair
//! against `BaseRateCalculator`. The DAG still records a single subscription
//! `subscribes_directly: true`, granularity `YEAR`, priority
//! `EMISSION_CALCULATOR` — but with a placeholder `process_id` of `0`, since
//! the static analyser could not resolve the `GenericCalculatorBase`
//! `polProcessID` string; [`subscriptions`](Calculator::subscriptions)
//! resolves it to process 1.
//!
//! # Fidelity notes
//!
//! * **`FLOAT` intermediate columns.** Every `NH3REC` working table stores
//! its rates in `FLOAT` (32-bit) columns while MariaDB evaluates in
//! `DOUBLE`. This port computes in [`f64`] end to end and does not
//! reproduce the inter-step truncation — a sub-`1e-7` relative drift, left
//! to the/74 calculator-integration-validation gate (the
//! `CH4N2ORunningStartCalculator` precedent).
//! * **No integer division.** The SQL has no integer/integer literal
//! division, so the MariaDB `div_precision_increment` rounding gotcha does
//! not arise.
//! * **Sum order.** The `GROUP BY` steps carry `ORDER BY NULL`, leaving the
//! `f64` accumulation order undefined; the port accumulates in input-row
//! order, deterministic for fixed input.
//!
//! # Data plane
//!
//! [`Calculator::execute`] is a shell: its [`CalculatorContext`] exposes only
//! the placeholder `ExecutionTables` / `ScratchNamespace`, which have
//! no row storage. The faithful pipeline is [`Nh3RunningCalculator::calculate`],
//! fully unit-tested. Once the `DataFrameStore` ()
//! lands, `execute` materialises a [`RunningInputs`] and a [`RunningContext`]
//! from the context, calls `calculate`, and writes the [`EmissionRow`]s back.

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
    RunSpecMonthIdRow, SourceBinDistributionRow, SourceBinEmissionRate, SourceBinRow,
    SourceTypeModelYearRow,
};

/// Chain-DAG name — matches the Java class and the `calculator-dag.json`
/// entry.
const CALCULATOR_NAME: &str = "NH3RunningCalculator";

/// Running Exhaust — the one process this calculator covers. The Java
/// `polProcessID` string `"3001"` decodes to `pollutant 30 × 100 + process 1`.
const RUNNING_EXHAUST_PROCESS_ID: u16 = 1;

// ===========================================================================
// Input tables specific to the running calculator. The shared emission-rate,
// source-bin and I/M tables are defined in `super::common`.
// ===========================================================================

/// One `SHO` (Source Hours Operating) row — running-exhaust activity for a
/// `(hourDay, month, year, age, sourceType)` cell.
///
/// `SHO` is extract-filtered to the iteration link, so it carries no `linkID`
/// of its own; the SQL's `SHO2` working table likewise drops it. The output
/// `linkID` comes from the iteration link of [`RunningContext`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ShoRow {
    /// `hourDayID` — joins to [`HourDayRow::hour_day_id`].
    pub hour_day_id: i32,
    /// `monthID` — calendar month.
    pub month_id: i32,
    /// `yearID` — calendar year.
    pub year_id: i32,
    /// `ageID` — vehicle age in years; `modelYearID = yearID - ageID`.
    pub age_id: i32,
    /// `sourceTypeID` — MOVES source (vehicle) type.
    pub source_type_id: i32,
    /// `SHO` — source hours operating.
    pub sho: f64,
}

/// One `Link` row — supplies the output `zoneID` and `roadTypeID` for the
/// iteration link.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LinkRow {
    /// `linkID` — the link primary key.
    pub link_id: i32,
    /// `zoneID` — the zone the link belongs to.
    pub zone_id: i32,
    /// `roadTypeID` — road type of the link.
    pub road_type_id: i32,
}

/// The fully materialised inputs to [`Nh3RunningCalculator::calculate`] — the
/// tables the SQL's "Extract Data" section produces, as plain row vectors.
///
/// A future (`DataFrameStore`) wiring populates this from the per-run
/// filtered execution database; until then it is the explicit data-plane
/// contract the unit tests build directly.
#[derive(Debug, Clone, Default)]
pub struct RunningInputs {
    /// `SHO` rows — the running-exhaust activity.
    pub sho: Vec<ShoRow>,
    /// `Link` rows.
    pub link: Vec<LinkRow>,
    /// `RunSpecMonth` — the calendar months the run covers.
    pub runspec_months: Vec<i32>,
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
pub struct RunningContext {
    /// `##context.year##` — the calendar year of the run.
    pub year_id: i32,
    /// `##context.iterLocation.stateRecordID##` — the output `stateID`.
    pub state_id: i32,
    /// `##context.iterLocation.countyRecordID##` — the output `countyID`, and
    /// the county the I/M merge filters `IMCoverage` to.
    pub county_id: i32,
    /// `##context.iterLocation.linkRecordID##` — the iteration link, which
    /// gates the `OpModeDistribution` and `Link` joins.
    pub link_id: i32,
}

/// One `FullyWeightedEmissionRate` row — a base rate weighted by both source
/// bin and operating mode, the result of `fully_weight` (`NH3REC-3`).
///
/// The SQL's working table also carries the constant `zoneID`/`linkID`/
/// `yearID`; those come from [`RunningContext`] and are not stored per row.
#[derive(Debug, Clone, Copy, PartialEq)]
struct FullyWeightedRate {
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

/// `WeightedAndAdjustedEmissionRate3` content keyed for the `SHO3` join — the
/// `(pollutant, process, fuelType)` resolution plus the two weighted rates.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Waer3Entry {
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

impl TableRow for ShoRow {
    fn table_name() -> &'static str {
        "SHO"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("hourDayID".into(), DataType::Int32),
            ("monthID".into(), DataType::Int32),
            ("yearID".into(), DataType::Int32),
            ("ageID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
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
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
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
        let t = "SHO";
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
        let source_type_id = get_i32("sourceTypeID")?;
        let sho = get_f64("SHO")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(ShoRow {
                    hour_day_id: hour_day_id.get(i).ok_or_else(|| null("hourDayID"))?,
                    month_id: month_id.get(i).ok_or_else(|| null("monthID"))?,
                    year_id: year_id.get(i).ok_or_else(|| null("yearID"))?,
                    age_id: age_id.get(i).ok_or_else(|| null("ageID"))?,
                    source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    sho: sho.get(i).ok_or_else(|| null("SHO"))?,
                })
            })
            .collect()
    }
}

impl TableRow for LinkRow {
    fn table_name() -> &'static str {
        "Link"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("linkID".into(), DataType::Int32),
            ("zoneID".into(), DataType::Int32),
            ("roadTypeID".into(), DataType::Int32),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "linkID".into(),
                    rows.iter().map(|r| r.link_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "zoneID".into(),
                    rows.iter().map(|r| r.zone_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "roadTypeID".into(),
                    rows.iter().map(|r| r.road_type_id).collect::<Vec<i32>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "Link";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let link_id = get_i32("linkID")?;
        let zone_id = get_i32("zoneID")?;
        let road_type_id = get_i32("roadTypeID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(LinkRow {
                    link_id: link_id.get(i).ok_or_else(|| null("linkID"))?,
                    zone_id: zone_id.get(i).ok_or_else(|| null("zoneID"))?,
                    road_type_id: road_type_id.get(i).ok_or_else(|| null("roadTypeID"))?,
                })
            })
            .collect()
    }
}

/// The MOVES running-exhaust ammonia calculator.
///
/// A zero-sized value type: the calculator owns no per-run state, exactly as
/// the [`Calculator`] trait requires. All run-varying input flows through
/// [`Nh3RunningCalculator::calculate`].
#[derive(Debug, Clone, Copy, Default)]
pub struct Nh3RunningCalculator;

impl Nh3RunningCalculator {
    /// Chain-DAG name — matches the Java class and the `calculator-dag.json`
    /// entry.
    pub const NAME: &'static str = CALCULATOR_NAME;

    /// Construct the calculator.
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    /// Run the calculator over a fully materialised set of input tables.
    ///
    /// Chains the four `NH3REC` processing steps of
    /// `NH3RunningCalculator.sql` and returns the `MOVESWorkerOutput` rows the
    /// SQL would insert, sorted by their dimension columns for deterministic
    /// output.
    #[must_use]
    pub fn calculate(inputs: &RunningInputs, ctx: &RunningContext) -> Vec<EmissionRow> {
        // NH3REC 1: merge I/M coverage.
        let merged = merge_im_coverage(
            ctx.year_id,
            ctx.county_id,
            &inputs.pollutant_process_mapped_model_year,
            &inputs.im_factor,
            &inputs.age_category,
            &inputs.im_coverage,
            &inputs.pollutant_process_assoc,
        );
        // NH3REC-2: weight by source bin.
        let source_bin_rates = weight_by_source_bin(
            ctx.year_id,
            &inputs.emission_rate_by_age,
            &inputs.age_category,
            &inputs.source_type_model_year,
            &inputs.source_bin_distribution,
            &inputs.source_bin,
        );
        // NH3REC-3: weight by operating mode (and cross-join the months).
        let fully_weighted = fully_weight(&source_bin_rates, inputs, ctx);
        // NH3REC 4: multiply by SHO activity, join Link.
        let rows_with_im = multiply_by_activity(&fully_weighted, inputs, ctx);
        // -- Apply IM.
        finalize_with_im(rows_with_im, &merged)
    }
}

/// `fully_weight`'s `NH3REC-3` `GROUP BY` key — `(polProcessID, sourceTypeID,
/// modelYearID, fuelTypeID, hourDayID)`.
type OpModeGroupKey = (i32, i32, i32, i32, i32);

/// `NH3REC-2`'s `SBWeightedEmissionRate` cross-join with the RunSpec months,
/// then `NH3REC-3`'s operating-mode weighting.
///
/// `SBWeightedEmissionRate` cross-joins each [`SourceBinEmissionRate`] with
/// every RunSpec month; `NH3REC-3` then sums `meanBaseRate × opModeFraction`
/// over operating mode, grouped by `(…, monthID, …, hourDayID)`. The month is
/// an independent dimension the `GROUP BY` carries unchanged, so this port
/// weights by operating mode first and expands the months afterwards — the
/// two commute.
///
/// The `OpModeDistribution` join is `USING (polProcessID, sourceTypeID,
/// linkID, opModeID)`; `SBWeightedEmissionRate.linkID` is the constant
/// iteration link, so the join keeps only `OpModeDistribution` rows for that
/// link. The SQL's follow-up `UPDATE` resolves `dayID`/`hourID` from
/// `HourDay`; a `hourDayID` absent from `HourDay` would be dropped by the
/// next step's inner join, so it is dropped here.
fn fully_weight(
    source_bin_rates: &[SourceBinEmissionRate],
    inputs: &RunningInputs,
    ctx: &RunningContext,
) -> Vec<FullyWeightedRate> {
    // OpModeDistribution for the iteration link, indexed by the join key.
    let mut omd_by: HashMap<(i32, i32, i32), Vec<&OpModeDistributionRow>> = HashMap::new();
    for omd in &inputs.op_mode_distribution {
        if omd.link_id != ctx.link_id {
            continue;
        }
        omd_by
            .entry((omd.pol_process_id, omd.source_type_id, omd.op_mode_id))
            .or_default()
            .push(omd);
    }
    let hour_day: HashMap<i32, &HourDayRow> =
        inputs.hour_day.iter().map(|r| (r.hour_day_id, r)).collect();

    // NH3REC-3: sum opModeFraction × meanBaseRate over operating mode,
    // grouped by (polProcess, sourceType, modelYear, fuelType, hourDayID).
    let mut acc: HashMap<OpModeGroupKey, (f64, f64)> = HashMap::new();
    for sber in source_bin_rates {
        let Some(omds) = omd_by.get(&(sber.pol_process_id, sber.source_type_id, sber.op_mode_id))
        else {
            continue;
        };
        for omd in omds {
            let entry = acc
                .entry((
                    sber.pol_process_id,
                    sber.source_type_id,
                    sber.model_year_id,
                    sber.fuel_type_id,
                    omd.hour_day_id,
                ))
                .or_insert((0.0, 0.0));
            entry.0 += omd.op_mode_fraction * sber.mean_base_rate;
            entry.1 += omd.op_mode_fraction * sber.mean_base_rate_im;
        }
    }

    // Resolve dayID/hourID from HourDay, then cross-join the RunSpec months.
    let mut out: Vec<FullyWeightedRate> = Vec::new();
    let mut keys: Vec<&OpModeGroupKey> = acc.keys().collect();
    keys.sort_unstable();
    for key in keys {
        let &(pol_process_id, source_type_id, model_year_id, fuel_type_id, hour_day_id) = key;
        let Some(hd) = hour_day.get(&hour_day_id) else {
            continue;
        };
        let (mean_base_rate, mean_base_rate_im) = acc[key];
        for &month_id in &inputs.runspec_months {
            out.push(FullyWeightedRate {
                month_id,
                pol_process_id,
                source_type_id,
                model_year_id,
                fuel_type_id,
                day_id: hd.day_id,
                hour_id: hd.hour_id,
                mean_base_rate,
                mean_base_rate_im,
            });
        }
    }
    out
}

/// `WeightedAndAdjustedEmissionRate3` indexed for the `SHO3` join — each
/// `(yearID, monthID, dayID, hourID, sourceTypeID, modelYearID)` maps to its
/// [`Waer3Entry`] rows.
type Waer3Index = HashMap<(i32, i32, i32, i32, i32, i32), Vec<Waer3Entry>>;

/// `NH3REC 4` — multiply the fully weighted rates by `SHO` activity and join
/// `Link` for the output geography.
///
/// Ports the SQL's `SHO2`/`WeightedAndAdjustedEmissionRate3`/`SHO3`/
/// `MOVESWorkerOutput` chain:
///
/// * `SHO2` joins `SHO` to `HourDay` and derives `modelYearID = yearID -
/// ageID`;
/// * `WeightedAndAdjustedEmissionRate3` resolves `(pollutantID, processID)`
/// from `PollutantProcessAssoc`;
/// * `SHO3` joins `SHO2` to `WeightedAndAdjustedEmissionRate3` on
/// `(yearID, monthID, dayID, hourID, sourceTypeID, modelYearID)`, forming
/// `emissionQuant = SHO × meanBaseRate` (and `emissionQuantIM` likewise);
/// * the final insert joins `Link` for the iteration link's
/// `zoneID`/`roadTypeID`.
///
/// Returns each row paired with its `emissionQuantIM` for the [`finalize_with_im`]
/// blend.
fn multiply_by_activity(
    fully_weighted: &[FullyWeightedRate],
    inputs: &RunningInputs,
    ctx: &RunningContext,
) -> Vec<(EmissionRow, f64)> {
    // SHO3 joins Link on linkID; every SHO3 row carries the constant
    // iteration link, so the iteration `Link` row is the only one that joins.
    // An absent iteration link drops every output row (the SQL inner join).
    let link: HashMap<i32, &LinkRow> = inputs.link.iter().map(|r| (r.link_id, r)).collect();
    let Some(link_row) = link.get(&ctx.link_id) else {
        return Vec::new();
    };

    // WeightedAndAdjustedEmissionRate3, indexed by the SHO3 join key. The
    // SQL's WAER3 re-joins HourDay, but `fully_weight` already resolved
    // dayID/hourID to the same values, so only the PollutantProcessAssoc
    // resolution remains.
    let ppa: HashMap<i32, &PollutantProcessAssocRow> = inputs
        .pollutant_process_assoc
        .iter()
        .map(|r| (r.pol_process_id, r))
        .collect();
    let mut waer3_by: Waer3Index = HashMap::new();
    for fw in fully_weighted {
        let Some(assoc) = ppa.get(&fw.pol_process_id) else {
            continue;
        };
        waer3_by
            .entry((
                ctx.year_id,
                fw.month_id,
                fw.day_id,
                fw.hour_id,
                fw.source_type_id,
                fw.model_year_id,
            ))
            .or_default()
            .push(Waer3Entry {
                pollutant_id: assoc.pollutant_id,
                process_id: assoc.process_id,
                fuel_type_id: fw.fuel_type_id,
                mean_base_rate: fw.mean_base_rate,
                mean_base_rate_im: fw.mean_base_rate_im,
            });
    }

    // SHO2 ⋈ WAER3 → SHO3 → MOVESWorkerOutput.
    let hour_day: HashMap<i32, &HourDayRow> =
        inputs.hour_day.iter().map(|r| (r.hour_day_id, r)).collect();
    let mut out: Vec<(EmissionRow, f64)> = Vec::new();
    for sho in &inputs.sho {
        // SHO2: INNER JOIN HourDay USING (hourDayID).
        let Some(hd) = hour_day.get(&sho.hour_day_id) else {
            continue;
        };
        let model_year_id = sho.year_id - sho.age_id;
        // SHO3: INNER JOIN WAER3 on (year, month, day, hour, sourceType,
        // modelYear).
        let Some(waers) = waer3_by.get(&(
            sho.year_id,
            sho.month_id,
            hd.day_id,
            hd.hour_id,
            sho.source_type_id,
            model_year_id,
        )) else {
            continue;
        };
        for waer in waers {
            let row = EmissionRow {
                year_id: sho.year_id,
                month_id: sho.month_id,
                day_id: hd.day_id,
                hour_id: hd.hour_id,
                state_id: ctx.state_id,
                county_id: ctx.county_id,
                zone_id: link_row.zone_id,
                link_id: link_row.link_id,
                pollutant_id: waer.pollutant_id,
                process_id: waer.process_id,
                source_type_id: sho.source_type_id,
                fuel_type_id: waer.fuel_type_id,
                model_year_id,
                road_type_id: link_row.road_type_id,
                emission_quant: sho.sho * waer.mean_base_rate,
            };
            out.push((row, sho.sho * waer.mean_base_rate_im));
        }
    }
    out
}

/// The calculator's single master-loop subscription.
///
/// `GenericCalculatorBase.subscribeToMe` subscribes once per process the
/// calculator's `polProcessID`s span; `"3001"` is process 1 (Running
/// Exhaust), so there is exactly one subscription, at `YEAR` granularity and
/// `EMISSION_CALCULATOR` priority (the Java constructor passes a zero priority
/// offset). `calculator-dag.json` records the granularity and priority but a
/// placeholder `process_id` of `0`, because the static analyser cannot
/// resolve `GenericCalculatorBase`'s runtime `polProcessID` lookup — the true
/// process id (1) comes from the constructor.
fn subscriptions() -> &'static [CalculatorSubscription] {
    static SUBS: OnceLock<[CalculatorSubscription; 1]> = OnceLock::new();
    SUBS.get_or_init(|| {
        let priority = Priority::parse("EMISSION_CALCULATOR")
            .expect("\"EMISSION_CALCULATOR\" is a valid MasterLoop priority");
        [CalculatorSubscription::new(
            ProcessId(RUNNING_EXHAUST_PROCESS_ID),
            Granularity::Year,
            priority,
        )]
    })
}

/// The `(pollutant, process)` pairs the calculator registers — **none**.
///
/// `NH3RunningCalculator` is superseded by `BaseRateCalculator` (see the
/// module-level supersession note): it is absent from `CalculatorInfo.txt`
/// and `calculator-dag.json` records `registrations_count: 0`. NH3 Running
/// Exhaust `(30, 1)` is registered to `BaseRateCalculator`, so registering it
/// here too would double-register the pair in the calculator registry. The
/// Java constructor's `GenericCalculatorBase` `polProcessID` argument is
/// intentionally not surfaced as a registration.
static REGISTRATIONS: &[PollutantProcessAssociation] = &[];

/// Default-DB tables `NH3RunningCalculator.sql`'s processing pass reads.
///
/// The script's extract section also pulls `County`, `criteriaRatio`,
/// `FuelFormulation`, `FuelSubType`, `FuelSupply`, `FuelType`,
/// `MonthGroupHour`, `MonthOfAnyYear`, `Year` and `Zone`, none of which the
/// processing pass consumes (the Ammonia calculator carries no fuel,
/// temperature or humidity effects); they are omitted here.
static INPUT_TABLES: &[&str] = &[
    "AgeCategory",
    "EmissionRateByAge",
    "HourDay",
    "IMCoverage",
    "IMFactor",
    "Link",
    "OpModeDistribution",
    "PollutantProcessAssoc",
    "PollutantProcessMappedModelYear",
    "RunSpecMonth",
    "SHO",
    "SourceBin",
    "SourceBinDistribution",
    "SourceTypeModelYear",
];

impl Calculator for Nh3RunningCalculator {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn subscriptions(&self) -> &[CalculatorSubscription] {
        subscriptions()
    }

    /// `NH3RunningCalculator` registers **no** `(pollutant, process)` pairs /// see `REGISTRATIONS` and the module-level supersession note.
    fn registrations(&self) -> &[PollutantProcessAssociation] {
        REGISTRATIONS
    }

    // `upstream` keeps the trait default (empty): `calculator-dag.json`
    // records no `depends_on` edges for `NH3RunningCalculator`.

    fn input_tables(&self) -> &[&'static str] {
        INPUT_TABLES
    }

    fn execute(&self, ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
        let tables = ctx.tables();
        let pos = ctx.position();
        // The SQL substitutes `##context.year##`,
        // `##context.iterLocation.countyRecordID##` and
        // `##context.iterLocation.linkRecordID##` as concrete run constants into
        // The master loop guarantees context fields are set at the subscribed
        // granularity; a None here is a programming error.
        let mc = |what: &'static str| Error::MissingContext { what: what.into() };
        let run_ctx = RunningContext {
            year_id: pos
                .time
                .year
                .map(|y| y as i32)
                .ok_or_else(|| mc("context.year"))?,
            state_id: pos
                .location
                .state_id
                .map(|s| s as i32)
                .ok_or_else(|| mc("context.stateID"))?,
            county_id: pos
                .location
                .county_id
                .map(|c| c as i32)
                .ok_or_else(|| mc("context.countyID"))?,
            link_id: pos
                .location
                .link_id
                .map(|l| l as i32)
                .ok_or_else(|| mc("context.linkID"))?,
        };
        let inputs = RunningInputs {
            sho: tables.iter_typed::<ShoRow>("SHO")?,
            link: tables.iter_typed::<LinkRow>("Link")?,
            runspec_months: tables
                .iter_typed::<RunSpecMonthIdRow>("RunSpecMonth")?
                .into_iter()
                .map(|r| r.month_id)
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
            im_coverage: tables.iter_typed_or_empty::<ImCoverageRow>("IMCoverage")?,
        };
        let rows = Nh3RunningCalculator::calculate(&inputs, &run_ctx);
        crate::wiring::emit_rows(rows)
    }
}

/// Construct the calculator as a boxed trait object — matches the engine's
/// calculator-factory signature so the registry can register it.
#[must_use]
pub fn factory() -> Box<dyn Calculator> {
    Box::new(Nh3RunningCalculator::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::calculators::nh3::common::NH3_POLLUTANT_ID;

    /// NH3 Running Exhaust `polProcessID` — `pollutant 30 × 100 + process 1`.
    const NH3_RUNNING_POL_PROCESS: i32 = 3001;

    /// A one-`SHO`, one-bin, one-operating-mode running input with no I/M
    /// coverage. The single output row is
    /// `emissionQuant = SHO 100 × (opModeFraction 1 × (sbaf 1 × meanBaseRate
    /// 2)) = 200`.
    fn minimal_inputs() -> RunningInputs {
        RunningInputs {
            sho: vec![ShoRow {
                hour_day_id: 85,
                month_id: 7,
                year_id: 2020,
                age_id: 2, // modelYearID = 2020 - 2 = 2018
                source_type_id: 21,
                sho: 100.0,
            }],
            link: vec![LinkRow {
                link_id: 5001,
                zone_id: 261_610,
                road_type_id: 4,
            }],
            runspec_months: vec![7],
            emission_rate_by_age: vec![EmissionRateByAgeRow {
                source_bin_id: 1000,
                pol_process_id: NH3_RUNNING_POL_PROCESS,
                op_mode_id: 1,
                age_group_id: 3,
                mean_base_rate: 2.0,
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
                pol_process_id: NH3_RUNNING_POL_PROCESS,
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
                pol_process_id: NH3_RUNNING_POL_PROCESS,
                op_mode_id: 1,
                op_mode_fraction: 1.0,
            }],
            hour_day: vec![HourDayRow {
                hour_day_id: 85,
                day_id: 5,
                hour_id: 8,
            }],
            pollutant_process_assoc: vec![PollutantProcessAssocRow {
                pol_process_id: NH3_RUNNING_POL_PROCESS,
                process_id: 1,
                pollutant_id: NH3_POLLUTANT_ID,
            }],
            pollutant_process_mapped_model_year: Vec::new(),
            im_factor: Vec::new(),
            im_coverage: Vec::new(),
        }
    }

    fn ctx() -> RunningContext {
        RunningContext {
            year_id: 2020,
            state_id: 26,
            county_id: 26_161,
            link_id: 5001,
        }
    }

    #[test]
    fn calculate_multiplies_activity_by_the_weighted_rate() {
        let out = Nh3RunningCalculator::calculate(&minimal_inputs(), &ctx());
        assert_eq!(out.len(), 1);
        let row = out[0];
        assert_eq!(row.pollutant_id, NH3_POLLUTANT_ID);
        assert_eq!(row.process_id, 1);
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
        assert_eq!(row.road_type_id, 4);
        // SHO 100 × opModeFraction 1 × (sbaf 1 × meanBaseRate 2) = 200.
        assert!((row.emission_quant - 200.0).abs() < 1e-9);
    }

    #[test]
    fn calculate_blends_in_the_im_quantity_where_coverage_exists() {
        let mut inputs = minimal_inputs();
        // Add I/M coverage so the row blends emissionQuant (200, from
        // meanBaseRate) with emissionQuantIM (100, from meanBaseRateIM).
        inputs.pollutant_process_mapped_model_year = vec![PollutantProcessMappedModelYearRow {
            pol_process_id: NH3_RUNNING_POL_PROCESS,
            model_year_id: 2018,
            im_model_year_group_id: 7,
        }];
        inputs.im_factor = vec![ImFactorRow {
            pol_process_id: NH3_RUNNING_POL_PROCESS,
            inspect_freq: 1,
            test_standards_id: 11,
            source_type_id: 21,
            fuel_type_id: 1,
            im_model_year_group_id: 7,
            age_group_id: 3,
            im_factor: 25.0,
        }];
        inputs.im_coverage = vec![ImCoverageRow {
            pol_process_id: NH3_RUNNING_POL_PROCESS,
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

        let out = Nh3RunningCalculator::calculate(&inputs, &ctx());
        assert_eq!(out.len(), 1);
        // IMAdjustFract = 25 × 80 × 0.01 = 20. Blend = 100 × 20 + 200 ×
        // (1 - 20) = 2000 - 3800 = -1800 → GREATEST(…, 0) = 0.
        assert!((out[0].emission_quant - 0.0).abs() < 1e-9);
    }

    #[test]
    fn calculate_drops_sho_whose_hour_day_is_unknown() {
        let mut inputs = minimal_inputs();
        inputs.sho[0].hour_day_id = 999; // no HourDay row
        assert!(Nh3RunningCalculator::calculate(&inputs, &ctx()).is_empty());
    }

    #[test]
    fn calculate_drops_every_row_when_the_iteration_link_is_absent() {
        let mut inputs = minimal_inputs();
        inputs.link.clear();
        assert!(Nh3RunningCalculator::calculate(&inputs, &ctx()).is_empty());
    }

    #[test]
    fn calculate_expands_one_rate_across_every_runspec_month() {
        let mut inputs = minimal_inputs();
        inputs.runspec_months = vec![6, 7, 8];
        // One SHO per month so each month has activity to multiply.
        inputs.sho = vec![6, 7, 8]
            .into_iter()
            .map(|month_id| ShoRow {
                hour_day_id: 85,
                month_id,
                year_id: 2020,
                age_id: 2,
                source_type_id: 21,
                sho: 100.0,
            })
            .collect();

        let out = Nh3RunningCalculator::calculate(&inputs, &ctx());
        assert_eq!(out.len(), 3);
        assert_eq!(
            out.iter().map(|r| r.month_id).collect::<Vec<_>>(),
            [6, 7, 8]
        );
        for row in &out {
            assert!((row.emission_quant - 200.0).abs() < 1e-9);
        }
    }

    #[test]
    fn subscriptions_are_running_exhaust_at_year_granularity() {
        let calc = Nh3RunningCalculator::new();
        let subs = calc.subscriptions();
        assert_eq!(subs.len(), 1);
        assert_eq!(subs[0].process_id, ProcessId(RUNNING_EXHAUST_PROCESS_ID));
        assert_eq!(subs[0].granularity, Granularity::Year);
    }

    #[test]
    fn registrations_are_empty_because_base_rate_calculator_supersedes_this() {
        assert!(Nh3RunningCalculator::new().registrations().is_empty());
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
        store.insert("SHO", ShoRow::into_dataframe(inputs.sho).unwrap());
        store.insert("Link", LinkRow::into_dataframe(inputs.link).unwrap());
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
                0,
                ctx_vals.link_id as u32,
            ),
            time: ExecutionTime::year(ctx_vals.year_id as u16),
        };
        let ctx = CalculatorContext::with_position_and_tables(position, store);
        let out = Nh3RunningCalculator::new()
            .execute(&ctx)
            .expect("execute ok");
        let df = out.dataframe().expect("expected non-empty DataFrame");
        assert_eq!(
            df.height(),
            1,
            "minimal inputs produce exactly one NH3 running row"
        );
        let quant = df
            .column("emissionQuant")
            .unwrap()
            .f64()
            .unwrap()
            .get(0)
            .unwrap();
        // SHO 100 × opModeFraction 1 × (sbaf 1 × meanBaseRate 2) = 200.
        assert!(
            (quant - 200.0).abs() < 1e-9,
            "emissionQuant {quant} != 200.0"
        );
    }

    #[test]
    fn factory_builds_a_calculator_named_for_the_java_class() {
        assert_eq!(factory().name(), "NH3RunningCalculator");
    }
}
