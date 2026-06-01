//! Shared infrastructure for the Well-To-Pump (WTP) calculator cluster//! .
//!
//! The four WTP calculators ([`super`]) all read the same `MOVESWorkerOutput`
//! shape and three of them ([`super::total_energy`], [`super::ch4n2o`],
//! [`super::co2_atmospheric`]) extract the same set of default-DB tables. This
//! module holds what they share:
//!
//! * [`WorkerOutputRow`] — the `MOVESWorkerOutput` row subset every WTP
//! calculator reads and writes.
//! * [`GreetWellToPumpRow`], [`FuelSupplyRow`], [`FuelFormulationRow`],
//! [`FuelSubTypeRow`], [`YearRow`], [`MonthGroupRow`] — the default-DB
//! tables the SQL "Extract Data" sections pull.
//! * [`WtpInputs`] — the bundle of those tables, the `calculate` argument of
//! the three GREET-based WTP calculators.
//! * [`build_wtp_factor_by_fuel_type`] — the GREET year-interpolation plus
//! market-share weighting that `WellToPumpCalculator.sql` and
//! `CH4N2OWTPCalculator.sql` perform in their "Extract Data" sections.
//!
//! # Fidelity — `FLOAT` intermediates
//!
//! `GREETWellToPump.emissionRate`, `FuelSupply.marketShare`,
//! `FuelSubtype.energyContent` and `MOVESWorkerOutput.emissionQuant` are all
//! `FLOAT` (32-bit) in MOVES, already `f32`-quantised before this port sees
//! them. The WTP SQL additionally writes the interpolated `WTPFactor` and the
//! market-share-weighted `WTPFactorByFuelType.WTPFactor` to `FLOAT` temp
//! columns, truncating the `DOUBLE` arithmetic to `f32` between the
//! interpolation, the weighting and the "Processing" join. This port computes
//! in `f64` end to end and does not reproduce those intermediate truncations//! a sub-`1e-7` relative drift. Reproducing MOVES bug-for-bug is the
//! calculator integration-validation call (), matching the
//! `CO2AERunningStartExtendedIdleCalculator` / `SO2Calculator` precedent.

use std::collections::HashMap;

use moves_framework::{Error, TableRow};
use polars::prelude::{DataFrame, DataType, NamedFrom, PolarsResult, Schema, Series};

fn row_err(table: &'static str, row: usize, column: &'static str, msg: String) -> Error {
    Error::RowExtraction {
        table: table.into(),
        row,
        column: column.into(),
        message: msg,
    }
}

/// One `MOVESWorkerOutput` row — the column subset every WTP calculator reads
/// from and writes back to the master output table.
///
/// `MOVESRunID`, `iterationID` and `SCC` are pure pass-through columns the WTP
/// SQL copies verbatim (where present at all — `CH4N2OWTPCalculator.sql` omits
/// `MOVESRunID`); they are not modelled here, matching the `SO2Calculator` /
/// `CO2AERunningStartExtendedIdleCalculator` precedent, and the output
/// wiring carries them. `regClassID` and `emissionRate` are likewise absent:
/// unlike the running/start exhaust calculators, **none of the four WTP SQL
/// scripts select `regClassID` or compute `emissionRate`** — a WTP output row
/// carries only `emissionQuant`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct WorkerOutputRow {
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
 /// `pollutantID`.
    pub pollutant_id: i32,
 /// `processID`.
    pub process_id: i32,
 /// `sourceTypeID`.
    pub source_type_id: i32,
 /// `fuelTypeID`.
    pub fuel_type_id: i32,
 /// `modelYearID`.
    pub model_year_id: i32,
 /// `roadTypeID`.
    pub road_type_id: i32,
 /// `emissionQuant` — the emission quantity.
    pub emission_quant: f64,
}

/// One `GREETWellToPump` row — an Argonne GREET well-to-pump emission rate for
/// a `(pollutant, fuelSubType, year)`.
///
/// `GREETWellToPump` is keyed `(yearID, pollutantID, fuelSubtypeID)` in
/// practice; [`build_wtp_factor_by_fuel_type`] indexes it by
/// `(pollutantID, fuelSubtypeID) → yearID → emissionRate`, so a duplicate
/// triple resolves last-write-wins.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GreetWellToPumpRow {
 /// `pollutantID` — the well-to-pump pollutant the rate is for.
    pub pollutant_id: i32,
 /// `fuelSubtypeID` — the fuel subtype the rate is for.
    pub fuel_sub_type_id: i32,
 /// `yearID` — the calendar year the rate is tabulated at.
    pub year_id: i32,
 /// `emissionRate` — the GREET emission rate. `FLOAT` in MOVES.
    pub emission_rate: f64,
}

/// One `FuelSupply` row — a fuel formulation's market share in the run's fuel
/// region for a `(fuelYear, monthGroup)`.
///
/// The SQL extracts `FuelSupply` filtered to the run's single fuel region, so
/// `fuelRegionID` is constant and is not modelled.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FuelSupplyRow {
 /// `fuelYearID` — joins to [`YearRow::fuel_year_id`].
    pub fuel_year_id: i32,
 /// `monthGroupID` — the month group this share applies to.
    pub month_group_id: i32,
 /// `fuelFormulationID` — joins to [`FuelFormulationRow::fuel_formulation_id`].
    pub fuel_formulation_id: i32,
 /// `marketShare` — this formulation's share of the fuel supply. `FLOAT`.
    pub market_share: f64,
}

/// One `FuelFormulation` row — resolves a fuel formulation to its subtype.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FuelFormulationRow {
 /// `fuelFormulationID` — the formulation primary key.
    pub fuel_formulation_id: i32,
 /// `fuelSubtypeID` — joins to [`FuelSubTypeRow::fuel_sub_type_id`].
    pub fuel_sub_type_id: i32,
}

/// One `FuelSubtype` row — resolves a fuel subtype to its parent fuel type.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FuelSubTypeRow {
 /// `fuelSubtypeID` — the subtype primary key.
    pub fuel_sub_type_id: i32,
 /// `fuelTypeID` — the parent fuel type.
    pub fuel_type_id: i32,
}

/// One `Year` row — resolves a `fuelYearID` to its calendar `yearID`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct YearRow {
 /// `yearID` — the calendar year.
    pub year_id: i32,
 /// `fuelYearID` — the fuel year, joins to [`FuelSupplyRow::fuel_year_id`].
    pub fuel_year_id: i32,
}

/// One `MonthOfAnyYear` row — the `monthID → monthGroupID` mapping the WTP
/// "Processing" join resolves.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MonthGroupRow {
 /// `monthID` — the calendar month.
    pub month_id: i32,
 /// `monthGroupID` — the month group it belongs to.
    pub month_group_id: i32,
}

/// The default-DB tables the three GREET-based WTP calculators'
/// ([`super::total_energy`], [`super::ch4n2o`], [`super::co2_atmospheric`])
/// SQL "Extract Data" sections pull.
///
/// A future (`DataFrameStore`) wiring populates this from the per-run
/// filtered execution database; until then it is the explicit data-plane
/// contract the unit tests build directly. `target_year` carries the
/// MasterLoop context year (`##context.year##`) the SQL substitutes into the
/// GREET year-bracket query.
#[derive(Debug, Clone, Default)]
pub struct WtpInputs {
 /// `GREETWellToPump` rows — filtered to the calculator's pollutant set.
    pub greet: Vec<GreetWellToPumpRow>,
 /// `FuelSupply` rows (single fuel region).
    pub fuel_supply: Vec<FuelSupplyRow>,
 /// `FuelFormulation` rows.
    pub fuel_formulation: Vec<FuelFormulationRow>,
 /// `FuelSubtype` rows.
    pub fuel_sub_type: Vec<FuelSubTypeRow>,
 /// `Year` rows.
    pub year: Vec<YearRow>,
 /// `MonthOfAnyYear` rows — the `monthID → monthGroupID` mapping.
    pub month_of_any_year: Vec<MonthGroupRow>,
 /// `MOVESWorkerOutput` rows — the upstream calculators' output the WTP
 /// "Processing" section reads.
    pub worker_output: Vec<WorkerOutputRow>,
 /// The MasterLoop context year (`##context.year##`).
    pub target_year: i32,
}

/// One `WTPFactorByFuelType` cell — a well-to-pump emission factor for a
/// pollutant, after GREET year-interpolation and market-share weighting.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct WtpFactorCell {
 /// `pollutantID` — the well-to-pump pollutant this factor is for.
    pub pollutant_id: i32,
 /// `WTPFactor` — the market-share-weighted, year-interpolated factor.
    pub factor: f64,
}

/// `WTPFactorByFuelType` as an index — `(yearID, monthGroupID, fuelTypeID)` to
/// the per-pollutant factor cells of that group.
///
/// The SQL keys `WTPFactorByFuelType` by `(countyID, yearID, monthGroupID,
/// pollutantID, fuelTypeID)`; `countyID` is the literal context county and is
/// carried straight from the energy row in the "Processing" join (the
/// single-county invariant, matching `SO2Calculator`). The remaining four key
/// columns split into the three-column map key and the [`WtpFactorCell`]'s
/// `pollutant_id`.
pub type WtpFactorTable = HashMap<(i32, i32, i32), Vec<WtpFactorCell>>;

/// Interpolate the GREET well-to-pump emission rates to `target_year`, keyed
/// by `fuelSubtypeID`.
///
/// Ports the `GREETWellToPumpBounds` / `GREETWellToPumpLo` /
/// `GREETWellToPumpHi` / `WTPFactor` chain of the WTP SQL "Extract Data"
/// section. For each `(pollutant, fuelSubType)` the GREET table tabulates:
///
/// * `lo` — the latest tabulated year `≤ target_year`, or the earliest
/// tabulated year when `target_year` precedes all of them.
/// * `hi` — the earliest tabulated year `> target_year`, or the latest
/// tabulated year when `target_year` is at or past all of them.
///
/// (The SQL's `Lo` bound uses `≤` and its `Hi` bound uses a strict `>`; this
/// asymmetry means an exact hit on a tabulated year takes that year as `lo`
/// and the *next* as `hi`, and the interpolation below then returns the
/// tabulated rate unchanged.) The factor is the linear interpolation
///
/// ```text
/// factor = rate(lo) + (rate(hi) − rate(lo)) × (target_year − lo) ÷ (hi − lo)
/// ```
///
/// with the denominator forced to 1 when `hi == lo` (the SQL's
/// `IF(hi<>lo, hi-lo, 1)`), which is harmless since `rate(hi) == rate(lo)`
/// there. `target_year` outside the tabulated range clamps to the nearest
/// endpoint (`lo == hi`).
///
/// # Fidelity — integer division
///
/// The SQL computes `(target_year − lo) ÷ (hi − lo)` with both operands
/// integers; MariaDB evaluates that as `DECIMAL` rounded to
/// `div_precision_increment` (default 4) places — e.g. `1/3` becomes
/// `0.3333`. This port divides in `f64`. The divergence scales the
/// `rate(hi) − rate(lo)` delta (not the whole factor) and stays well within
/// the tolerance budget; reproducing MariaDB's rounding bug-for-bug is
/// deferred to , matching the `CO2AERunningStartExtendedIdleCalculator`
/// `44/12` precedent.
fn interpolate_wtp_factors(
    greet: &[GreetWellToPumpRow],
    target_year: i32,
) -> HashMap<i32, Vec<WtpFactorCell>> {
 // Group GREET rows by (pollutant, fuelSubType) → yearID → emissionRate.
    let mut tabulated: HashMap<(i32, i32), HashMap<i32, f64>> = HashMap::new();
    for g in greet {
        tabulated
            .entry((g.pollutant_id, g.fuel_sub_type_id))
            .or_default()
            .insert(g.year_id, g.emission_rate);
    }

    let mut factors: HashMap<i32, Vec<WtpFactorCell>> = HashMap::new();
    for ((pollutant_id, fuel_sub_type_id), years) in &tabulated {
 // GREETWellToPumpBounds — the earliest/latest tabulated year. `years`
 // is non-empty: it was created by the loop above only on an insert.
        let min_year = years.keys().min().copied().unwrap_or(target_year);
        let max_year = years.keys().max().copied().unwrap_or(target_year);
 // GREETWellToPumpLo — latest year ≤ target_year, else the earliest.
        let lo_year = years
            .keys()
            .filter(|&&y| y <= target_year)
            .max()
            .copied()
            .unwrap_or(min_year);
 // GREETWellToPumpHi — earliest year > target_year, else the latest.
        let hi_year = years
            .keys()
            .filter(|&&y| y > target_year)
            .min()
            .copied()
            .unwrap_or(max_year);
        let lo_rate = years[&lo_year];
        let hi_rate = years[&hi_year];
        let denominator = if hi_year == lo_year {
            1.0
        } else {
            f64::from(hi_year - lo_year)
        };
        let fraction = f64::from(target_year - lo_year) / denominator;
        let factor = lo_rate + (hi_rate - lo_rate) * fraction;
        factors
            .entry(*fuel_sub_type_id)
            .or_default()
            .push(WtpFactorCell {
                pollutant_id: *pollutant_id,
                factor,
            });
    }
    factors
}

/// Build `WTPFactorByFuelType` — the GREET year-interpolation followed by the
/// market-share weighting that aggregates the per-fuel-subtype WTP factors up
/// to fuel types.
///
/// Ports the "Extract Data" computation shared verbatim by
/// `WellToPumpCalculator.sql` and `CH4N2OWTPCalculator.sql`:
/// the private `interpolate_wtp_factors` step yields the
/// per-`(pollutant, fuelSubType)` factor at `inputs.target_year`, then for
/// every `FuelSupply` row
///
/// ```text
/// WTPFactorByFuelType[year, monthGroup, pollutant, fuelType]
/// += WTPFactor[pollutant, fuelSubType] × marketShare
/// ```
///
/// joining `FuelSupply → FuelFormulation → FuelSubtype` for the fuel type and
/// `FuelSupply → Year` for the calendar year. Every SQL join is an
/// `INNER JOIN`, so a fuel-supply row that fails to resolve a formulation,
/// subtype or year is dropped; the port reproduces that with map lookups that
/// skip on a miss. The `Year` join keeps only the rows whose `yearID` equals
/// `target_year` (the SQL's `y.yearID = wf.yearID`, and `WTPFactor.yearID` is
/// itself `target_year`), so every key in the returned table carries
/// `target_year`.
#[must_use]
pub fn build_wtp_factor_by_fuel_type(inputs: &WtpInputs) -> WtpFactorTable {
    let wtp_factor = interpolate_wtp_factors(&inputs.greet, inputs.target_year);

    let formulation: HashMap<i32, &FuelFormulationRow> = inputs
        .fuel_formulation
        .iter()
        .map(|ff| (ff.fuel_formulation_id, ff))
        .collect();
    let sub_type: HashMap<i32, &FuelSubTypeRow> = inputs
        .fuel_sub_type
        .iter()
        .map(|fst| (fst.fuel_sub_type_id, fst))
        .collect();
 // Year resolves fuelYearID → yearID, keeping only the target-year rows
 // (the SQL's y.yearID = wf.yearID, wf.yearID being target_year).
    let target_fuel_years: std::collections::HashSet<i32> = inputs
        .year
        .iter()
        .filter(|y| y.year_id == inputs.target_year)
        .map(|y| y.fuel_year_id)
        .collect();

 // Accumulate Σ(WTPFactor × marketShare), grouped by the four SQL key
 // columns minus the literal-context countyID.
    let mut weighted: HashMap<(i32, i32, i32, i32), f64> = HashMap::new();
    for fs in &inputs.fuel_supply {
 // INNER JOIN Year ON Year.fuelYearID = FuelSupply.fuelYearID, the
 // surviving rows being target_year only.
        if !target_fuel_years.contains(&fs.fuel_year_id) {
            continue;
        }
 // INNER JOIN FuelFormulation USING (fuelFormulationID).
        let Some(ff) = formulation.get(&fs.fuel_formulation_id) else {
            continue;
        };
 // INNER JOIN FuelSubtype USING (fuelSubtypeID).
        let Some(fst) = sub_type.get(&ff.fuel_sub_type_id) else {
            continue;
        };
 // INNER JOIN WTPFactor ON fuelSubtypeID — one cell per pollutant.
        let Some(cells) = wtp_factor.get(&ff.fuel_sub_type_id) else {
            continue;
        };
        for cell in cells {
 *weighted
                .entry((
                    inputs.target_year,
                    fs.month_group_id,
                    cell.pollutant_id,
                    fst.fuel_type_id,
                ))
                .or_default() += cell.factor * fs.market_share;
        }
    }

 // Reshape to the (year, monthGroup, fuelType) → [cell] index the WTP
 // "Processing" join consumes.
    let mut table: WtpFactorTable = HashMap::new();
    for ((year_id, month_group_id, pollutant_id, fuel_type_id), factor) in weighted {
        table
            .entry((year_id, month_group_id, fuel_type_id))
            .or_default()
            .push(WtpFactorCell {
                pollutant_id,
                factor,
            });
    }
    for cells in table.values_mut() {
        cells.sort_unstable_by_key(|c| c.pollutant_id);
    }
    table
}

/// Index a `MonthOfAnyYear` extract as `monthID → monthGroupID`.
///
/// A duplicate `monthID` resolves last-write-wins; the table is unique on
/// `monthID` in practice.
#[must_use]
pub fn month_group_index(month_of_any_year: &[MonthGroupRow]) -> HashMap<i32, i32> {
    month_of_any_year
        .iter()
        .map(|m| (m.month_id, m.month_group_id))
        .collect()
}

impl TableRow for WorkerOutputRow {
    fn table_name() -> &'static str {
        "MOVESWorkerOutput"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("yearID".into(), DataType::Int32),
            ("monthID".into(), DataType::Int32),
            ("dayID".into(), DataType::Int32),
            ("hourID".into(), DataType::Int32),
            ("stateID".into(), DataType::Int32),
            ("countyID".into(), DataType::Int32),
            ("zoneID".into(), DataType::Int32),
            ("linkID".into(), DataType::Int32),
            ("pollutantID".into(), DataType::Int32),
            ("processID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("fuelTypeID".into(), DataType::Int32),
            ("modelYearID".into(), DataType::Int32),
            ("roadTypeID".into(), DataType::Int32),
            ("emissionQuant".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "yearID".into(),
                    rows.iter().map(|r| r.year_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "monthID".into(),
                    rows.iter().map(|r| r.month_id).collect::<Vec<i32>>(),
                )
                .into(),
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
                    "stateID".into(),
                    rows.iter().map(|r| r.state_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "countyID".into(),
                    rows.iter().map(|r| r.county_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "zoneID".into(),
                    rows.iter().map(|r| r.zone_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "linkID".into(),
                    rows.iter().map(|r| r.link_id).collect::<Vec<i32>>(),
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
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "fuelTypeID".into(),
                    rows.iter().map(|r| r.fuel_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "modelYearID".into(),
                    rows.iter().map(|r| r.model_year_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "roadTypeID".into(),
                    rows.iter().map(|r| r.road_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "emissionQuant".into(),
                    rows.iter().map(|r| r.emission_quant).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "MOVESWorkerOutput";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let yr = get_i32("yearID")?;
        let mo = get_i32("monthID")?;
        let da = get_i32("dayID")?;
        let hr = get_i32("hourID")?;
        let st = get_i32("stateID")?;
        let co = get_i32("countyID")?;
        let zo = get_i32("zoneID")?;
        let li = get_i32("linkID")?;
        let po = get_i32("pollutantID")?;
        let pr = get_i32("processID")?;
        let sty = get_i32("sourceTypeID")?;
        let fty = get_i32("fuelTypeID")?;
        let myr = get_i32("modelYearID")?;
        let rty = get_i32("roadTypeID")?;
        let eq = df
            .column("emissionQuant")
            .map_err(|e| row_err(t, 0, "emissionQuant", e.to_string()))?
            .f64()
            .map_err(|e| row_err(t, 0, "emissionQuant", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(WorkerOutputRow {
                    year_id: yr.get(i).ok_or_else(|| null("yearID"))?,
                    month_id: mo.get(i).ok_or_else(|| null("monthID"))?,
                    day_id: da.get(i).ok_or_else(|| null("dayID"))?,
                    hour_id: hr.get(i).ok_or_else(|| null("hourID"))?,
                    state_id: st.get(i).ok_or_else(|| null("stateID"))?,
                    county_id: co.get(i).ok_or_else(|| null("countyID"))?,
                    zone_id: zo.get(i).ok_or_else(|| null("zoneID"))?,
                    link_id: li.get(i).ok_or_else(|| null("linkID"))?,
                    pollutant_id: po.get(i).ok_or_else(|| null("pollutantID"))?,
                    process_id: pr.get(i).ok_or_else(|| null("processID"))?,
                    source_type_id: sty.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    fuel_type_id: fty.get(i).ok_or_else(|| null("fuelTypeID"))?,
                    model_year_id: myr.get(i).ok_or_else(|| null("modelYearID"))?,
                    road_type_id: rty.get(i).ok_or_else(|| null("roadTypeID"))?,
                    emission_quant: eq.get(i).ok_or_else(|| null("emissionQuant"))?,
                })
            })
            .collect()
    }
}

impl TableRow for GreetWellToPumpRow {
    fn table_name() -> &'static str {
        "GREETWellToPump"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("pollutantID".into(), DataType::Int32),
            ("fuelSubtypeID".into(), DataType::Int32),
            ("yearID".into(), DataType::Int32),
            ("emissionRate".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "pollutantID".into(),
                    rows.iter().map(|r| r.pollutant_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "fuelSubtypeID".into(),
                    rows.iter()
                        .map(|r| r.fuel_sub_type_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "yearID".into(),
                    rows.iter().map(|r| r.year_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "emissionRate".into(),
                    rows.iter().map(|r| r.emission_rate).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "GREETWellToPump";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let po = get_i32("pollutantID")?;
        let fs = get_i32("fuelSubtypeID")?;
        let yr = get_i32("yearID")?;
        let er = df
            .column("emissionRate")
            .map_err(|e| row_err(t, 0, "emissionRate", e.to_string()))?
            .f64()
            .map_err(|e| row_err(t, 0, "emissionRate", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(GreetWellToPumpRow {
                    pollutant_id: po.get(i).ok_or_else(|| null("pollutantID"))?,
                    fuel_sub_type_id: fs.get(i).ok_or_else(|| null("fuelSubtypeID"))?,
                    year_id: yr.get(i).ok_or_else(|| null("yearID"))?,
                    emission_rate: er.get(i).ok_or_else(|| null("emissionRate"))?,
                })
            })
            .collect()
    }
}

impl TableRow for FuelSupplyRow {
    fn table_name() -> &'static str {
        "FuelSupply"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("fuelYearID".into(), DataType::Int32),
            ("monthGroupID".into(), DataType::Int32),
            ("fuelFormulationID".into(), DataType::Int32),
            ("marketShare".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "fuelYearID".into(),
                    rows.iter().map(|r| r.fuel_year_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "monthGroupID".into(),
                    rows.iter().map(|r| r.month_group_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "fuelFormulationID".into(),
                    rows.iter()
                        .map(|r| r.fuel_formulation_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "marketShare".into(),
                    rows.iter().map(|r| r.market_share).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "FuelSupply";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let fy = get_i32("fuelYearID")?;
        let mg = get_i32("monthGroupID")?;
        let ff = get_i32("fuelFormulationID")?;
        let ms = df
            .column("marketShare")
            .map_err(|e| row_err(t, 0, "marketShare", e.to_string()))?
            .f64()
            .map_err(|e| row_err(t, 0, "marketShare", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(FuelSupplyRow {
                    fuel_year_id: fy.get(i).ok_or_else(|| null("fuelYearID"))?,
                    month_group_id: mg.get(i).ok_or_else(|| null("monthGroupID"))?,
                    fuel_formulation_id: ff.get(i).ok_or_else(|| null("fuelFormulationID"))?,
                    market_share: ms.get(i).ok_or_else(|| null("marketShare"))?,
                })
            })
            .collect()
    }
}

impl TableRow for FuelFormulationRow {
    fn table_name() -> &'static str {
        "FuelFormulation"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("fuelFormulationID".into(), DataType::Int32),
            ("fuelSubtypeID".into(), DataType::Int32),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "fuelFormulationID".into(),
                    rows.iter()
                        .map(|r| r.fuel_formulation_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "fuelSubtypeID".into(),
                    rows.iter()
                        .map(|r| r.fuel_sub_type_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "FuelFormulation";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let ff = get_i32("fuelFormulationID")?;
        let fs = get_i32("fuelSubtypeID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(FuelFormulationRow {
                    fuel_formulation_id: ff.get(i).ok_or_else(|| null("fuelFormulationID"))?,
                    fuel_sub_type_id: fs.get(i).ok_or_else(|| null("fuelSubtypeID"))?,
                })
            })
            .collect()
    }
}

impl TableRow for FuelSubTypeRow {
    fn table_name() -> &'static str {
        "FuelSubtype"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("fuelSubtypeID".into(), DataType::Int32),
            ("fuelTypeID".into(), DataType::Int32),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "fuelSubtypeID".into(),
                    rows.iter()
                        .map(|r| r.fuel_sub_type_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "fuelTypeID".into(),
                    rows.iter().map(|r| r.fuel_type_id).collect::<Vec<i32>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "FuelSubtype";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let fs = get_i32("fuelSubtypeID")?;
        let ft = get_i32("fuelTypeID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(FuelSubTypeRow {
                    fuel_sub_type_id: fs.get(i).ok_or_else(|| null("fuelSubtypeID"))?,
                    fuel_type_id: ft.get(i).ok_or_else(|| null("fuelTypeID"))?,
                })
            })
            .collect()
    }
}

impl TableRow for YearRow {
    fn table_name() -> &'static str {
        "Year"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("yearID".into(), DataType::Int32),
            ("fuelYearID".into(), DataType::Int32),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "yearID".into(),
                    rows.iter().map(|r| r.year_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "fuelYearID".into(),
                    rows.iter().map(|r| r.fuel_year_id).collect::<Vec<i32>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "Year";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let yr = get_i32("yearID")?;
        let fy = get_i32("fuelYearID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(YearRow {
                    year_id: yr.get(i).ok_or_else(|| null("yearID"))?,
                    fuel_year_id: fy.get(i).ok_or_else(|| null("fuelYearID"))?,
                })
            })
            .collect()
    }
}

impl TableRow for MonthGroupRow {
    fn table_name() -> &'static str {
        "MonthOfAnyYear"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("monthID".into(), DataType::Int32),
            ("monthGroupID".into(), DataType::Int32),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "monthID".into(),
                    rows.iter().map(|r| r.month_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "monthGroupID".into(),
                    rows.iter().map(|r| r.month_group_id).collect::<Vec<i32>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "MonthOfAnyYear";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let mo = get_i32("monthID")?;
        let mg = get_i32("monthGroupID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(MonthGroupRow {
                    month_id: mo.get(i).ok_or_else(|| null("monthID"))?,
                    month_group_id: mg.get(i).ok_or_else(|| null("monthGroupID"))?,
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

 /// `target_year` between two tabulated years interpolates linearly.
    #[test]
    fn interpolate_midpoint_between_two_years() {
        let greet = vec![
            GreetWellToPumpRow {
                pollutant_id: 91,
                fuel_sub_type_id: 21,
                year_id: 2010,
                emission_rate: 100.0,
            },
            GreetWellToPumpRow {
                pollutant_id: 91,
                fuel_sub_type_id: 21,
                year_id: 2020,
                emission_rate: 200.0,
            },
        ];
        let factors = interpolate_wtp_factors(&greet, 2015);
        let cells = &factors[&21];
        assert_eq!(cells.len(), 1);
 // 100 + (200 − 100) × (2015 − 2010) / (2020 − 2010) = 150.
        assert!((cells[0].factor - 150.0).abs() < 1e-9);
    }

 /// An exact hit on a tabulated year returns that year's rate unchanged:
 /// `lo` is the year itself, `hi` is the next, and `(target − lo) = 0`.
    #[test]
    fn interpolate_exact_year_returns_tabulated_rate() {
        let greet = vec![
            GreetWellToPumpRow {
                pollutant_id: 91,
                fuel_sub_type_id: 21,
                year_id: 2010,
                emission_rate: 100.0,
            },
            GreetWellToPumpRow {
                pollutant_id: 91,
                fuel_sub_type_id: 21,
                year_id: 2020,
                emission_rate: 200.0,
            },
        ];
        let factors = interpolate_wtp_factors(&greet, 2010);
        assert!((factors[&21][0].factor - 100.0).abs() < 1e-9);
    }

 /// `target_year` outside the tabulated range clamps to the nearest
 /// endpoint — `lo == hi`, so the factor is that endpoint's rate.
    #[test]
    fn interpolate_clamps_outside_the_tabulated_range() {
        let greet = vec![
            GreetWellToPumpRow {
                pollutant_id: 91,
                fuel_sub_type_id: 21,
                year_id: 2010,
                emission_rate: 100.0,
            },
            GreetWellToPumpRow {
                pollutant_id: 91,
                fuel_sub_type_id: 21,
                year_id: 2020,
                emission_rate: 200.0,
            },
        ];
 // Before the range → earliest year's rate.
        assert!((interpolate_wtp_factors(&greet, 1990)[&21][0].factor - 100.0).abs() < 1e-9);
 // After the range → latest year's rate.
        assert!((interpolate_wtp_factors(&greet, 2050)[&21][0].factor - 200.0).abs() < 1e-9);
    }

 /// A single tabulated year yields that year's rate for any target.
    #[test]
    fn interpolate_single_year_is_constant() {
        let greet = vec![GreetWellToPumpRow {
            pollutant_id: 5,
            fuel_sub_type_id: 21,
            year_id: 2015,
            emission_rate: 42.0,
        }];
        for target in [2000, 2015, 2030] {
            assert!((interpolate_wtp_factors(&greet, target)[&21][0].factor - 42.0).abs() < 1e-9);
        }
    }

 /// Distinct pollutants for one fuel subtype produce one cell each.
    #[test]
    fn interpolate_keeps_pollutants_distinct() {
        let greet = vec![
            GreetWellToPumpRow {
                pollutant_id: 5,
                fuel_sub_type_id: 21,
                year_id: 2015,
                emission_rate: 10.0,
            },
            GreetWellToPumpRow {
                pollutant_id: 6,
                fuel_sub_type_id: 21,
                year_id: 2015,
                emission_rate: 20.0,
            },
        ];
        let cells = &interpolate_wtp_factors(&greet, 2015)[&21];
        assert_eq!(cells.len(), 2);
        assert!(cells
            .iter()
            .any(|c| c.pollutant_id == 5 && c.factor == 10.0));
        assert!(cells
            .iter()
            .any(|c| c.pollutant_id == 6 && c.factor == 20.0));
    }

 /// A one-formulation supply weights the interpolated factor by its market
 /// share and keys the result by `(target_year, monthGroup, fuelType)`.
    #[test]
    fn build_factor_weights_by_market_share() {
        let inputs = WtpInputs {
            greet: vec![GreetWellToPumpRow {
                pollutant_id: 91,
                fuel_sub_type_id: 21,
                year_id: 2020,
                emission_rate: 100.0,
            }],
            fuel_supply: vec![FuelSupplyRow {
                fuel_year_id: 2020,
                month_group_id: 1,
                fuel_formulation_id: 100,
                market_share: 0.25,
            }],
            fuel_formulation: vec![FuelFormulationRow {
                fuel_formulation_id: 100,
                fuel_sub_type_id: 21,
            }],
            fuel_sub_type: vec![FuelSubTypeRow {
                fuel_sub_type_id: 21,
                fuel_type_id: 2,
            }],
            year: vec![YearRow {
                year_id: 2020,
                fuel_year_id: 2020,
            }],
            month_of_any_year: vec![],
            worker_output: vec![],
            target_year: 2020,
        };
        let table = build_wtp_factor_by_fuel_type(&inputs);
        let cells = &table[&(2020, 1, 2)];
        assert_eq!(cells.len(), 1);
 // 100.0 × 0.25.
        assert!((cells[0].factor - 25.0).abs() < 1e-9);
        assert_eq!(cells[0].pollutant_id, 91);
    }

 /// Two formulations of one fuel type sum their market-share-weighted
 /// factors into a single fuel-type cell.
    #[test]
    fn build_factor_sums_formulations_of_one_fuel_type() {
        let inputs = WtpInputs {
            greet: vec![
                GreetWellToPumpRow {
                    pollutant_id: 91,
                    fuel_sub_type_id: 21,
                    year_id: 2020,
                    emission_rate: 100.0,
                },
                GreetWellToPumpRow {
                    pollutant_id: 91,
                    fuel_sub_type_id: 22,
                    year_id: 2020,
                    emission_rate: 200.0,
                },
            ],
            fuel_supply: vec![
                FuelSupplyRow {
                    fuel_year_id: 2020,
                    month_group_id: 1,
                    fuel_formulation_id: 100,
                    market_share: 0.5,
                },
                FuelSupplyRow {
                    fuel_year_id: 2020,
                    month_group_id: 1,
                    fuel_formulation_id: 101,
                    market_share: 0.5,
                },
            ],
            fuel_formulation: vec![
                FuelFormulationRow {
                    fuel_formulation_id: 100,
                    fuel_sub_type_id: 21,
                },
                FuelFormulationRow {
                    fuel_formulation_id: 101,
                    fuel_sub_type_id: 22,
                },
            ],
            fuel_sub_type: vec![
                FuelSubTypeRow {
                    fuel_sub_type_id: 21,
                    fuel_type_id: 2,
                },
                FuelSubTypeRow {
                    fuel_sub_type_id: 22,
                    fuel_type_id: 2,
                },
            ],
            year: vec![YearRow {
                year_id: 2020,
                fuel_year_id: 2020,
            }],
            month_of_any_year: vec![],
            worker_output: vec![],
            target_year: 2020,
        };
        let table = build_wtp_factor_by_fuel_type(&inputs);
 // 0.5 × 100 + 0.5 × 200 = 150, both subtypes rolling up to fuel type 2.
        assert!((table[&(2020, 1, 2)][0].factor - 150.0).abs() < 1e-9);
    }

 /// A fuel-supply row whose `Year` join resolves a year other than the
 /// target is dropped — the SQL keeps only `y.yearID = target_year`.
    #[test]
    fn build_factor_drops_non_target_year_supply() {
        let inputs = WtpInputs {
            greet: vec![GreetWellToPumpRow {
                pollutant_id: 91,
                fuel_sub_type_id: 21,
                year_id: 2020,
                emission_rate: 100.0,
            }],
            fuel_supply: vec![FuelSupplyRow {
                fuel_year_id: 1990,
                month_group_id: 1,
                fuel_formulation_id: 100,
                market_share: 1.0,
            }],
            fuel_formulation: vec![FuelFormulationRow {
                fuel_formulation_id: 100,
                fuel_sub_type_id: 21,
            }],
            fuel_sub_type: vec![FuelSubTypeRow {
                fuel_sub_type_id: 21,
                fuel_type_id: 2,
            }],
 // The only Year row resolves fuel year 1990 → calendar 1990, not
 // the target 2020.
            year: vec![YearRow {
                year_id: 1990,
                fuel_year_id: 1990,
            }],
            month_of_any_year: vec![],
            worker_output: vec![],
            target_year: 2020,
        };
        assert!(build_wtp_factor_by_fuel_type(&inputs).is_empty());
    }

 /// A fuel-supply row with no matching formulation/subtype is dropped.
    #[test]
    fn build_factor_drops_unjoined_supply() {
        let mut inputs = WtpInputs {
            greet: vec![GreetWellToPumpRow {
                pollutant_id: 91,
                fuel_sub_type_id: 21,
                year_id: 2020,
                emission_rate: 100.0,
            }],
            fuel_supply: vec![FuelSupplyRow {
                fuel_year_id: 2020,
                month_group_id: 1,
                fuel_formulation_id: 100,
                market_share: 1.0,
            }],
            fuel_formulation: vec![FuelFormulationRow {
                fuel_formulation_id: 100,
                fuel_sub_type_id: 21,
            }],
            fuel_sub_type: vec![FuelSubTypeRow {
                fuel_sub_type_id: 21,
                fuel_type_id: 2,
            }],
            year: vec![YearRow {
                year_id: 2020,
                fuel_year_id: 2020,
            }],
            month_of_any_year: vec![],
            worker_output: vec![],
            target_year: 2020,
        };
 // No formulation → the supply row cannot resolve a subtype.
        inputs.fuel_formulation.clear();
        assert!(build_wtp_factor_by_fuel_type(&inputs).is_empty());
    }

    #[test]
    fn month_group_index_maps_month_to_group() {
        let index = month_group_index(&[
            MonthGroupRow {
                month_id: 1,
                month_group_id: 1,
            },
            MonthGroupRow {
                month_id: 7,
                month_group_id: 7,
            },
        ]);
        assert_eq!(index.get(&1), Some(&1));
        assert_eq!(index.get(&7), Some(&7));
        assert_eq!(index.get(&12), None);
    }
}
