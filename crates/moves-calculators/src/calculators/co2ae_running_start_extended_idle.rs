//! Port of `CO2AERunningStartExtendedIdleCalculator.java` and
//! `database/CO2AERunningStartExtendedIdleCalculator.sql`//! .
//!
//! This calculator produces two greenhouse-gas pollutants for the running,
//! start, extended-idle and auxiliary-power exhaust processes:
//!
//! * **Atmospheric CO2** (pollutant 90) — the CO2 released when the carbon in
//! the fuel burned oxidises. It is a direct function of Total Energy
//! Consumption: energy ⇒ fuel ⇒ carbon ⇒ CO2.
//! * **CO2 Equivalent** (pollutant 98) — the climate-impact-weighted sum of
//! Atmospheric CO2, methane and nitrous oxide, each scaled by its global
//! warming potential.
//!
//! # Chained calculator
//!
//! `CO2AERunningStartExtendedIdleCalculator` is a *chained* calculator: its
//! Java `subscribeToMe` does not subscribe to the MasterLoop but chains the
//! calculator onto the ones that produce Total Energy Consumption, methane and
//! nitrous oxide, so the CO2 pollutants are added to whatever produced those.
//! `calculator-dag.json` records `subscribes_directly: false`,
//! `subscriptions: []`, and `depends_on: ["BaseRateCalculator",
//! "CrankcaseEmissionCalculatorNonPM", "HCSpeciationCalculator"]`; the
//! [`Calculator`] metadata methods mirror it — [`subscriptions`](Calculator::subscriptions)
//! is empty and [`upstream`](Calculator::upstream) names those three.
//!
//! # The two-step algorithm
//!
//! `calculate` ports the SQL's "Processing" section, which runs in two ordered
//! steps. The crucial detail is that **step 2 consumes step 1a's output**: the
//! SQL inserts the Atmospheric CO2 rows into `MOVESWorkerOutput`, then step 2
//! reads `MOVESWorkerOutput` back — now including those fresh rows.
//!
//! **Step 1a — Atmospheric CO2.** For every Total Energy Consumption record
//! (`MOVESWorkerOutput`, pollutant 91):
//!
//! ```text
//! AtmosphericCO2 = Σ energy × sumCarbonContent × sumOxidationFraction × 44/12
//! ```
//!
//! summed over the rows of each output dimension cell. `sumCarbonContent` and
//! `sumOxidationFraction` are the market-share-weighted carbon content and
//! oxidation fraction of the cell's `(year, monthGroup, fuelType)` fuel supply
//! the SQL's `CarbonOxidationByFuelType` working table. `44/12` is the mass
//! ratio of a CO2 molecule (44 g/mol) to its carbon atom (12 g/mol): oxidising
//! one unit mass of carbon yields 44/12 units of CO2.
//!
//! **Step 2 — CO2 Equivalent.** For the Atmospheric CO2 (90), methane (5) and
//! nitrous oxide (6) records:
//!
//! ```text
//! CO2Equivalent = Σ emission × globalWarmingPotential
//! ```
//!
//! summed over the rows of each output dimension cell. Each input pollutant's
//! `globalWarmingPotential` is its entry in the `Pollutant` table (the SQL's
//! `CO2EqPollutant` extract, filtered to a positive potential).
//!
//! Every SQL join is an `INNER JOIN`, so a row that fails to match a join key
//! is dropped; the port reproduces that with map lookups that skip on a miss.
//!
//! # Scope of this port
//!
//! `calculate` is the SQL "Processing" section plus the one real computation
//! in the "Extract Data" section — the market-share-weighted
//! `CarbonOxidationByFuelType` aggregation. Its [`Co2aeInputs`] argument is the
//! set of tables the SQL extracts, as plain row vectors; a future
//! (`DataFrameStore`) wiring populates it from the per-run filtered execution
//! database.
//!
//! Two of [`Co2aeInputs`]' fields are not extracted tables but RunSpec-derived
//! process filters. The Java `doExecute` builds `##CO2Step1AprocessIDs##` and
//! `##CO2Step2processIDs##` — the processes for which Atmospheric CO2 / CO2
//! Equivalent were actually requested — and substitutes them into the SQL's
//! `WHERE` clauses; when a step is requested for no process the macro becomes
//! the never-true `1=2`. [`Co2aeInputs::step1a_process_ids`] and
//! [`Co2aeInputs::step2_process_ids`] carry those process lists; an empty list
//! is the `1=2` case and yields no rows for that step. The Java's
//! whole-calculator gate — `doExecute` returns no SQL when neither pollutant is
//! requested — then falls out for free: both lists empty yields an empty
//! [`Co2aeOutput`]. The Step 2 pollutant set `##CO2Step2pollutantIDs##` is the
//! Java-side constant `"90,5,6"`, modelled here as the `CO2_EQUIVALENT_INPUTS`
//! constant.
//!
//! # Fidelity notes
//!
//! **The `44/12` mass ratio.** The SQL literal `(44/12)` is an integer/integer
//! division. MariaDB evaluates an exact-operand `/` as `DECIMAL`, rounded to
//! `div_precision_increment` (default 4) extra places — so `44/12` is
//! `3.6667`, not `3.66666…`. This port uses the exact `f64` ratio
//! `44.0 / 12.0`; the ~9e-6 relative divergence is within the tolerance
//! budget, and reproducing MariaDB's rounding bug-for-bug is the calculator
//! integration-validation call (), matching the / /
//! `SO2Calculator` precedent.
//!
//! **`FLOAT` columns.** `marketShare`, `carbonContent`, `oxidationFraction`
//! (model inputs) and `MOVESWorkerOutput.emissionQuant` / `.emissionRate` are
//! all `FLOAT` (32-bit) in MOVES — already `f32`-quantised before `calculate`
//! sees them. The SQL also writes the
//! `CarbonOxidationByFuelType.sumCarbonContent` / `.sumOxidationFraction`
//! aggregates to `FLOAT` temp columns, truncating the `DOUBLE` sum to `f32`
//! between the Extract and Processing sections. This port sums and multiplies
//! in `f64` end to end and does not reproduce that intermediate truncation — a
//! sub-1e-7 relative drift, again deferred to .
//! `globalWarmingPotential` is a `SMALLINT` — an exact integer, no rounding.
//!
//! **The `GROUP BY` key.** Both processing steps `GROUP BY` the full
//! `MOVESWorkerOutput` dimension: the fourteen integer columns the
//! `WorkerOutputRow::dimension_key` method returns, plus `MOVESRunID` and `SCC`
//! (`iterationID` is neither selected nor grouped). `MOVESRunID` is constant
//! within a run. `SCC` is a Source Classification *Code* derived from the
//! source-type / road-type / fuel-type / process columns, so two rows that
//! agree on the fourteen integer columns also agree on `SCC` — grouping by the
//! integer dimension yields the same partition as the full SQL `GROUP BY`.
//! `MOVESRunID` and `SCC` are pass-through columns left to the output
//! wiring, matching the `SO2Calculator` / `DistanceCalculator` precedent.
//!
//! # Data plane
//!
//! [`Calculator::execute`] is fully wired and on the live execution path. It
//! reads `RunSpecPollutantProcess` from the [`CalculatorContext`] tables to
//! derive the Step 1a / Step 2 process filters (the Java `##CO2Step1AprocessIDs##`
//! / `##CO2Step2processIDs##` macros), reads the SQL's extracted input tables
//! (`FuelSupply`, `FuelFormulation`, `FuelSubtype`, `Year`, `MonthOfAnyYear`,
//! `Pollutant`, and `MOVESWorkerOutput`) via [`CalculatorContext::tables`],
//! materialises a [`Co2aeInputs`], calls [`calculate`](CO2AERunningStartExtendedIdleCalculator::calculate),
//! and emits the produced Atmospheric CO2 and CO2 Equivalent rows back into
//! `MOVESWorkerOutput` via the shared `wiring::emit_rows` helper. The SQL's
//! two-step insert-then-read-back ordering is preserved inside `calculate`,
//! which feeds the Step 1a Atmospheric CO2 rows into Step 2 before they are
//! returned. The numeric algorithm is also independently unit-tested on
//! `calculate`.

use rustc_hash::FxHashMap;

use moves_data::{PollutantId, PollutantProcessAssociation, ProcessId};
use moves_framework::{
    Calculator, CalculatorContext, CalculatorOutput, CalculatorSubscription, DataFrameStoreTyped,
    Error, TableRow,
};
use polars::prelude::{DataFrame, DataType, NamedFrom, PolarsResult, Schema, Series};

/// Stable module name — matches the Java class and the
/// `CO2AERunningStartExtendedIdleCalculator` entry in the calculator-chain DAG
/// (`calculator-dag.json`).
const CALCULATOR_NAME: &str = "CO2AERunningStartExtendedIdleCalculator";

/// Atmospheric CO2 — `Pollutant` id 90. The pollutant Step 1a produces and one
/// of the three Step 2 sums.
const ATMOSPHERIC_CO2_POLLUTANT_ID: i32 = 90;

/// CO2 Equivalent — `Pollutant` id 98. The pollutant Step 2 produces.
const CO2_EQUIVALENT_POLLUTANT_ID: i32 = 98;

/// Total Energy Consumption — `Pollutant` id 91. The energy rows Step 1a
/// consumes are the `MOVESWorkerOutput` records for this pollutant.
const TOTAL_ENERGY_POLLUTANT_ID: i32 = 91;

/// Methane — `Pollutant` id 5. One of the three Step 2 sums.
const METHANE_POLLUTANT_ID: i32 = 5;

/// Nitrous Oxide — `Pollutant` id 6. One of the three Step 2 sums.
const NITROUS_OXIDE_POLLUTANT_ID: i32 = 6;

/// The three pollutants Step 2 weights into CO2 Equivalent — the Java-side
/// constant `##CO2Step2pollutantIDs##` (`"90,5,6"`), built unconditionally in
/// `doExecute`. A `MOVESWorkerOutput` row is a Step 2 input only if its
/// pollutant is one of these.
const CO2_EQUIVALENT_INPUTS: [i32; 3] = [
    ATMOSPHERIC_CO2_POLLUTANT_ID,
    METHANE_POLLUTANT_ID,
    NITROUS_OXIDE_POLLUTANT_ID,
];

/// Mass ratio of a carbon dioxide molecule (44 g/mol) to its carbon atom
/// (12 g/mol). Oxidising one unit mass of fuel carbon yields `44/12` units of
/// CO2. The SQL literal is the integer division `(44/12)`; see the module
/// fidelity note for the MariaDB rounding divergence this exact `f64` ratio
/// does not reproduce.
const CARBON_TO_CO2_MASS_RATIO: f64 = 44.0 / 12.0;

// ===========================================================================
// Input tables — plain Rust mirrors of the tables the SQL's "Extract Data"
// section pulls. Following the convention, every `INT`/`SMALLINT`
// identifier is an `i32` and every `FLOAT`/`DOUBLE` quantity is an `f64`. Only
// the columns the CO2AE algorithm reads are modelled.
// ===========================================================================

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
 /// `marketShare` — this formulation's share of the fuel supply. `FLOAT` in
 /// MOVES.
    pub market_share: f64,
}

/// One `FuelFormulation` row — bridges a fuel formulation to its subtype.
///
/// CO2AE reads no quantity from `FuelFormulation`; it is purely the join hop
/// `FuelSupply.fuelFormulationID → FuelFormulation.fuelSubtypeID → FuelSubtype`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FuelFormulationRow {
 /// `fuelFormulationID` — the formulation primary key.
    pub fuel_formulation_id: i32,
 /// `fuelSubtypeID` — joins to [`FuelSubtypeRow::fuel_subtype_id`].
    pub fuel_subtype_id: i32,
}

/// One `FuelSubtype` row — a fuel subtype's parent fuel type, carbon content
/// and oxidation fraction.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FuelSubtypeRow {
 /// `fuelSubtypeID` — the subtype primary key.
    pub fuel_subtype_id: i32,
 /// `fuelTypeID` — the parent fuel type the carbon-oxidation cell is keyed by.
    pub fuel_type_id: i32,
 /// `carbonContent` — mass of carbon per unit energy. `FLOAT` in MOVES.
    pub carbon_content: f64,
 /// `oxidationFraction` — fraction of fuel carbon that oxidises to CO2.
 /// `FLOAT` in MOVES.
    pub oxidation_fraction: f64,
}

/// One `Year` row — resolves a `fuelYearID` into its calendar `yearID`.
///
/// The SQL extracts `Year` filtered to `yearID = ##context.year##`, so the run
/// carries a single calendar year here.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct YearRow {
 /// `yearID` — the calendar year.
    pub year_id: i32,
 /// `fuelYearID` — the fuel year; joins to [`FuelSupplyRow::fuel_year_id`].
    pub fuel_year_id: i32,
}

/// One `MonthOfAnyYear` row — the `monthID → monthGroupID` mapping (the SQL's
/// `CO2MonthofAnyYear` extract).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MonthGroupRow {
 /// `monthID` — the calendar month.
    pub month_id: i32,
 /// `monthGroupID` — the month group it belongs to.
    pub month_group_id: i32,
}

/// One `CO2EqPollutant` row — a pollutant's global warming potential.
///
/// The SQL extracts this from `Pollutant` filtered to a non-null,
/// strictly-positive `globalWarmingPotential`; the caller supplies that
/// already-filtered set. `pollutantName` and `energyOrMass` are extracted too
/// but the "Processing" section reads only the potential, so they are not
/// modelled.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Co2EqPollutantRow {
 /// `pollutantID` — joins to [`WorkerOutputRow::pollutant_id`].
    pub pollutant_id: i32,
 /// `globalWarmingPotential` — the CO2-equivalence multiplier. `SMALLINT`
 /// (exact integer) in MOVES.
    pub global_warming_potential: i32,
}

/// One `MOVESWorkerOutput` row — a dimension cell plus its two emission values.
///
/// This single type models both roles the SQL gives `MOVESWorkerOutput`: the
/// **input** energy / methane / nitrous-oxide rows `calculate` reads, and the
/// **output** Atmospheric CO2 / CO2 Equivalent rows it produces — which the SQL
/// inserts straight back into `MOVESWorkerOutput`. Step 1a's output is itself a
/// Step 2 input, so a shared type mirrors the SQL exactly.
///
/// `MOVESRunID`, `iterationID` and `SCC` are not modelled — see the module
/// `GROUP BY` fidelity note.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct WorkerOutputRow {
 /// `pollutantID` — 91 for Step 1a inputs; 90 / 5 / 6 for Step 2 inputs;
 /// 90 / 98 on the rows `calculate` produces.
    pub pollutant_id: i32,
 /// `processID` — the emission process.
    pub process_id: i32,
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
 /// `emissionQuant` — the emission quantity.
    pub emission_quant: f64,
 /// `emissionRate` — the emission rate.
    pub emission_rate: f64,
}

impl WorkerOutputRow {
 /// The fourteen integer columns the SQL's two `GROUP BY` clauses share,
 /// minus `pollutantID` (replaced by the literal output pollutant) and the
 /// pass-through `MOVESRunID` / `SCC` (see the module fidelity note). Used
 /// both as the grouping key and — via [`Ord`] on the array — as the
 /// deterministic output sort key; MOVES leaves `MOVESWorkerOutput`
 /// physically unordered.
    fn dimension_key(&self) -> [i32; 14] {
        [
            self.year_id,
            self.month_id,
            self.day_id,
            self.hour_id,
            self.state_id,
            self.county_id,
            self.zone_id,
            self.link_id,
            self.process_id,
            self.source_type_id,
            self.reg_class_id,
            self.fuel_type_id,
            self.model_year_id,
            self.road_type_id,
        ]
    }
}

fn row_err(table: &'static str, row: usize, column: &'static str, msg: String) -> Error {
    Error::RowExtraction {
        table: table.into(),
        row,
        column: column.into(),
        message: msg,
    }
}

/// One `RunSpecPollutantProcess` row — a `polProcessID` the RunSpec requests
/// output for. Used by `execute` to derive `step1a_process_ids` and
/// `step2_process_ids`.
#[derive(Debug, Clone, Copy)]
struct RunSpecPollutantProcessRow {
    pol_process_id: i32,
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
        let pp = df
            .column("polProcessID")
            .map_err(|e| row_err(t, 0, "polProcessID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "polProcessID", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                Ok(RunSpecPollutantProcessRow {
                    pol_process_id: pp
                        .get(i)
                        .ok_or_else(|| row_err(t, i, "polProcessID", "null value".into()))?,
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
                    rows.iter().map(|r| r.fuel_subtype_id).collect::<Vec<i32>>(),
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
        let fst = get_i32("fuelSubtypeID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(FuelFormulationRow {
                    fuel_formulation_id: ff.get(i).ok_or_else(|| null("fuelFormulationID"))?,
                    fuel_subtype_id: fst.get(i).ok_or_else(|| null("fuelSubtypeID"))?,
                })
            })
            .collect()
    }
}

impl TableRow for FuelSubtypeRow {
    fn table_name() -> &'static str {
        "FuelSubtype"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("fuelSubtypeID".into(), DataType::Int32),
            ("fuelTypeID".into(), DataType::Int32),
            ("carbonContent".into(), DataType::Float64),
            ("oxidationFraction".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "fuelSubtypeID".into(),
                    rows.iter().map(|r| r.fuel_subtype_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "fuelTypeID".into(),
                    rows.iter().map(|r| r.fuel_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "carbonContent".into(),
                    rows.iter().map(|r| r.carbon_content).collect::<Vec<f64>>(),
                )
                .into(),
                Series::new(
                    "oxidationFraction".into(),
                    rows.iter()
                        .map(|r| r.oxidation_fraction)
                        .collect::<Vec<f64>>(),
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
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .f64()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let fst = get_i32("fuelSubtypeID")?;
        let ft = get_i32("fuelTypeID")?;
        let cc = get_f64("carbonContent")?;
        let ox = get_f64("oxidationFraction")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(FuelSubtypeRow {
                    fuel_subtype_id: fst.get(i).ok_or_else(|| null("fuelSubtypeID"))?,
                    fuel_type_id: ft.get(i).ok_or_else(|| null("fuelTypeID"))?,
                    carbon_content: cc.get(i).ok_or_else(|| null("carbonContent"))?,
                    oxidation_fraction: ox.get(i).ok_or_else(|| null("oxidationFraction"))?,
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

impl TableRow for Co2EqPollutantRow {
    fn table_name() -> &'static str {
        "Pollutant"
    }
    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("pollutantID".into(), DataType::Int32),
            ("globalWarmingPotential".into(), DataType::Int32),
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
                    "globalWarmingPotential".into(),
                    rows.iter()
                        .map(|r| r.global_warming_potential)
                        .collect::<Vec<i32>>(),
                )
                .into(),
            ],
        )
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "Pollutant";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let poll = get_i32("pollutantID")?;
        let gwp = get_i32("globalWarmingPotential")?;
        // The SQL extracts `Pollutant` filtered to a non-null, strictly-positive
        // `globalWarmingPotential` (only CO2-equivalent species — CO2, CH4, N2O
        // — carry one; every other pollutant row has NULL). The port reads the
        // raw `Pollutant` table, so apply that WHERE clause here: skip rows whose
        // `globalWarmingPotential` is NULL or non-positive rather than erroring.
        (0..df.height())
            .filter_map(|i| {
                let pollutant_id = poll.get(i)?;
                let g = gwp.get(i)?;
                if g <= 0 {
                    return None;
                }
                Some(Ok(Co2EqPollutantRow {
                    pollutant_id,
                    global_warming_potential: g,
                }))
            })
            .collect()
    }
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
            ("regClassID".into(), DataType::Int32),
            ("fuelTypeID".into(), DataType::Int32),
            ("modelYearID".into(), DataType::Int32),
            ("roadTypeID".into(), DataType::Int32),
            ("emissionQuant".into(), DataType::Float64),
            ("emissionRate".into(), DataType::Float64),
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
                    "regClassID".into(),
                    rows.iter().map(|r| r.reg_class_id).collect::<Vec<i32>>(),
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
                Series::new(
                    "emissionRate".into(),
                    rows.iter().map(|r| r.emission_rate).collect::<Vec<f64>>(),
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
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .f64()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let yr = get_i32("yearID")?;
        let mo = get_i32("monthID")?;
        let day = get_i32("dayID")?;
        let hr = get_i32("hourID")?;
        let state = get_i32("stateID")?;
        let county = get_i32("countyID")?;
        let zone = get_i32("zoneID")?;
        let link = get_i32("linkID")?;
        let poll = get_i32("pollutantID")?;
        let proc = get_i32("processID")?;
        let st = get_i32("sourceTypeID")?;
        let rc = get_i32("regClassID")?;
        let ft = get_i32("fuelTypeID")?;
        let my = get_i32("modelYearID")?;
        let rt = get_i32("roadTypeID")?;
        let eq = get_f64("emissionQuant")?;
        let er = get_f64("emissionRate")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(WorkerOutputRow {
                    year_id: yr.get(i).ok_or_else(|| null("yearID"))?,
                    month_id: mo.get(i).ok_or_else(|| null("monthID"))?,
                    day_id: day.get(i).ok_or_else(|| null("dayID"))?,
                    hour_id: hr.get(i).ok_or_else(|| null("hourID"))?,
                    state_id: state.get(i).ok_or_else(|| null("stateID"))?,
                    county_id: county.get(i).ok_or_else(|| null("countyID"))?,
                    zone_id: zone.get(i).ok_or_else(|| null("zoneID"))?,
                    link_id: link.get(i).ok_or_else(|| null("linkID"))?,
                    pollutant_id: poll.get(i).ok_or_else(|| null("pollutantID"))?,
                    process_id: proc.get(i).ok_or_else(|| null("processID"))?,
                    source_type_id: st.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    reg_class_id: rc.get(i).ok_or_else(|| null("regClassID"))?,
                    fuel_type_id: ft.get(i).ok_or_else(|| null("fuelTypeID"))?,
                    model_year_id: my.get(i).ok_or_else(|| null("modelYearID"))?,
                    road_type_id: rt.get(i).ok_or_else(|| null("roadTypeID"))?,
                    emission_quant: eq.get(i).ok_or_else(|| null("emissionQuant"))?,
                    emission_rate: er.get(i).ok_or_else(|| null("emissionRate"))?,
                })
            })
            .collect()
    }
}

/// Inputs to [`CO2AERunningStartExtendedIdleCalculator::calculate`] — the
/// tables the SQL's "Extract Data" section produces plus the two
/// RunSpec-derived process filters (see the module documentation).
///
/// A future (`DataFrameStore`) wiring populates this from the per-run
/// filtered execution database; until then it is the explicit data-plane
/// contract the unit tests build directly.
#[derive(Debug, Clone, Default)]
pub struct Co2aeInputs {
 /// `FuelSupply` rows (single fuel region).
    pub fuel_supply: Vec<FuelSupplyRow>,
 /// `FuelFormulation` rows.
    pub fuel_formulation: Vec<FuelFormulationRow>,
 /// `FuelSubtype` rows.
    pub fuel_subtype: Vec<FuelSubtypeRow>,
 /// `Year` rows (single calendar year).
    pub year: Vec<YearRow>,
 /// `MonthOfAnyYear` rows — the `monthID → monthGroupID` mapping.
    pub month_of_any_year: Vec<MonthGroupRow>,
 /// `CO2EqPollutant` rows — global warming potentials, pre-filtered to a
 /// positive potential. A pollutant absent here has no row to `INNER JOIN`,
 /// so its emissions are dropped from Step 2.
    pub co2_eq_pollutant: Vec<Co2EqPollutantRow>,
 /// `MOVESWorkerOutput` rows — the Total Energy Consumption rows Step 1a
 /// reads and the methane / nitrous-oxide rows Step 2 reads. Any other
 /// pollutant present is ignored.
    pub worker_output: Vec<WorkerOutputRow>,
 /// Step 1a process filter — the processes `##CO2Step1AprocessIDs##`
 /// expands to. An empty list is the SQL's never-true `1=2`: no Atmospheric
 /// CO2 is produced.
    pub step1a_process_ids: Vec<i32>,
 /// Step 2 process filter — the processes `##CO2Step2processIDs##` expands
 /// to. An empty list is the SQL's never-true `1=2`: no CO2 Equivalent is
 /// produced.
    pub step2_process_ids: Vec<i32>,
}

/// Output of [`CO2AERunningStartExtendedIdleCalculator::calculate`] — the two
/// sets of rows the SQL's two processing steps insert into `MOVESWorkerOutput`.
///
/// Both vectors are sorted by the `WorkerOutputRow::dimension_key` method for
/// deterministic output.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Co2aeOutput {
 /// Atmospheric CO2 (pollutant 90) rows — Step 1a output.
    pub atmospheric_co2: Vec<WorkerOutputRow>,
 /// CO2 Equivalent (pollutant 98) rows — Step 2 output.
    pub co2_equivalent: Vec<WorkerOutputRow>,
}

/// A `CarbonOxidationByFuelType` cell — the market-share-weighted fuel
/// properties of one `(yearID, monthGroupID, fuelTypeID)` group.
#[derive(Debug, Clone, Copy, Default)]
struct CarbonOxidationCell {
 /// `Σ marketShare × carbonContent`.
    sum_carbon_content: f64,
 /// `Σ marketShare × oxidationFraction`.
    sum_oxidation_fraction: f64,
}

/// Build the SQL's `CarbonOxidationByFuelType` working table.
///
/// `sumCarbonContent = Σ marketShare × carbonContent` and
/// `sumOxidationFraction = Σ marketShare × oxidationFraction`, grouped by
/// `(yearID, monthGroupID, fuelTypeID)` over the fuel supply joined
/// `FuelFormulation → FuelSubtype` and to `Year`. The SQL also groups by
/// `fuelRegionID`, but the extract is single-region so that column is constant;
/// `countyID` and `pollutantID` are likewise literals and are not keyed on.
/// Every join is an `INNER JOIN` — a fuel-supply row that misses any hop is
/// dropped.
fn carbon_oxidation_by_fuel_type(
    inputs: &Co2aeInputs,
) -> FxHashMap<(i32, i32, i32), CarbonOxidationCell> {
    let formulation: FxHashMap<i32, &FuelFormulationRow> = inputs
        .fuel_formulation
        .iter()
        .map(|ff| (ff.fuel_formulation_id, ff))
        .collect();
    let subtype: FxHashMap<i32, &FuelSubtypeRow> = inputs
        .fuel_subtype
        .iter()
        .map(|fst| (fst.fuel_subtype_id, fst))
        .collect();
 // `Year` resolves fuelYearID → yearID; the extract carries one calendar year.
    let year_of_fuel_year: FxHashMap<i32, i32> = inputs
        .year
        .iter()
        .map(|y| (y.fuel_year_id, y.year_id))
        .collect();

    let mut cells: FxHashMap<(i32, i32, i32), CarbonOxidationCell> = FxHashMap::default();
    for fs in &inputs.fuel_supply {
 // INNER JOIN FuelFormulation ON fuelFormulationID.
        let Some(ff) = formulation.get(&fs.fuel_formulation_id) else {
            continue;
        };
 // INNER JOIN FuelSubtype ON fuelSubtypeID.
        let Some(fst) = subtype.get(&ff.fuel_subtype_id) else {
            continue;
        };
 // INNER JOIN Year ON Year.fuelYearID = FuelSupply.fuelYearID.
        let Some(&year_id) = year_of_fuel_year.get(&fs.fuel_year_id) else {
            continue;
        };
        let cell = cells
            .entry((year_id, fs.month_group_id, fst.fuel_type_id))
            .or_default();
        cell.sum_carbon_content += fs.market_share * fst.carbon_content;
        cell.sum_oxidation_fraction += fs.market_share * fst.oxidation_fraction;
    }
    cells
}

/// Step 1a — Atmospheric CO2 from Total Energy Consumption.
///
/// `emissionQuant = Σ energy × sumCarbonContent × sumOxidationFraction × 44/12`,
/// summed over each dimension cell (`emissionRate` likewise). Joins each
/// Total Energy row to its `CarbonOxidationByFuelType` cell — keyed
/// `(yearID, monthGroupID, fuelTypeID)`, the month group resolved through
/// `MonthOfAnyYear` — and keeps only the processes
/// [`Co2aeInputs::step1a_process_ids`] lists. Every join is an `INNER JOIN`.
fn atmospheric_co2_rows(
    inputs: &Co2aeInputs,
    carbon_oxidation: &FxHashMap<(i32, i32, i32), CarbonOxidationCell>,
) -> Vec<WorkerOutputRow> {
    let month_group_of_month: FxHashMap<i32, i32> = inputs
        .month_of_any_year
        .iter()
        .map(|m| (m.month_id, m.month_group_id))
        .collect();

    let mut groups: FxHashMap<[i32; 14], WorkerOutputRow> = FxHashMap::default();
    for w in &inputs.worker_output {
 // mwo.pollutantID = ##totalEnergyConsumptionID## (91).
        if w.pollutant_id != TOTAL_ENERGY_POLLUTANT_ID {
            continue;
        }
 // ##CO2Step1AprocessIDs## — an empty list is the never-true `1=2`.
        if !inputs.step1a_process_ids.contains(&w.process_id) {
            continue;
        }
 // INNER JOIN may ON may.monthID = mwo.monthID.
        let Some(&month_group_id) = month_group_of_month.get(&w.month_id) else {
            continue;
        };
 // INNER JOIN coft ON yearID, monthGroupID, fuelTypeID. countyID is the
 // single-county invariant — see the module documentation.
        let Some(cell) = carbon_oxidation.get(&(w.year_id, month_group_id, w.fuel_type_id)) else {
            continue;
        };
 // SUM(emission × sumCarbonContent × sumOxidationFraction × 44/12),
 // accumulated per dimension cell.
        let row = groups
            .entry(w.dimension_key())
            .or_insert_with(|| WorkerOutputRow {
                pollutant_id: ATMOSPHERIC_CO2_POLLUTANT_ID,
                emission_quant: 0.0,
                emission_rate: 0.0,
                ..*w
            });
        row.emission_quant += w.emission_quant
 * cell.sum_carbon_content
 * cell.sum_oxidation_fraction
 * CARBON_TO_CO2_MASS_RATIO;
        row.emission_rate += w.emission_rate
 * cell.sum_carbon_content
 * cell.sum_oxidation_fraction
 * CARBON_TO_CO2_MASS_RATIO;
    }

    let mut out: Vec<WorkerOutputRow> = groups.into_values().collect();
    out.sort_unstable_by_key(WorkerOutputRow::dimension_key);
    out
}

/// Step 2 — CO2 Equivalent from Atmospheric CO2, methane and nitrous oxide.
///
/// `emissionQuant = Σ emission × globalWarmingPotential`, summed over each
/// dimension cell (`emissionRate` likewise). The SQL runs after Step 1a's
/// `INSERT`, so it reads the original `MOVESWorkerOutput` rows **plus** the
/// freshly-inserted Atmospheric CO2 rows; `atmospheric_co2` carries the latter.
/// Keeps only the pollutants [`CO2_EQUIVALENT_INPUTS`] (`IN (90,5,6)`), the
/// processes [`Co2aeInputs::step2_process_ids`] lists, and rows whose pollutant
/// has a `CO2EqPollutant` potential to `INNER JOIN`.
fn co2_equivalent_rows(
    inputs: &Co2aeInputs,
    atmospheric_co2: &[WorkerOutputRow],
) -> Vec<WorkerOutputRow> {
    let gwp_of_pollutant: FxHashMap<i32, i32> = inputs
        .co2_eq_pollutant
        .iter()
        .map(|p| (p.pollutant_id, p.global_warming_potential))
        .collect();

    let mut groups: FxHashMap<[i32; 14], WorkerOutputRow> = FxHashMap::default();
    for w in inputs.worker_output.iter().chain(atmospheric_co2.iter()) {
 // mwo.pollutantID IN (##CO2Step2pollutantIDs##) — "90,5,6".
        if !CO2_EQUIVALENT_INPUTS.contains(&w.pollutant_id) {
            continue;
        }
 // ##CO2Step2processIDs## — an empty list is the never-true `1=2`.
        if !inputs.step2_process_ids.contains(&w.process_id) {
            continue;
        }
 // INNER JOIN CO2EqPollutant ON pollutantID — drops a pollutant with no
 // (positive) global warming potential.
        let Some(&gwp) = gwp_of_pollutant.get(&w.pollutant_id) else {
            continue;
        };
        let gwp = f64::from(gwp);
 // SUM(emission × globalWarmingPotential), accumulated per dimension cell.
        let row = groups
            .entry(w.dimension_key())
            .or_insert_with(|| WorkerOutputRow {
                pollutant_id: CO2_EQUIVALENT_POLLUTANT_ID,
                emission_quant: 0.0,
                emission_rate: 0.0,
                ..*w
            });
        row.emission_quant += w.emission_quant * gwp;
        row.emission_rate += w.emission_rate * gwp;
    }

    let mut out: Vec<WorkerOutputRow> = groups.into_values().collect();
    out.sort_unstable_by_key(WorkerOutputRow::dimension_key);
    out
}

/// The MOVES Atmospheric CO2 / CO2 Equivalent calculator for running, start,
/// extended-idle and auxiliary-power exhaust.
///
/// A zero-sized value type: it owns no per-run state, exactly as the
/// [`Calculator`] trait contract requires. All run-varying input flows through
/// the [`Co2aeInputs`] argument to [`calculate`](Self::calculate).
#[derive(Debug, Clone, Copy, Default)]
pub struct CO2AERunningStartExtendedIdleCalculator;

impl CO2AERunningStartExtendedIdleCalculator {
 /// Stable module name — matches the Java class and the chain-DAG entry.
    pub const NAME: &'static str = CALCULATOR_NAME;

 /// Compute the Atmospheric CO2 and CO2 Equivalent rows — the port of the
 /// `CO2AERunningStartExtendedIdleCalculator.sql` "Processing" section.
 ///
 /// Runs the two ordered steps: Step 1a derives Atmospheric CO2 from Total
 /// Energy Consumption, then Step 2 derives CO2 Equivalent from Atmospheric
 /// CO2, methane and nitrous oxide — consuming Step 1a's output, exactly as
 /// the SQL reads back `MOVESWorkerOutput` after inserting it. See the
 /// [module documentation](self) for the algorithm and fidelity notes.
    #[must_use]
    pub fn calculate(&self, inputs: &Co2aeInputs) -> Co2aeOutput {
        let carbon_oxidation = carbon_oxidation_by_fuel_type(inputs);
        let atmospheric_co2 = atmospheric_co2_rows(inputs, &carbon_oxidation);
        let co2_equivalent = co2_equivalent_rows(inputs, &atmospheric_co2);
        Co2aeOutput {
            atmospheric_co2,
            co2_equivalent,
        }
    }
}

/// `CO2AERunningStartExtendedIdleCalculator` is a chained calculator/// `subscribes_directly: false` in `calculator-dag.json` — so it declares no
/// MasterLoop subscription.
static NO_SUBSCRIPTIONS: &[CalculatorSubscription] = &[];

/// The eight `(pollutant, process)` pairs the calculator registers.
///
/// Atmospheric CO2 (90) and CO2 Equivalent (98), each for the running (1),
/// start (2), extended-idle (90) and auxiliary-power (91) exhaust processes/// the eight `Registration` directives recorded for this calculator in
/// `CalculatorInfo.txt` (`registrations_count: 8` in `calculator-dag.json`).
///
/// The Java constructor additionally registers CO2 Equivalent for the three
/// crankcase processes (15 / 16 / 17); `CalculatorInfo.txt` does **not**, so
/// the runtime registry never wires them. Per the rule, the
/// `CalculatorInfo.txt` set is authoritative — the port registers the eight.
static REGISTRATIONS: &[PollutantProcessAssociation] = &[
    PollutantProcessAssociation {
        pollutant_id: PollutantId(90),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(98),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(90),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(98),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(90),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(98),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(90),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(98),
        process_id: ProcessId(91),
    },
];

/// The upstream calculators this one chains off — `calculator-dag.json` records
/// `depends_on: ["BaseRateCalculator", "CrankcaseEmissionCalculatorNonPM",
/// "HCSpeciationCalculator"]`. `BaseRateCalculator` produces the Total Energy
/// Consumption (91) records Step 1a consumes; the other two produce the methane
/// and nitrous-oxide records Step 2 consumes.
static UPSTREAM: &[&str] = &[
    "BaseRateCalculator",
    "CrankcaseEmissionCalculatorNonPM",
    "HCSpeciationCalculator",
];

/// Default-DB / scratch tables the CO2AE computation consumes — the tables the
/// SQL's "Extract Data" section pulls that feed the "Processing" section.
///
/// `MOVESWorkerOutput` carries the upstream calculators' Total Energy
/// Consumption, methane and nitrous-oxide rows; `Pollutant` is the source of
/// the global warming potentials the SQL pre-aggregates into `CO2EqPollutant`.
static INPUT_TABLES: &[&str] = &[
    "FuelFormulation",
    "FuelSubtype",
    "FuelSupply",
    "MOVESWorkerOutput",
    "MonthOfAnyYear",
    "Pollutant",
    "Year",
];

impl Calculator for CO2AERunningStartExtendedIdleCalculator {
    fn name(&self) -> &'static str {
        Self::NAME
    }

 /// A chained calculator: it does not subscribe to the MasterLoop directly
 /// but fires when its upstream calculators do. `calculator-dag.json`
 /// records `subscribes_directly: false` and an empty `subscriptions` list.
    fn subscriptions(&self) -> &[CalculatorSubscription] {
        NO_SUBSCRIPTIONS
    }

    fn registrations(&self) -> &[PollutantProcessAssociation] {
        REGISTRATIONS
    }

 /// Chains off `BaseRateCalculator`, `CrankcaseEmissionCalculatorNonPM` and
 /// `HCSpeciationCalculator` — `calculator-dag.json` `depends_on`.
    fn upstream(&self) -> &[&'static str] {
        UPSTREAM
    }

    fn input_tables(&self) -> &[&'static str] {
        INPUT_TABLES
    }

    fn execute(&self, ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
        let tables = ctx.tables();
        let rs_pol_processes =
            tables.iter_typed::<RunSpecPollutantProcessRow>("RunSpecPollutantProcess")?;
        let step1a_process_ids: Vec<i32> = rs_pol_processes
            .iter()
            .filter(|r| r.pol_process_id / 100 == ATMOSPHERIC_CO2_POLLUTANT_ID)
            .map(|r| r.pol_process_id % 100)
            .collect();
        let step2_process_ids: Vec<i32> = rs_pol_processes
            .iter()
            .filter(|r| r.pol_process_id / 100 == CO2_EQUIVALENT_POLLUTANT_ID)
            .map(|r| r.pol_process_id % 100)
            .collect();
        let inputs = Co2aeInputs {
            fuel_supply: tables.iter_typed::<FuelSupplyRow>("FuelSupply")?,
            fuel_formulation: tables.iter_typed::<FuelFormulationRow>("FuelFormulation")?,
            fuel_subtype: tables.iter_typed::<FuelSubtypeRow>("FuelSubtype")?,
            year: tables.iter_typed::<YearRow>("Year")?,
            month_of_any_year: tables.iter_typed::<MonthGroupRow>("MonthOfAnyYear")?,
            co2_eq_pollutant: tables.iter_typed::<Co2EqPollutantRow>("Pollutant")?,
            worker_output: tables.iter_typed::<WorkerOutputRow>("MOVESWorkerOutput")?,
            step1a_process_ids,
            step2_process_ids,
        };
        let output = self.calculate(&inputs);
        let mut rows = output.atmospheric_co2;
        rows.extend(output.co2_equivalent);
        crate::wiring::emit_rows(rows)
    }
}

/// Construct the calculator as a boxed trait object — matches the engine's
/// calculator-factory signature so the registry can register it.
#[must_use]
pub fn factory() -> Box<dyn Calculator> {
    Box::new(CO2AERunningStartExtendedIdleCalculator)
}

#[cfg(test)]
mod tests {
    use super::*;

 /// Build a one-fuel / one-energy-row input.
 ///
 /// The single Total Energy row drives the whole chain: Step 1a turns it
 /// into one Atmospheric CO2 row, which Step 2 turns into one CO2 Equivalent
 /// row. The fuel cell is `sumCarbonContent = 2.0`, `sumOxidationFraction =
 /// 0.5`, so the Step 1a factor is `2.0 × 0.5 × 44/12 = 44/12`:
 ///
 /// * `atmosphericCO2.emissionQuant = 12.0 × 44/12 = 44.0`
 /// * `atmosphericCO2.emissionRate = 6.0 × 44/12 = 22.0`
 ///
 /// and with `globalWarmingPotential(90) = 1`, CO2 Equivalent equals the
 /// Atmospheric CO2 it consumes — `44.0` / `22.0`.
    fn minimal_inputs() -> Co2aeInputs {
        Co2aeInputs {
            fuel_supply: vec![FuelSupplyRow {
                fuel_year_id: 2020,
                month_group_id: 3,
                fuel_formulation_id: 100,
                market_share: 1.0,
            }],
            fuel_formulation: vec![FuelFormulationRow {
                fuel_formulation_id: 100,
                fuel_subtype_id: 21,
            }],
            fuel_subtype: vec![FuelSubtypeRow {
                fuel_subtype_id: 21,
                fuel_type_id: 2,
                carbon_content: 2.0,
                oxidation_fraction: 0.5,
            }],
            year: vec![YearRow {
                year_id: 2020,
                fuel_year_id: 2020,
            }],
            month_of_any_year: vec![MonthGroupRow {
                month_id: 7,
                month_group_id: 3,
            }],
            co2_eq_pollutant: vec![
                Co2EqPollutantRow {
                    pollutant_id: 90, // Atmospheric CO2 — the reference gas
                    global_warming_potential: 1,
                },
                Co2EqPollutantRow {
                    pollutant_id: 5, // Methane
                    global_warming_potential: 25,
                },
                Co2EqPollutantRow {
                    pollutant_id: 6, // Nitrous Oxide
                    global_warming_potential: 298,
                },
            ],
            worker_output: vec![WorkerOutputRow {
                pollutant_id: 91, // Total Energy Consumption
                process_id: 1,    // Running Exhaust
                year_id: 2020,
                month_id: 7,
                day_id: 5,
                hour_id: 8,
                state_id: 26,
                county_id: 26_161,
                zone_id: 261_610,
                link_id: 5001,
                source_type_id: 21,
                reg_class_id: 30,
                fuel_type_id: 2,
                model_year_id: 2018,
                road_type_id: 4,
                emission_quant: 12.0,
                emission_rate: 6.0,
            }],
            step1a_process_ids: vec![1],
            step2_process_ids: vec![1],
        }
    }

 /// Assert `actual` matches `expected` within `f64` slack — the
 /// FLOAT-column / `44÷12` fidelity notes mean the port computes in `f64`.
    fn assert_close(actual: f64, expected: f64) {
        assert!(
            (actual - expected).abs() < 1e-9,
            "{actual} != expected {expected}",
        );
    }

    #[test]
    fn calculate_minimal_input_produces_atmospheric_and_equivalent() {
        let out = CO2AERunningStartExtendedIdleCalculator.calculate(&minimal_inputs());
        assert_eq!(out.atmospheric_co2.len(), 1);
        assert_eq!(out.co2_equivalent.len(), 1);

        let a = out.atmospheric_co2[0];
 // The dimension cell is carried straight from the energy row; the
 // pollutant is relabelled to Atmospheric CO2.
        assert_eq!(a.pollutant_id, 90);
        assert_eq!(a.process_id, 1);
        assert_eq!(a.year_id, 2020);
        assert_eq!(a.month_id, 7);
        assert_eq!(a.day_id, 5);
        assert_eq!(a.hour_id, 8);
        assert_eq!(a.state_id, 26);
        assert_eq!(a.county_id, 26_161);
        assert_eq!(a.zone_id, 261_610);
        assert_eq!(a.link_id, 5001);
        assert_eq!(a.source_type_id, 21);
        assert_eq!(a.reg_class_id, 30);
        assert_eq!(a.fuel_type_id, 2);
        assert_eq!(a.model_year_id, 2018);
        assert_eq!(a.road_type_id, 4);
 // 12.0 × 2.0 × 0.5 × 44/12 and 6.0 × 2.0 × 0.5 × 44/12.
        assert_close(a.emission_quant, 44.0);
        assert_close(a.emission_rate, 22.0);

        let e = out.co2_equivalent[0];
        assert_eq!(e.pollutant_id, 98);
        assert_eq!(e.process_id, 1);
 // 44.0 × gwp(90)=1 and 22.0 × 1.
        assert_close(e.emission_quant, 44.0);
        assert_close(e.emission_rate, 22.0);
    }

    #[test]
    fn atmospheric_co2_applies_the_carbon_to_co2_mass_ratio() {
 // With sumCarbonContent × sumOxidationFraction = 1 and one unit of
 // energy, the Atmospheric CO2 is exactly the 44/12 mass ratio. The port
 // uses the exact f64 ratio, not MariaDB's 4-decimal 3.6667 — see the
 // module fidelity note.
        let mut inputs = minimal_inputs();
        inputs.fuel_subtype[0].carbon_content = 1.0;
        inputs.fuel_subtype[0].oxidation_fraction = 1.0;
        inputs.worker_output[0].emission_quant = 1.0;
        inputs.worker_output[0].emission_rate = 1.0;

        let out = CO2AERunningStartExtendedIdleCalculator.calculate(&inputs);
        assert_eq!(out.atmospheric_co2.len(), 1);
        assert_close(out.atmospheric_co2[0].emission_quant, 44.0 / 12.0);
        assert_close(out.atmospheric_co2[0].emission_rate, 44.0 / 12.0);
    }

    #[test]
    fn atmospheric_co2_weights_carbon_and_oxidation_by_market_share() {
 // Two formulations of fuel type 2 with unequal market shares; the
 // carbon-oxidation cell is a share-weighted sum, not a plain sum or an
 // average:
 // sumCarbonContent = 0.25×10 + 0.75×2 = 4.0
 // sumOxidationFraction = 0.25×1.0 + 0.75×0.2 = 0.4
 // emissionQuant = 12.0 × 4.0 × 0.4 × 44/12 = 1.6 × 44 = 70.4
        let mut inputs = minimal_inputs();
        inputs.fuel_supply = vec![
            FuelSupplyRow {
                fuel_year_id: 2020,
                month_group_id: 3,
                fuel_formulation_id: 100,
                market_share: 0.25,
            },
            FuelSupplyRow {
                fuel_year_id: 2020,
                month_group_id: 3,
                fuel_formulation_id: 101,
                market_share: 0.75,
            },
        ];
        inputs.fuel_formulation = vec![
            FuelFormulationRow {
                fuel_formulation_id: 100,
                fuel_subtype_id: 21,
            },
            FuelFormulationRow {
                fuel_formulation_id: 101,
                fuel_subtype_id: 22,
            },
        ];
        inputs.fuel_subtype = vec![
            FuelSubtypeRow {
                fuel_subtype_id: 21,
                fuel_type_id: 2,
                carbon_content: 10.0,
                oxidation_fraction: 1.0,
            },
            FuelSubtypeRow {
                fuel_subtype_id: 22,
                fuel_type_id: 2,
                carbon_content: 2.0,
                oxidation_fraction: 0.2,
            },
        ];

        let out = CO2AERunningStartExtendedIdleCalculator.calculate(&inputs);
        assert_eq!(out.atmospheric_co2.len(), 1);
        assert_close(out.atmospheric_co2[0].emission_quant, 70.4);
        assert_close(out.atmospheric_co2[0].emission_rate, 35.2); // 6 × 4.0 × 0.4 × 44/12
    }

    #[test]
    fn atmospheric_co2_sums_duplicate_dimension_rows() {
 // Two Total Energy rows with the identical dimension are summed into
 // one Atmospheric CO2 row — the SQL GROUP BY + SUM.
        let mut inputs = minimal_inputs();
        let cell = inputs.worker_output[0];
        inputs.worker_output.push(WorkerOutputRow {
            emission_quant: 3.0,
            emission_rate: 1.5,
            ..cell
        });

        let out = CO2AERunningStartExtendedIdleCalculator.calculate(&inputs);
        assert_eq!(out.atmospheric_co2.len(), 1);
 // (12.0 + 3.0) × 2.0 × 0.5 × 44/12 = 15.0 × 44/12 = 55.0.
        assert_close(out.atmospheric_co2[0].emission_quant, 55.0);
 // (6.0 + 1.5) × 44/12 = 7.5 × 44/12 = 27.5.
        assert_close(out.atmospheric_co2[0].emission_rate, 27.5);
    }

    #[test]
    fn atmospheric_co2_skips_process_not_in_step1a_filter() {
 // The energy row's process is absent from step1a_process_ids ([1]);
 // ##CO2Step1AprocessIDs## excludes it, so no Atmospheric CO2 — and so
 // no Step 2 input either.
        let mut inputs = minimal_inputs();
        inputs.worker_output[0].process_id = 2;

        let out = CO2AERunningStartExtendedIdleCalculator.calculate(&inputs);
        assert!(out.atmospheric_co2.is_empty());
        assert!(out.co2_equivalent.is_empty());
    }

    #[test]
    fn atmospheric_co2_empty_when_step1a_process_ids_empty() {
 // An empty step1a_process_ids is the SQL's never-true `1=2`.
        let mut inputs = minimal_inputs();
        inputs.step1a_process_ids.clear();

        let out = CO2AERunningStartExtendedIdleCalculator.calculate(&inputs);
        assert!(out.atmospheric_co2.is_empty());
        assert!(out.co2_equivalent.is_empty());
    }

    #[test]
    fn atmospheric_co2_skips_non_energy_rows() {
 // A worker-output row whose pollutant is not Total Energy Consumption
 // (91) and not a Step 2 input drives neither step.
        let mut inputs = minimal_inputs();
        inputs.worker_output[0].pollutant_id = 2; // CO, say

        let out = CO2AERunningStartExtendedIdleCalculator.calculate(&inputs);
        assert!(out.atmospheric_co2.is_empty());
        assert!(out.co2_equivalent.is_empty());
    }

    #[test]
    fn atmospheric_co2_dropped_without_a_carbon_oxidation_cell() {
 // Each INNER JOIN feeding CarbonOxidationByFuelType, dropped in turn // no cell, so the Step 1a join drops the energy row.
        let mut no_supply = minimal_inputs();
        no_supply.fuel_supply.clear();
        assert!(CO2AERunningStartExtendedIdleCalculator
            .calculate(&no_supply)
            .atmospheric_co2
            .is_empty());

        let mut no_formulation = minimal_inputs();
        no_formulation.fuel_formulation.clear();
        assert!(CO2AERunningStartExtendedIdleCalculator
            .calculate(&no_formulation)
            .atmospheric_co2
            .is_empty());

        let mut no_subtype = minimal_inputs();
        no_subtype.fuel_subtype.clear();
        assert!(CO2AERunningStartExtendedIdleCalculator
            .calculate(&no_subtype)
            .atmospheric_co2
            .is_empty());

        let mut no_year = minimal_inputs();
        no_year.year.clear();
        assert!(CO2AERunningStartExtendedIdleCalculator
            .calculate(&no_year)
            .atmospheric_co2
            .is_empty());

 // The energy row's fuel type has no carbon-oxidation cell.
        let mut other_fuel_type = minimal_inputs();
        other_fuel_type.worker_output[0].fuel_type_id = 9;
        assert!(CO2AERunningStartExtendedIdleCalculator
            .calculate(&other_fuel_type)
            .atmospheric_co2
            .is_empty());
    }

    #[test]
    fn atmospheric_co2_dropped_without_a_month_group() {
 // The energy row's month is absent from MonthOfAnyYear — the
 // CO2MonthofAnyYear inner join drops it.
        let mut inputs = minimal_inputs();
        inputs.month_of_any_year.clear();
        assert!(CO2AERunningStartExtendedIdleCalculator
            .calculate(&inputs)
            .atmospheric_co2
            .is_empty());
    }

    #[test]
    fn co2_equivalent_consumes_freshly_computed_atmospheric_co2() {
 // The worker output carries only Total Energy — no pollutant-90, -5 or
 // -6 row. CO2 Equivalent is non-empty purely because Step 2 reads the
 // Atmospheric CO2 that Step 1a just produced and inserted.
        let inputs = minimal_inputs();
        assert!(inputs.worker_output.iter().all(|w| w.pollutant_id == 91));

        let out = CO2AERunningStartExtendedIdleCalculator.calculate(&inputs);
        assert_eq!(out.co2_equivalent.len(), 1);
        assert_close(out.co2_equivalent[0].emission_quant, 44.0);
    }

    #[test]
    fn co2_equivalent_sums_atmospheric_methane_and_nitrous_oxide() {
 // Add a methane and a nitrous-oxide row at the energy row's dimension
 // cell. Step 2 sums Atmospheric CO2 (from Step 1a), methane and nitrous
 // oxide, each weighted by its global warming potential:
 // quant = 44.0×1 + 2.0×25 + 1.0×298 = 44 + 50 + 298 = 392.0
 // rate = 22.0×1 + 1.0×25 + 1.0×298 = 22 + 25 + 298 = 345.0
        let mut inputs = minimal_inputs();
        let cell = inputs.worker_output[0];
        inputs.worker_output.push(WorkerOutputRow {
            pollutant_id: 5, // Methane
            emission_quant: 2.0,
            emission_rate: 1.0,
            ..cell
        });
        inputs.worker_output.push(WorkerOutputRow {
            pollutant_id: 6, // Nitrous Oxide
            emission_quant: 1.0,
            emission_rate: 1.0,
            ..cell
        });

        let out = CO2AERunningStartExtendedIdleCalculator.calculate(&inputs);
 // Step 1a sees only the one Total Energy row.
        assert_eq!(out.atmospheric_co2.len(), 1);
        assert_eq!(out.co2_equivalent.len(), 1);
        assert_close(out.co2_equivalent[0].emission_quant, 392.0);
        assert_close(out.co2_equivalent[0].emission_rate, 345.0);
    }

    #[test]
    fn co2_equivalent_skips_pollutant_without_a_global_warming_potential() {
 // Drop Atmospheric CO2's CO2EqPollutant row: Step 1a still produces the
 // 90 row, but Step 2's INNER JOIN finds no potential for it.
        let mut inputs = minimal_inputs();
        inputs.co2_eq_pollutant.retain(|p| p.pollutant_id != 90);

        let out = CO2AERunningStartExtendedIdleCalculator.calculate(&inputs);
        assert_eq!(out.atmospheric_co2.len(), 1);
        assert!(out.co2_equivalent.is_empty());
    }

    #[test]
    fn co2_equivalent_skips_process_not_in_step2_filter() {
 // ##CO2Step2processIDs## excludes the 90 row's process; Step 1a is
 // unaffected, Step 2 produces nothing.
        let mut inputs = minimal_inputs();
        inputs.step2_process_ids = vec![2];

        let out = CO2AERunningStartExtendedIdleCalculator.calculate(&inputs);
        assert_eq!(out.atmospheric_co2.len(), 1);
        assert!(out.co2_equivalent.is_empty());
    }

    #[test]
    fn co2_equivalent_excludes_total_energy_rows() {
 // Even given a global warming potential, a Total Energy (91) row is
 // excluded from Step 2 — the filter is `pollutantID IN (90,5,6)`, not
 // "has a potential". CO2 Equivalent stays the 90-only 44.0.
        let mut inputs = minimal_inputs();
        inputs.co2_eq_pollutant.push(Co2EqPollutantRow {
            pollutant_id: 91,
            global_warming_potential: 1000,
        });

        let out = CO2AERunningStartExtendedIdleCalculator.calculate(&inputs);
        assert_eq!(out.co2_equivalent.len(), 1);
        assert_close(out.co2_equivalent[0].emission_quant, 44.0);
    }

    #[test]
    fn output_is_sorted_by_dimension_key() {
 // Two Total Energy rows on distinct links yield two dimension cells;
 // each step's output comes back dimension-key sorted regardless of
 // input order.
        let mut inputs = minimal_inputs();
        let cell = inputs.worker_output[0];
        inputs.worker_output.insert(
            0,
            WorkerOutputRow {
                link_id: 9999, // sorts after link 5001
                ..cell
            },
        );

        let out = CO2AERunningStartExtendedIdleCalculator.calculate(&inputs);
        assert_eq!(out.atmospheric_co2.len(), 2);
        assert_eq!(out.co2_equivalent.len(), 2);
        assert!(
            out.atmospheric_co2
                .windows(2)
                .all(|w| w[0].dimension_key() <= w[1].dimension_key()),
            "atmospheric_co2 is not sorted by dimension key",
        );
        assert!(
            out.co2_equivalent
                .windows(2)
                .all(|w| w[0].dimension_key() <= w[1].dimension_key()),
            "co2_equivalent is not sorted by dimension key",
        );
        assert_eq!(out.atmospheric_co2[0].link_id, 5001);
        assert_eq!(out.atmospheric_co2[1].link_id, 9999);
    }

    #[test]
    fn calculate_empty_input_yields_empty_output() {
        let out = CO2AERunningStartExtendedIdleCalculator.calculate(&Co2aeInputs::default());
        assert_eq!(out, Co2aeOutput::default());
    }

    #[test]
    fn calculator_name_matches_dag_module() {
        assert_eq!(
            CO2AERunningStartExtendedIdleCalculator.name(),
            "CO2AERunningStartExtendedIdleCalculator"
        );
        assert_eq!(
            CO2AERunningStartExtendedIdleCalculator::NAME,
            "CO2AERunningStartExtendedIdleCalculator"
        );
    }

    #[test]
    fn calculator_is_chained_with_no_subscriptions() {
 // calculator-dag.json: subscribes_directly false, subscriptions [].
        assert!(CO2AERunningStartExtendedIdleCalculator
            .subscriptions()
            .is_empty());
    }

    #[test]
    fn registrations_match_the_eight_calculator_info_directives() {
 // calculator-dag.json records registrations_count 8: Atmospheric CO2
 // (90) and CO2 Equivalent (98), each for the running (1), start (2),
 // extended-idle (90) and aux-power (91) exhaust processes.
        let regs = CO2AERunningStartExtendedIdleCalculator.registrations();
        assert_eq!(regs.len(), 8);
        assert!(regs
            .iter()
            .all(|r| r.pollutant_id == PollutantId(90) || r.pollutant_id == PollutantId(98)));
        let mut procs: Vec<u16> = regs.iter().map(|r| r.process_id.0).collect();
        procs.sort_unstable();
        procs.dedup();
        assert_eq!(procs, vec![1, 2, 90, 91]);
 // Each process carries both the Atmospheric CO2 and CO2 Equivalent pair.
        for p in [1u16, 2, 90, 91] {
            assert_eq!(regs.iter().filter(|r| r.process_id.0 == p).count(), 2);
        }
    }

    #[test]
    fn calculator_chains_off_three_upstream_calculators() {
 // calculator-dag.json records depends_on with these three.
        assert_eq!(
            CO2AERunningStartExtendedIdleCalculator.upstream(),
            &[
                "BaseRateCalculator",
                "CrankcaseEmissionCalculatorNonPM",
                "HCSpeciationCalculator",
            ]
        );
    }

    #[test]
    fn calculator_declares_input_tables() {
        let tables = CO2AERunningStartExtendedIdleCalculator.input_tables();
        for expected in [
            "FuelFormulation",
            "FuelSubtype",
            "FuelSupply",
            "MOVESWorkerOutput",
            "MonthOfAnyYear",
            "Pollutant",
            "Year",
        ] {
            assert!(tables.contains(&expected), "missing input table {expected}");
        }
    }

    #[test]
    fn execute_returns_nonempty_dataframe_for_minimal_inputs() {
        use moves_framework::{DataFrameStore, InMemoryStore};
        let inputs = minimal_inputs();
        let mut store = InMemoryStore::new();
        store.insert(
            "FuelSupply",
            FuelSupplyRow::into_dataframe(inputs.fuel_supply.clone()).unwrap(),
        );
        store.insert(
            "FuelFormulation",
            FuelFormulationRow::into_dataframe(inputs.fuel_formulation.clone()).unwrap(),
        );
        store.insert(
            "FuelSubtype",
            FuelSubtypeRow::into_dataframe(inputs.fuel_subtype.clone()).unwrap(),
        );
        store.insert(
            "Year",
            YearRow::into_dataframe(inputs.year.clone()).unwrap(),
        );
        store.insert(
            "MonthOfAnyYear",
            MonthGroupRow::into_dataframe(inputs.month_of_any_year.clone()).unwrap(),
        );
        store.insert(
            "Pollutant",
            Co2EqPollutantRow::into_dataframe(inputs.co2_eq_pollutant.clone()).unwrap(),
        );
        store.insert(
            "MOVESWorkerOutput",
            WorkerOutputRow::into_dataframe(inputs.worker_output.clone()).unwrap(),
        );
 // RunSpecPollutantProcess: pollutant 90 (AtmosphericCO2) for process 1 → 90*100+1=9001
 // pollutant 98 (CO2Equivalent) for process 1 → 98*100+1=9801
        store.insert(
            "RunSpecPollutantProcess",
            RunSpecPollutantProcessRow::into_dataframe(vec![
                RunSpecPollutantProcessRow {
                    pol_process_id: 90 * 100 + 1,
                },
                RunSpecPollutantProcessRow {
                    pol_process_id: 98 * 100 + 1,
                },
            ])
            .unwrap(),
        );
        let ctx = CalculatorContext::with_tables(store);
        let out = CO2AERunningStartExtendedIdleCalculator
            .execute(&ctx)
            .expect("execute ok");
        let df = out.dataframe().expect("output should contain a DataFrame");
        assert!(
            df.height() > 0,
            "execute must return at least one output row"
        );
    }

    #[test]
    fn factory_builds_a_named_calculator() {
        assert_eq!(factory().name(), "CO2AERunningStartExtendedIdleCalculator");
    }

    #[test]
    fn calculator_is_object_safe() {
 // The registry stores calculators as Box<dyn Calculator>.
        let calc: Box<dyn Calculator> = Box::new(CO2AERunningStartExtendedIdleCalculator);
        assert_eq!(calc.name(), "CO2AERunningStartExtendedIdleCalculator");
    }
}
