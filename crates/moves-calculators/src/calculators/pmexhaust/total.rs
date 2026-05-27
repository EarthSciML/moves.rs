//! `PMTotalExhaustCalculator` — the total-exhaust-PM half of Phase 3 Task 53
//! (the running-exhaust-PM half is [`super::running`]).
//!
//! Pure-Rust port of `PMTotalExhaustCalculator.java` and its companion
//! `database/PMTotalExhaustCalculator.sql`.
//!
//! # What this calculator does
//!
//! MOVES reports particulate matter in two size classes — PM10 (pollutant
//! 100) and PM2.5 (pollutant 110) — and each is the *sum* of three chemical
//! components produced upstream by other calculators:
//!
//! | Total | Pollutant | Components (pollutant ids) |
//! |-------|-----------|----------------------------|
//! | Primary Exhaust PM10 — Total  | 100 | organic carbon 101, elemental carbon 102, sulfate 105 |
//! | Primary Exhaust PM2.5 — Total | 110 | organic carbon 111, elemental carbon 112, sulfate 115 |
//!
//! `PMTotalExhaustCalculator` is a **chained** calculator: it does not
//! subscribe to the master loop on its own. It runs after the calculators
//! that produce the OC / EC / sulfate component rows, and it forms each total
//! by *re-labelling* every component row with the total's pollutant id.
//!
//! The re-labelling does not itself add anything up — it copies each
//! component emission row verbatim, changing only `pollutantID` to 100 (for
//! the PM10 components) or 110 (for the PM2.5 components). The Java
//! constructor calls `ExecutionRunSpec.pollutantNeedsAggregation` for both
//! totals, which marks pollutants 100 and 110 for the output processor's
//! aggregation pass; that later pass collapses the re-labelled rows that
//! share a dimension key into one summed total. So this calculator's job is
//! purely the re-label; the addition is the aggregator's.
//!
//! # The SQL, verbatim
//!
//! `PMTotalExhaustCalculator.sql` has two gated sections. `PM10Total`:
//!
//! ```sql
//! insert into PMTotalMOVESWorkerOutputTemp (… columns …)
//! select …, 100 as pollutantID, … from MOVESWorkerOutput mwo
//! where mwo.pollutantID in (101,102,105);
//! ```
//!
//! and `PM25Total`, identical but `110 as pollutantID` and
//! `pollutantID in (111,112,115)`. The temp table is then appended back into
//! `MOVESWorkerOutput`. [`PmTotalExhaustCalculator::run`] reproduces exactly
//! this: filter the worker-output rows to the component pollutants, copy each
//! with the total's pollutant id, return the copies for the caller to append.
//!
//! Section `PM10Total` is emitted before `PM25Total`; [`run`](PmTotalExhaustCalculator::run)
//! keeps that order.
//!
//! # RunSpec gating
//!
//! `doExecute` enables the `PM10Total` section only when the RunSpec requests
//! `(PM10 Total, current process)`, and likewise for `PM25Total`; if neither
//! is requested it abandons the calculation. [`TotalSelection`] models that
//! per-section gate — the registry passes the selection implied by the
//! RunSpec, and an all-`false` selection yields no rows.
//!
//! # Chain metadata
//!
//! `characterization/calculator-chains/calculator-dag.json` records
//! `PMTotalExhaustCalculator` with `subscribes_directly: false`,
//! `subscriptions: []`, `registrations_count: 0`, and `depends_on: []`.
//! The DAG (like `CalculatorInfo.txt`) was captured from a `BaseRateCalculator`
//! run, in which this legacy non-base-rate calculator was never instantiated,
//! so it contributes no chain edges, registration pairs, or subscriptions to
//! the capture. The trait methods below echo that empty wiring; see each
//! method's doc comment for what the Java does at runtime.
//!
//! # Data plane (Task 50)
//!
//! [`Calculator::execute`] is a shell. Its [`CalculatorContext`] exposes only
//! the Phase 2 placeholder `ExecutionTables` / `ScratchNamespace`, which have
//! no row storage, so `execute` cannot read the upstream component rows nor
//! append the re-labelled rows. The faithful re-label is
//! [`PmTotalExhaustCalculator::run`], fully unit-tested; once the
//! `DataFrameStore` (migration-plan Task 50) lands, `execute` reads the
//! worker-output rows from the context, calls `run`, and appends the result.

use moves_data::PollutantProcessAssociation;
use moves_framework::{
    Calculator, CalculatorContext, CalculatorOutput, CalculatorSubscription, DataFrameStoreTyped,
    Error, TableRow,
};
use polars::prelude::{DataFrame, DataType, NamedFrom, PolarsResult, Schema, Series};

/// Stable module name in the calculator-chain DAG — matches the Java class.
const CALCULATOR_NAME: &str = "PMTotalExhaustCalculator";

/// `Primary Exhaust PM10 - Total` — pollutant 100, the total the
/// `PM10Total` section produces.
const PM10_TOTAL_POLLUTANT_ID: i32 = 100;
/// `Primary Exhaust PM2.5 - Total` — pollutant 110, the total the
/// `PM25Total` section produces.
const PM25_TOTAL_POLLUTANT_ID: i32 = 110;

/// The PM10 component pollutants summed into [`PM10_TOTAL_POLLUTANT_ID`]:
/// organic carbon (101), elemental carbon (102), sulfate (105). Matches the
/// `where mwo.pollutantID in (101,102,105)` of the `PM10Total` section.
const PM10_COMPONENT_POLLUTANT_IDS: [i32; 3] = [101, 102, 105];
/// The PM2.5 component pollutants summed into [`PM25_TOTAL_POLLUTANT_ID`]:
/// organic carbon (111), elemental carbon (112), sulfate (115). Matches the
/// `where mwo.pollutantID in (111,112,115)` of the `PM25Total` section.
const PM25_COMPONENT_POLLUTANT_IDS: [i32; 3] = [111, 112, 115];

/// One `MOVESWorkerOutput` emission row — the columns
/// `PMTotalExhaustCalculator.sql` copies through its `PMTotalMOVESWorkerOutputTemp`
/// temp table.
///
/// The calculator reads rows of this shape (the OC / EC / sulfate component
/// emissions other calculators produced) and writes rows of the same shape
/// (the totals). Every column is carried through unchanged except
/// [`pollutant_id`](Self::pollutant_id), which the re-label replaces.
///
/// The id columns are the MOVES `SMALLINT` / `INTEGER` dimension keys; they
/// are held as [`i32`] uniformly. `scc` is the optional `CHAR(10)` source
/// classification code, carried through opaquely. `emission_quant` is the
/// `FLOAT emissionQuant` column, held as [`f64`] (see the [`super::running`]
/// fidelity note on MOVES `FLOAT` columns).
#[derive(Debug, Clone, PartialEq)]
pub struct PmWorkerRow {
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
    /// `pollutantID` — the only column the re-label changes.
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
    /// `SCC` — the optional `CHAR(10)` source classification code.
    pub scc: Option<String>,
    /// `emissionQuant` — the emission quantity.
    pub emission_quant: f64,
}

impl PmWorkerRow {
    /// A copy of this row with `pollutantID` replaced by `pollutant_id` —
    /// the per-row work of the SQL `select …, <total> as pollutantID, …`.
    ///
    /// Every other column, `scc` included, is carried through verbatim.
    #[must_use]
    pub fn relabelled(&self, pollutant_id: i32) -> PmWorkerRow {
        PmWorkerRow {
            pollutant_id,
            ..self.clone()
        }
    }
}

/// Which PM totals to form — the `PM10Total` / `PM25Total` script sections
/// `doExecute` enables from the RunSpec.
///
/// `doExecute` turns on `PM10Total` when the RunSpec requests
/// `(PM10 Total, current process)` and `PM25Total` when it requests
/// `(PM2.5 Total, current process)`; with neither requested it abandons the
/// calculation. [`PmTotalExhaustCalculator::run`] honours the same gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TotalSelection {
    /// Enable the `PM10Total` section (re-label 101/102/105 → 100).
    pub pm10_total: bool,
    /// Enable the `PM25Total` section (re-label 111/112/115 → 110).
    pub pm25_total: bool,
}

impl TotalSelection {
    /// Both totals enabled — the common case for a run that requests PM.
    #[must_use]
    pub const fn both() -> Self {
        Self {
            pm10_total: true,
            pm25_total: true,
        }
    }

    /// `true` when at least one section is enabled. `doExecute` returns
    /// without work — `null` — when this is `false`.
    #[must_use]
    pub const fn any(self) -> bool {
        self.pm10_total || self.pm25_total
    }
}

/// The Total Exhaust PM calculator.
///
/// A zero-sized value type: the calculator owns no per-run state, as the
/// [`Calculator`] trait contract requires. All run-varying input flows
/// through [`PmTotalExhaustCalculator::run`]'s arguments.
#[derive(Debug, Clone, Copy, Default)]
pub struct PmTotalExhaustCalculator;

impl PmTotalExhaustCalculator {
    /// Chain-DAG name — matches the Java class and the `calculator-dag.json`
    /// entry.
    pub const NAME: &'static str = CALCULATOR_NAME;

    /// Construct the calculator.
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    /// Form the requested PM totals from a set of `MOVESWorkerOutput` rows.
    ///
    /// Ports the processing section of `PMTotalExhaustCalculator.sql`: for
    /// each enabled section, every `worker_output` row whose `pollutant_id`
    /// is one of that section's components is copied with the total's
    /// pollutant id. The returned rows are the ones the SQL appends back into
    /// `MOVESWorkerOutput`; the caller appends them likewise.
    ///
    /// The result is *not* aggregated — re-labelled component rows that share
    /// a dimension key are left as separate rows, exactly as the SQL leaves
    /// them. The output processor's aggregation pass (enabled for pollutants
    /// 100 and 110 by the Java constructor's `pollutantNeedsAggregation`
    /// calls) sums them later.
    ///
    /// `PM10Total` rows precede `PM25Total` rows, matching the section order
    /// in the script. Within a section the input order is preserved.
    #[must_use]
    pub fn run(&self, worker_output: &[PmWorkerRow], select: TotalSelection) -> Vec<PmWorkerRow> {
        let mut out = Vec::new();
        if select.pm10_total {
            for row in worker_output {
                if PM10_COMPONENT_POLLUTANT_IDS.contains(&row.pollutant_id) {
                    out.push(row.relabelled(PM10_TOTAL_POLLUTANT_ID));
                }
            }
        }
        if select.pm25_total {
            for row in worker_output {
                if PM25_COMPONENT_POLLUTANT_IDS.contains(&row.pollutant_id) {
                    out.push(row.relabelled(PM25_TOTAL_POLLUTANT_ID));
                }
            }
        }
        out
    }
}

fn row_err(table: &'static str, row: usize, column: &'static str, msg: String) -> moves_framework::Error {
    moves_framework::Error::RowExtraction { table: table.into(), row, column: column.into(), message: msg }
}

impl TableRow for PmWorkerRow {
    fn table_name() -> &'static str { "MOVESWorkerOutput" }
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
            ("SCC".into(), DataType::String),
            ("emissionQuant".into(), DataType::Float64),
        ])
    }
    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(n, vec![
            Series::new("yearID".into(), rows.iter().map(|r| r.year_id).collect::<Vec<i32>>()).into(),
            Series::new("monthID".into(), rows.iter().map(|r| r.month_id).collect::<Vec<i32>>()).into(),
            Series::new("dayID".into(), rows.iter().map(|r| r.day_id).collect::<Vec<i32>>()).into(),
            Series::new("hourID".into(), rows.iter().map(|r| r.hour_id).collect::<Vec<i32>>()).into(),
            Series::new("stateID".into(), rows.iter().map(|r| r.state_id).collect::<Vec<i32>>()).into(),
            Series::new("countyID".into(), rows.iter().map(|r| r.county_id).collect::<Vec<i32>>()).into(),
            Series::new("zoneID".into(), rows.iter().map(|r| r.zone_id).collect::<Vec<i32>>()).into(),
            Series::new("linkID".into(), rows.iter().map(|r| r.link_id).collect::<Vec<i32>>()).into(),
            Series::new("pollutantID".into(), rows.iter().map(|r| r.pollutant_id).collect::<Vec<i32>>()).into(),
            Series::new("processID".into(), rows.iter().map(|r| r.process_id).collect::<Vec<i32>>()).into(),
            Series::new("sourceTypeID".into(), rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>()).into(),
            Series::new("fuelTypeID".into(), rows.iter().map(|r| r.fuel_type_id).collect::<Vec<i32>>()).into(),
            Series::new("modelYearID".into(), rows.iter().map(|r| r.model_year_id).collect::<Vec<i32>>()).into(),
            Series::new("roadTypeID".into(), rows.iter().map(|r| r.road_type_id).collect::<Vec<i32>>()).into(),
            Series::new("SCC".into(), rows.iter().map(|r| r.scc.clone()).collect::<Vec<Option<String>>>()).into(),
            Series::new("emissionQuant".into(), rows.iter().map(|r| r.emission_quant).collect::<Vec<f64>>()).into(),
        ])
    }
    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "MOVESWorkerOutput";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.i32().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?.f64().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let year_id = get_i32("yearID")?;
        let month_id = get_i32("monthID")?;
        let day_id = get_i32("dayID")?;
        let hour_id = get_i32("hourID")?;
        let state_id = get_i32("stateID")?;
        let county_id = get_i32("countyID")?;
        let zone_id = get_i32("zoneID")?;
        let link_id = get_i32("linkID")?;
        let pollutant_id = get_i32("pollutantID")?;
        let process_id = get_i32("processID")?;
        let source_type_id = get_i32("sourceTypeID")?;
        let fuel_type_id = get_i32("fuelTypeID")?;
        let model_year_id = get_i32("modelYearID")?;
        let road_type_id = get_i32("roadTypeID")?;
        let scc_ca = df.column("SCC").map_err(|e| row_err(t, 0, "SCC", e.to_string()))?.str().map_err(|e| row_err(t, 0, "SCC", e.to_string()))?;
        let emission_quant = get_f64("emissionQuant")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(PmWorkerRow {
                year_id: year_id.get(i).ok_or_else(|| null("yearID"))?,
                month_id: month_id.get(i).ok_or_else(|| null("monthID"))?,
                day_id: day_id.get(i).ok_or_else(|| null("dayID"))?,
                hour_id: hour_id.get(i).ok_or_else(|| null("hourID"))?,
                state_id: state_id.get(i).ok_or_else(|| null("stateID"))?,
                county_id: county_id.get(i).ok_or_else(|| null("countyID"))?,
                zone_id: zone_id.get(i).ok_or_else(|| null("zoneID"))?,
                link_id: link_id.get(i).ok_or_else(|| null("linkID"))?,
                pollutant_id: pollutant_id.get(i).ok_or_else(|| null("pollutantID"))?,
                process_id: process_id.get(i).ok_or_else(|| null("processID"))?,
                source_type_id: source_type_id.get(i).ok_or_else(|| null("sourceTypeID"))?,
                fuel_type_id: fuel_type_id.get(i).ok_or_else(|| null("fuelTypeID"))?,
                model_year_id: model_year_id.get(i).ok_or_else(|| null("modelYearID"))?,
                road_type_id: road_type_id.get(i).ok_or_else(|| null("roadTypeID"))?,
                scc: scc_ca.get(i).map(|s| s.to_string()),
                emission_quant: emission_quant.get(i).ok_or_else(|| null("emissionQuant"))?,
            })
        }).collect()
    }
}

/// `PMTotalExhaustCalculator` carries no master-loop subscription: it is a
/// chained calculator. See [`Calculator::subscriptions`].
static NO_SUBSCRIPTIONS: &[CalculatorSubscription] = &[];

/// `PMTotalExhaustCalculator` contributes no `Registration` pairs to the
/// chain-DAG capture. See [`Calculator::registrations`].
static NO_REGISTRATIONS: &[PollutantProcessAssociation] = &[];

impl Calculator for PmTotalExhaustCalculator {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    /// Empty — `PMTotalExhaustCalculator` is a *chained* calculator. Its Java
    /// `subscribeToMe` states "this is a chained calculator, so don't
    /// subscribe to the MasterLoop"; it instead attaches itself downstream of
    /// the calculators producing the OC / EC / sulfate component pollutants.
    /// `calculator-dag.json` records `subscribes_directly: false` with an
    /// empty `subscriptions` list.
    fn subscriptions(&self) -> &[CalculatorSubscription] {
        NO_SUBSCRIPTIONS
    }

    /// Empty — `calculator-dag.json` records `registrations_count: 0`.
    ///
    /// The Java constructor registers `Primary Exhaust PM10 - Total` (100)
    /// and `Primary Exhaust PM2.5 - Total` (110) for *every* emission
    /// process — it loops over `EmissionProcess.getAllEmissionProcesses()`,
    /// a set resolved at runtime from the execution database, not a static
    /// list. The chain-DAG capture, taken from a `BaseRateCalculator` run
    /// that never loaded this legacy calculator, recorded no pairs; with no
    /// static, capture-grounded pair list to return, this method returns the
    /// empty slice the trait explicitly permits.
    fn registrations(&self) -> &[PollutantProcessAssociation] {
        NO_REGISTRATIONS
    }

    /// Empty — the calculator's SQL reads `MOVESWorkerOutput`, the run's
    /// accumulating emission output, not a default-database table.
    /// `calculator-dag.json` records `depends_on: []`; at runtime the Java
    /// `subscribeToMe` chains the calculator off whichever calculators
    /// produced the OC / EC / sulfate component pollutants, a set resolved
    /// per run.
    fn input_tables(&self) -> &[&'static str] {
        &[]
    }

    fn execute(&self, ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
        let rows: Vec<PmWorkerRow> = ctx.tables().iter_typed("MOVESWorkerOutput")?;
        let out_rows = self.run(&rows, TotalSelection::both());
        crate::wiring::emit_rows(out_rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a worker-output row carrying `pollutant_id`; every other column
    /// gets a distinct, recognisable value so the re-label's "carry through"
    /// can be checked.
    fn row(pollutant_id: i32, emission_quant: f64) -> PmWorkerRow {
        PmWorkerRow {
            year_id: 2020,
            month_id: 7,
            day_id: 5,
            hour_id: 8,
            state_id: 26,
            county_id: 26161,
            zone_id: 261610,
            link_id: 2616101,
            pollutant_id,
            process_id: 1,
            source_type_id: 21,
            fuel_type_id: 2,
            model_year_id: 2015,
            road_type_id: 4,
            scc: Some("2201001000".to_string()),
            emission_quant,
        }
    }

    #[test]
    fn calculator_metadata() {
        let calc = PmTotalExhaustCalculator::new();
        assert_eq!(calc.name(), "PMTotalExhaustCalculator");
        // Chained calculator: no subscriptions, no captured registrations,
        // no default-DB input tables.
        assert!(calc.subscriptions().is_empty());
        assert!(calc.registrations().is_empty());
        assert!(calc.upstream().is_empty());
        assert!(calc.input_tables().is_empty());
    }

    #[test]
    fn calculator_is_object_safe() {
        // The registry stores calculators as `Box<dyn Calculator>`.
        let calcs: Vec<Box<dyn Calculator>> = vec![Box::new(PmTotalExhaustCalculator::new())];
        assert_eq!(calcs[0].name(), "PMTotalExhaustCalculator");
    }

    #[test]
    fn execute_returns_nonempty_dataframe_for_minimal_inputs() {
        use moves_framework::{DataFrameStore, InMemoryStore};
        use moves_framework::execution::execution_db::{ExecutionLocation, ExecutionTime, IterationPosition};

        // One PM2.5 component row (ec = 112) — relabelled to 110 by PM25Total.
        let input = vec![row(112, 2.0)];
        let mut store = InMemoryStore::new();
        store.insert("MOVESWorkerOutput", PmWorkerRow::into_dataframe(input).unwrap());
        let position = IterationPosition {
            iteration: 0,
            process_id: None,
            location: ExecutionLocation::link(26, 26_161, 261_610, 2_616_101),
            time: ExecutionTime::year(2020),
        };
        let ctx = CalculatorContext::with_position_and_tables(position, store);
        let calc = PmTotalExhaustCalculator::new();
        let out = calc.execute(&ctx).expect("execute ok");
        assert!(out.dataframe().is_some(), "expected non-empty DataFrame");
        assert!(out.dataframe().unwrap().height() > 0, "expected at least one row");
    }

    #[test]
    fn relabel_changes_only_the_pollutant_id() {
        let original = row(102, 1.5);
        let relabelled = original.relabelled(PM10_TOTAL_POLLUTANT_ID);
        assert_eq!(relabelled.pollutant_id, 100);
        // Every other column is carried through verbatim.
        assert_eq!(relabelled.year_id, original.year_id);
        assert_eq!(relabelled.process_id, original.process_id);
        assert_eq!(relabelled.source_type_id, original.source_type_id);
        assert_eq!(relabelled.road_type_id, original.road_type_id);
        assert_eq!(relabelled.scc, original.scc);
        assert_eq!(relabelled.emission_quant, original.emission_quant);
    }

    #[test]
    fn pm10_total_relabels_the_three_pm10_components() {
        let calc = PmTotalExhaustCalculator::new();
        let input = vec![
            row(101, 1.0), // organic carbon PM10
            row(102, 2.0), // elemental carbon PM10
            row(105, 4.0), // sulfate PM10
        ];
        let out = calc.run(
            &input,
            TotalSelection {
                pm10_total: true,
                pm25_total: false,
            },
        );
        assert_eq!(out.len(), 3);
        assert!(out.iter().all(|r| r.pollutant_id == 100));
        // The emission quantities are carried through unchanged — the
        // calculator re-labels, it does not add.
        let quants: Vec<f64> = out.iter().map(|r| r.emission_quant).collect();
        assert_eq!(quants, vec![1.0, 2.0, 4.0]);
    }

    #[test]
    fn pm25_total_relabels_the_three_pm25_components() {
        let calc = PmTotalExhaustCalculator::new();
        let input = vec![
            row(111, 1.0), // organic carbon PM2.5
            row(112, 2.0), // elemental carbon PM2.5
            row(115, 4.0), // sulfate PM2.5
        ];
        let out = calc.run(
            &input,
            TotalSelection {
                pm10_total: false,
                pm25_total: true,
            },
        );
        assert_eq!(out.len(), 3);
        assert!(out.iter().all(|r| r.pollutant_id == 110));
    }

    #[test]
    fn non_component_pollutants_are_ignored() {
        let calc = PmTotalExhaustCalculator::new();
        // Pollutants 1 (THC) and 100/110 (the totals themselves) are not
        // components of either total.
        let input = vec![row(1, 9.0), row(100, 9.0), row(110, 9.0)];
        let out = calc.run(&input, TotalSelection::both());
        assert!(out.is_empty());
    }

    #[test]
    fn both_sections_emit_pm10_before_pm25() {
        let calc = PmTotalExhaustCalculator::new();
        // One PM2.5 component then one PM10 component, in that input order.
        let input = vec![row(112, 2.0), row(102, 1.0)];
        let out = calc.run(&input, TotalSelection::both());
        assert_eq!(out.len(), 2);
        // The PM10Total section runs first, so pollutant 100 leads despite
        // its component appearing second in the input.
        assert_eq!(out[0].pollutant_id, 100);
        assert_eq!(out[1].pollutant_id, 110);
    }

    #[test]
    fn empty_selection_yields_no_rows() {
        let calc = PmTotalExhaustCalculator::new();
        let input = vec![row(102, 1.0), row(112, 2.0)];
        let out = calc.run(&input, TotalSelection::default());
        assert!(out.is_empty());
        assert!(!TotalSelection::default().any());
        assert!(TotalSelection::both().any());
    }

    #[test]
    fn a_component_row_is_relabelled_once_per_enabled_section_it_belongs_to() {
        // Component pollutant ids are disjoint between the two totals, so a
        // row contributes to at most one section regardless of selection.
        let calc = PmTotalExhaustCalculator::new();
        let input = vec![row(102, 1.0)]; // PM10 component only
        let out = calc.run(&input, TotalSelection::both());
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].pollutant_id, 100);
    }
}
