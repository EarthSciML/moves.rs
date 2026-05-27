//! Port of `CO2EqivalentWTPCalculator.java` and
//! `database/CO2EqivalentWTPCalculator.sql` — migration plan Phase 3,
//! Task 69.
//!
//! `CO2EqivalentWTPCalculator` computes **well-to-pump (upstream) CO2
//! equivalent** — the climate-impact-weighted sum of the well-to-pump
//! atmospheric CO2, methane and nitrous oxide, each scaled by its global
//! warming potential. (The class name's "Eqivalent" is a misspelling in the
//! MOVES source; this port keeps it in [`Calculator::name`] so the string
//! matches the chain DAG, but spells the Rust type `Co2EquivalentWtpCalculator`
//! correctly.)
//!
//! # Superseded — empty registrations
//!
//! `CO2EqivalentWTPCalculator` is not wired into the pinned MOVES runtime. The
//! Well-To-Pump process (id 99) has **no `Registration` directive at all** in
//! `CalculatorInfo.txt`, and `characterization/calculator-chains/calculator-dag.json`
//! records `CO2EqivalentWTPCalculator` with `registrations_count: 0`,
//! `subscriptions: []` and `depends_on: []`. The modern base-rate engine
//! (`BaseRateCalculator`, migration-plan Task 45) absorbed the per-pollutant
//! scripted-SQL calculators; the migration plan still lists this class as part
//! of Task 69, so the module ports its algorithm faithfully for reference and
//! cross-validation with [`Calculator::registrations`] returning an empty
//! slice. See [`super::common`] for the cluster's shared infrastructure.
//!
//! # Chained calculator — runs after the other three
//!
//! `CO2EqivalentWTPCalculator` is a *chained* calculator and the cluster's
//! second step: it consumes the well-to-pump rows the other three WTP
//! calculators emit. Its Java `subscribeToMe` chains it onto
//! `CO2AtmosphericWTPCalculator`, so it fires once the well-to-pump
//! atmospheric CO2 (90), methane (5) and nitrous oxide (6) records exist on
//! process 99. The chain DAG records `subscribes_directly: false`; the
//! [`Calculator`] metadata mirrors it — [`subscriptions`](Calculator::subscriptions)
//! is empty, and [`upstream`](Calculator::upstream) is empty too because the
//! unwired process leaves the DAG `depends_on` empty.
//!
//! # What it computes
//!
//! [`Co2EquivalentWtpCalculator::calculate`] ports
//! `CO2EqivalentWTPCalculator.sql`. For every well-to-pump atmospheric CO2
//! (90), methane (5) or nitrous oxide (6) record (`MOVESWorkerOutput`, process
//! 99):
//!
//! ```text
//! co2Equivalent = Σ (emission × globalWarmingPotential)
//! ```
//!
//! summed over the records of each output dimension cell. Each input
//! pollutant's `globalWarmingPotential` is its entry in the `Pollutant` table
//! (the SQL's `CO2EqWTPStep2Pollutant` extract). The output row is stamped
//! with CO2 equivalent (pollutant 98) and keeps the input record's process
//! (Well-To-Pump, 99).
//!
//! The SQL `GROUP BY` includes `mwo.pollutantID`, so each input pollutant
//! contributes its **own** output row — three CO2-equivalent rows per
//! dimension cell, one each from the atmospheric CO2, methane and N2O input,
//! all stamped pollutant 98. (`CO2AERunningStartExtendedIdleCalculator`'s
//! step 2 groups the same way.)
//!
//! The join `MOVESWorkerOutput INNER JOIN CO2EqWTPStep2Pollutant` is an
//! `INNER JOIN`, so an input record whose pollutant has no `Pollutant`-table
//! entry is dropped; the port reproduces that with a map lookup that skips on
//! a miss.
//!
//! # Fidelity — `globalWarmingPotential`
//!
//! `Pollutant.globalWarmingPotential` is a `SMALLINT` — an exact integer, no
//! rounding, matching the `CO2AERunningStartExtendedIdleCalculator` precedent.
//! `MOVESWorkerOutput.emissionQuant` is `FLOAT`, already `f32`-quantised
//! before this port sees it; the port multiplies and sums in `f64`.
//!
//! # Scope of this port
//!
//! [`calculate`](Co2EquivalentWtpCalculator::calculate) is the SQL
//! "Processing" section. Its [`Co2EquivalentWtpInputs`] argument is the set of
//! tables the SQL extracts, as plain row vectors; a future Task 50
//! (`DataFrameStore`) wiring populates it from the per-run filtered execution
//! database — in particular [`Co2EquivalentWtpInputs::worker_output`] carries
//! the process-99 rows the other three WTP calculators write back.
//!
//! The Java `doExecute` gates the whole calculator on the RunSpec actually
//! requesting CO2 Equivalent for Well-To-Pump, and its `##CO2Step2EqprocessIDs##`
//! macro then expands to the single Well-To-Pump process (the alternative
//! `1=2` branch is unreachable, since `doExecute` returns no SQL when the
//! pollutant is not requested). That is execution-gating, reproduced by
//! `calculate` filtering input records to the well-to-pump process and
//! returning no rows on empty input, matching the `SO2Calculator` precedent.
//! `MOVESRunID` and `SCC` are pass-through columns left to the Task 50 wiring.
//!
//! # Data plane (Task 50)
//!
//! [`Calculator::execute`] receives a [`CalculatorContext`] whose execution
//! tables and scratch namespace are Phase 2 placeholders until the
//! `DataFrameStore` lands (migration-plan Task 50), so `execute` cannot yet
//! read `MOVESWorkerOutput` nor write the CO2-equivalent rows back. The
//! numeric algorithm is fully ported and unit-tested on
//! [`calculate`](Co2EquivalentWtpCalculator::calculate); `execute` is a
//! documented shell returning an empty [`CalculatorOutput`].

use std::collections::{BTreeMap, HashMap};

use moves_data::PollutantProcessAssociation;
use moves_framework::{
    Calculator, CalculatorContext, CalculatorOutput, CalculatorSubscription, DataFrameStoreTyped,
    Error, TableRow,
};
use polars::prelude::{DataFrame, DataType, NamedFrom, PolarsResult, Schema, Series};

use super::common::WorkerOutputRow;

fn row_err(table: &'static str, row: usize, column: &'static str, msg: String) -> Error {
    Error::RowExtraction {
        table: table.into(),
        row,
        column: column.into(),
        message: msg,
    }
}

/// Stable module name — matches the Java class (misspelling and all) and the
/// `CO2EqivalentWTPCalculator` entry in the chain DAG (`calculator-dag.json`).
const CALCULATOR_NAME: &str = "CO2EqivalentWTPCalculator";

/// CO2 Equivalent — `Pollutant` id 98. The pollutant this calculator produces.
const EQUIVALENT_CO2_POLLUTANT_ID: i32 = 98;

/// Well-To-Pump — `EmissionProcess` id 99. The process the input records carry
/// and the output rows keep.
const WELL_TO_PUMP_PROCESS_ID: i32 = 99;

/// The input pollutants the CO2-equivalent sum draws on — atmospheric CO2 (90),
/// methane (5) and nitrous oxide (6); the SQL's `##CO2Step2EqpollutantIDs##`.
const EQUIVALENT_CO2_INPUT_POLLUTANT_IDS: [i32; 3] = [5, 6, 90];

/// One `Pollutant`-table row — the global warming potential of a pollutant.
///
/// Models the SQL's `CO2EqWTPStep2Pollutant` extract (a copy of `Pollutant`).
/// Only the `pollutantID` and `globalWarmingPotential` columns feed the
/// algorithm. MOVES extracts the full table; a pollutant absent from this
/// input is dropped by the inner join, and the three input pollutants (90, 5,
/// 6) always carry a populated `SMALLINT` potential in real data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PollutantGwpRow {
    /// `pollutantID`.
    pub pollutant_id: i32,
    /// `globalWarmingPotential` — the `SMALLINT` climate-impact multiplier.
    pub global_warming_potential: i32,
}

impl TableRow for PollutantGwpRow {
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
        let po = get_i32("pollutantID")?;
        let gw = get_i32("globalWarmingPotential")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(PollutantGwpRow {
                    pollutant_id: po.get(i).ok_or_else(|| null("pollutantID"))?,
                    global_warming_potential: gw
                        .get(i)
                        .ok_or_else(|| null("globalWarmingPotential"))?,
                })
            })
            .collect()
    }
}

/// Inputs to [`Co2EquivalentWtpCalculator::calculate`] — the tables the SQL's
/// "Extract Data" section produces, as plain row vectors.
///
/// A future Task 50 (`DataFrameStore`) wiring populates this from the per-run
/// filtered execution database; until then it is the explicit data-plane
/// contract the unit tests build directly.
#[derive(Debug, Clone, Default)]
pub struct Co2EquivalentWtpInputs {
    /// `MOVESWorkerOutput` rows. The calculation reads the well-to-pump
    /// atmospheric CO2, methane and nitrous oxide records (the process-99
    /// output of the other three WTP calculators); any other row is ignored.
    pub worker_output: Vec<WorkerOutputRow>,
    /// `Pollutant` rows — the `pollutantID → globalWarmingPotential` mapping.
    pub pollutant_gwp: Vec<PollutantGwpRow>,
}

/// The `GROUP BY` cell of `CO2EqivalentWTPCalculator.sql`'s
/// `MOVESOutputCO2Temp2eq`.
///
/// The SQL groups by the full output dimension **including** `mwo.pollutantID`
/// and `mwo.processID`, so the source pollutant is part of the key even though
/// every output row is stamped with CO2 equivalent. Field order is the
/// deterministic output sort order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct GroupKey {
    year_id: i32,
    month_id: i32,
    day_id: i32,
    hour_id: i32,
    state_id: i32,
    county_id: i32,
    zone_id: i32,
    link_id: i32,
    /// `mwo.processID` — a `GROUP BY` axis, carried through to the output row.
    process_id: i32,
    /// The source pollutant — a `GROUP BY` axis; the output row is stamped
    /// with CO2 equivalent regardless.
    source_pollutant_id: i32,
    source_type_id: i32,
    fuel_type_id: i32,
    model_year_id: i32,
    road_type_id: i32,
}

/// The MOVES well-to-pump CO2-equivalent calculator.
///
/// A zero-sized value type owning no per-run state, as the [`Calculator`]
/// trait contract requires; all run-varying input flows through the
/// [`Co2EquivalentWtpInputs`] argument to [`calculate`](Self::calculate).
#[derive(Debug, Clone, Copy, Default)]
pub struct Co2EquivalentWtpCalculator;

impl Co2EquivalentWtpCalculator {
    /// Stable module name — matches the Java class and the chain-DAG entry.
    pub const NAME: &'static str = CALCULATOR_NAME;

    /// Compute the well-to-pump CO2-equivalent rows — the port of
    /// `CO2EqivalentWTPCalculator.sql`.
    ///
    /// Returns no rows when the inputs carry no usable records: a record
    /// contributes only if it is a well-to-pump (process 99) atmospheric CO2,
    /// methane or nitrous oxide row and its pollutant resolves a global
    /// warming potential — the SQL `INNER JOIN`. The result is ordered by its
    /// `GROUP BY` cell for deterministic output; MOVES leaves
    /// `MOVESWorkerOutput` physically unordered.
    #[must_use]
    pub fn calculate(&self, inputs: &Co2EquivalentWtpInputs) -> Vec<WorkerOutputRow> {
        let gwp: HashMap<i32, i32> = inputs
            .pollutant_gwp
            .iter()
            .map(|p| (p.pollutant_id, p.global_warming_potential))
            .collect();

        // emissionQuant = Σ (emission × globalWarmingPotential), grouped by
        // the output dimension including the source pollutant and process.
        let mut groups: BTreeMap<GroupKey, f64> = BTreeMap::new();
        for record in &inputs.worker_output {
            // mwo.pollutantID IN (90, 5, 6).
            if !EQUIVALENT_CO2_INPUT_POLLUTANT_IDS.contains(&record.pollutant_id) {
                continue;
            }
            // ##CO2Step2EqprocessIDs## — mwo.processID IN (99).
            if record.process_id != WELL_TO_PUMP_PROCESS_ID {
                continue;
            }
            // INNER JOIN CO2EqWTPStep2Pollutant ON mwo.pollutantID.
            let Some(&potential) = gwp.get(&record.pollutant_id) else {
                continue;
            };
            let key = GroupKey {
                year_id: record.year_id,
                month_id: record.month_id,
                day_id: record.day_id,
                hour_id: record.hour_id,
                state_id: record.state_id,
                county_id: record.county_id,
                zone_id: record.zone_id,
                link_id: record.link_id,
                process_id: record.process_id,
                source_pollutant_id: record.pollutant_id,
                source_type_id: record.source_type_id,
                fuel_type_id: record.fuel_type_id,
                model_year_id: record.model_year_id,
                road_type_id: record.road_type_id,
            };
            *groups.entry(key).or_insert(0.0) += record.emission_quant * f64::from(potential);
        }

        groups
            .into_iter()
            .map(|(key, emission_quant)| WorkerOutputRow {
                year_id: key.year_id,
                month_id: key.month_id,
                day_id: key.day_id,
                hour_id: key.hour_id,
                state_id: key.state_id,
                county_id: key.county_id,
                zone_id: key.zone_id,
                link_id: key.link_id,
                pollutant_id: EQUIVALENT_CO2_POLLUTANT_ID,
                process_id: key.process_id,
                source_type_id: key.source_type_id,
                fuel_type_id: key.fuel_type_id,
                model_year_id: key.model_year_id,
                road_type_id: key.road_type_id,
                emission_quant,
            })
            .collect()
    }
}

/// `CO2EqivalentWTPCalculator` is a chained calculator —
/// `subscribes_directly: false` in `calculator-dag.json` — so it declares no
/// MasterLoop subscription.
static NO_SUBSCRIPTIONS: &[CalculatorSubscription] = &[];

/// `CO2EqivalentWTPCalculator` registers nothing: the Well-To-Pump process has
/// no `Registration` directive in `CalculatorInfo.txt` and the chain DAG
/// records `registrations_count: 0`. See the module-level supersession note.
static NO_REGISTRATIONS: &[PollutantProcessAssociation] = &[];

/// Default-DB tables the well-to-pump CO2-equivalent computation consumes.
static INPUT_TABLES: &[&str] = &["MOVESWorkerOutput", "Pollutant"];

impl Calculator for Co2EquivalentWtpCalculator {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    /// `CO2EqivalentWTPCalculator` is a chained calculator: it does not
    /// subscribe to the MasterLoop directly. `calculator-dag.json` records
    /// `subscribes_directly: false` and an empty `subscriptions` list.
    fn subscriptions(&self) -> &[CalculatorSubscription] {
        NO_SUBSCRIPTIONS
    }

    /// Empty — `CO2EqivalentWTPCalculator` is superseded by
    /// `BaseRateCalculator` and registers no `(pollutant, process)` pairs; see
    /// the module-level note.
    fn registrations(&self) -> &[PollutantProcessAssociation] {
        NO_REGISTRATIONS
    }

    fn input_tables(&self) -> &[&'static str] {
        INPUT_TABLES
    }

    /// Phase 2 skeleton — returns an empty [`CalculatorOutput`].
    ///
    /// [`CalculatorContext`] cannot yet surface the input tables or accept the
    /// `MOVESWorkerOutput` rows — that lands with the Task 50 `DataFrameStore`.
    /// The computation is ported and tested in
    /// [`Co2EquivalentWtpCalculator::calculate`].
    fn execute(&self, ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
        let tables = ctx.tables();
        let inputs = Co2EquivalentWtpInputs {
            worker_output: tables.iter_typed::<WorkerOutputRow>("MOVESWorkerOutput")?,
            pollutant_gwp: tables.iter_typed::<PollutantGwpRow>("Pollutant")?,
        };
        let rows = self.calculate(&inputs);
        crate::wiring::emit_rows(rows)
    }
}

/// Construct the calculator as a boxed trait object — matches the engine's
/// calculator-factory signature so the registry can register it.
#[must_use]
pub fn factory() -> Box<dyn Calculator> {
    Box::new(Co2EquivalentWtpCalculator)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One well-to-pump methane record and the global warming potentials of
    /// the three input pollutants. The single record is `200.0` of methane
    /// (GWP 25), so the one output row is `200.0 × 25 = 5000.0`.
    fn minimal_inputs() -> Co2EquivalentWtpInputs {
        Co2EquivalentWtpInputs {
            worker_output: vec![WorkerOutputRow {
                year_id: 2020,
                month_id: 1,
                day_id: 5,
                hour_id: 8,
                state_id: 26,
                county_id: 26_161,
                zone_id: 261_610,
                link_id: 5001,
                pollutant_id: 5,
                process_id: 99,
                source_type_id: 21,
                fuel_type_id: 2,
                model_year_id: 2018,
                road_type_id: 4,
                emission_quant: 200.0,
            }],
            pollutant_gwp: vec![
                PollutantGwpRow {
                    pollutant_id: 90,
                    global_warming_potential: 1,
                },
                PollutantGwpRow {
                    pollutant_id: 5,
                    global_warming_potential: 25,
                },
                PollutantGwpRow {
                    pollutant_id: 6,
                    global_warming_potential: 298,
                },
            ],
        }
    }

    fn assert_close(actual: f64, expected: f64) {
        assert!(
            (actual - expected).abs() < 1e-6,
            "{actual} != expected {expected}",
        );
    }

    #[test]
    fn calculate_minimal_input_yields_one_row() {
        let rows = Co2EquivalentWtpCalculator.calculate(&minimal_inputs());
        assert_eq!(rows.len(), 1);
        let r = rows[0];
        assert_eq!(r.county_id, 26_161);
        assert_eq!(r.fuel_type_id, 2);
        // Pollutant relabelled to CO2 equivalent; process kept at 99.
        assert_eq!(r.pollutant_id, 98);
        assert_eq!(r.process_id, 99);
        // 200.0 × 25.
        assert_close(r.emission_quant, 5_000.0);
    }

    #[test]
    fn calculate_keeps_source_pollutants_separate() {
        // One record per input pollutant in the same dimension cell: the
        // GROUP BY includes mwo.pollutantID, so each yields its own
        // CO2-equivalent output row.
        let mut inputs = minimal_inputs();
        inputs.worker_output = vec![
            WorkerOutputRow {
                pollutant_id: 90,
                emission_quant: 1_000.0,
                ..inputs.worker_output[0]
            },
            WorkerOutputRow {
                pollutant_id: 5,
                emission_quant: 200.0,
                ..inputs.worker_output[0]
            },
            WorkerOutputRow {
                pollutant_id: 6,
                emission_quant: 10.0,
                ..inputs.worker_output[0]
            },
        ];
        let rows = Co2EquivalentWtpCalculator.calculate(&inputs);
        assert_eq!(rows.len(), 3);
        assert!(rows.iter().all(|r| r.pollutant_id == 98));
        // 1000×1, 200×25, 10×298.
        let mut quants: Vec<f64> = rows.iter().map(|r| r.emission_quant).collect();
        quants.sort_by(f64::total_cmp);
        assert_close(quants[0], 1_000.0);
        assert_close(quants[1], 2_980.0);
        assert_close(quants[2], 5_000.0);
    }

    #[test]
    fn calculate_sums_records_of_one_source_pollutant() {
        // Two methane records in the same cell collapse into one output row.
        let mut inputs = minimal_inputs();
        inputs.worker_output.push(WorkerOutputRow {
            emission_quant: 40.0,
            ..inputs.worker_output[0]
        });
        let rows = Co2EquivalentWtpCalculator.calculate(&inputs);
        assert_eq!(rows.len(), 1);
        // (200.0 + 40.0) × 25.
        assert_close(rows[0].emission_quant, 6_000.0);
    }

    #[test]
    fn calculate_ignores_pollutants_outside_the_input_set() {
        // A well-to-pump Total Energy (91) record is not a CO2-equivalent
        // input — only 90, 5 and 6 are summed.
        let mut inputs = minimal_inputs();
        inputs.worker_output[0].pollutant_id = 91;
        assert!(Co2EquivalentWtpCalculator.calculate(&inputs).is_empty());
    }

    #[test]
    fn calculate_ignores_non_well_to_pump_records() {
        // A methane record on a tailpipe process (running exhaust, 1) is not
        // a well-to-pump input — the SQL filters mwo.processID IN (99).
        let mut inputs = minimal_inputs();
        inputs.worker_output[0].process_id = 1;
        assert!(Co2EquivalentWtpCalculator.calculate(&inputs).is_empty());
    }

    #[test]
    fn calculate_drops_records_without_a_global_warming_potential() {
        // No Pollutant-table entry for methane → the inner join drops it.
        let mut inputs = minimal_inputs();
        inputs.pollutant_gwp.retain(|p| p.pollutant_id != 5);
        assert!(Co2EquivalentWtpCalculator.calculate(&inputs).is_empty());
    }

    #[test]
    fn calculate_separates_distinct_dimension_cells() {
        let mut inputs = minimal_inputs();
        inputs.worker_output.push(WorkerOutputRow {
            link_id: 9999,
            ..inputs.worker_output[0]
        });
        let rows = Co2EquivalentWtpCalculator.calculate(&inputs);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].link_id, 5001);
        assert_eq!(rows[1].link_id, 9999);
    }

    #[test]
    fn calculate_empty_input_yields_no_rows() {
        assert!(Co2EquivalentWtpCalculator
            .calculate(&Co2EquivalentWtpInputs::default())
            .is_empty());
    }

    #[test]
    fn calculator_name_matches_dag_module() {
        // The dag preserves the MOVES source's "Eqivalent" misspelling.
        assert_eq!(
            Co2EquivalentWtpCalculator.name(),
            "CO2EqivalentWTPCalculator"
        );
        assert_eq!(
            Co2EquivalentWtpCalculator::NAME,
            "CO2EqivalentWTPCalculator"
        );
    }

    #[test]
    fn calculator_is_chained_with_no_subscriptions() {
        assert!(Co2EquivalentWtpCalculator.subscriptions().is_empty());
    }

    #[test]
    fn registrations_are_empty_because_the_process_is_unwired() {
        assert!(Co2EquivalentWtpCalculator.registrations().is_empty());
    }

    #[test]
    fn upstream_is_empty() {
        assert!(Co2EquivalentWtpCalculator.upstream().is_empty());
    }

    #[test]
    fn execute_returns_nonempty_dataframe_for_minimal_inputs() {
        use moves_framework::{DataFrameStore, InMemoryStore, TableRow};
        let inputs = minimal_inputs();
        let mut store = InMemoryStore::new();
        store.insert(
            "MOVESWorkerOutput",
            WorkerOutputRow::into_dataframe(inputs.worker_output).unwrap(),
        );
        store.insert(
            "Pollutant",
            PollutantGwpRow::into_dataframe(inputs.pollutant_gwp).unwrap(),
        );
        let ctx = CalculatorContext::with_tables(store);
        let out = Co2EquivalentWtpCalculator
            .execute(&ctx)
            .expect("execute ok");
        let df = out.dataframe().expect("output should contain a DataFrame");
        assert!(df.height() > 0, "minimal inputs produce at least one row");
    }

    #[test]
    fn factory_builds_a_named_calculator() {
        assert_eq!(factory().name(), "CO2EqivalentWTPCalculator");
    }

    #[test]
    fn calculator_is_object_safe() {
        let calc: Box<dyn Calculator> = Box::new(Co2EquivalentWtpCalculator);
        assert_eq!(calc.name(), "CO2EqivalentWTPCalculator");
    }
}
