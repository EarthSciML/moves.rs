//! Port of `WellToPumpProcessor.java` and `database/WellToPumpCalculator.sql`
//! .
//!
//! `WellToPumpProcessor` computes **well-to-pump (upstream) Total Energy
//! Consumption** — the energy spent extracting, refining and distributing the
//! fuel a vehicle later burns, as opposed to the pump-to-wheel energy the
//! vehicle itself consumes. It scales each pump-to-wheel Total Energy
//! Consumption record by a fuel-specific well-to-pump factor.
//!
//! # Superseded — empty registrations
//!
//! `WellToPumpProcessor` is not wired into the pinned MOVES runtime. The
//! Well-To-Pump process (id 99) has **no `Registration` directive at all** in
//! `CalculatorInfo.txt`, and `characterization/calculator-chains/calculator-dag.json`
//! records `WellToPumpProcessor` with `registrations_count: 0`,
//! `subscriptions: []` and `depends_on: []`. The modern base-rate engine
//! (`BaseRateCalculator`, ) absorbed the per-pollutant
//! scripted-SQL calculators; the still lists this class as part
//! of, so the module ports its algorithm faithfully for reference and
//! cross-validation with [`Calculator::registrations`] returning an empty
//! slice. See [`super::common`] for the cluster's shared infrastructure.
//!
//! # Chained calculator
//!
//! `WellToPumpProcessor` is a *chained* calculator: its Java `subscribeToMe`
//! does not subscribe to the MasterLoop but chains the calculator onto every
//! calculator that produces Total Energy Consumption (pollutant 91), so the
//! well-to-pump energy is added wherever pump-to-wheel energy is produced. The
//! chain DAG records `subscribes_directly: false`; the [`Calculator`] metadata
//! mirrors it — [`subscriptions`](Calculator::subscriptions) is empty, and
//! [`upstream`](Calculator::upstream) is empty too because the unwired process
//! leaves the DAG `depends_on` empty.
//!
//! # What it computes
//!
//! [`WellToPumpProcessor::calculate`] ports `WellToPumpCalculator.sql`. For
//! every pump-to-wheel Total Energy Consumption record (`MOVESWorkerOutput`,
//! pollutant 91, any process but Well-To-Pump itself):
//!
//! ```text
//! wellToPumpEnergy = Σ pumpToWheelEnergy × WTPFactor
//! ```
//!
//! summed over the records of each output dimension cell. `WTPFactor` is the
//! market-share-weighted, year-interpolated GREET well-to-pump factor of the
//! cell's `(year, monthGroup, fuelType)` — the
//! [`build_wtp_factor_by_fuel_type`] table.
//!
//! The SQL's `MOVESOutputTemp` aggregate is `SUM(mwo.emissionQuant) *
//! wfft.WTPFactor` — the factor multiplies the *summed* energy, since
//! `WTPFactor` is constant within a `GROUP BY` group (the group fixes the
//! county, year, month, fuel type and pollutant that key the factor table).
//! This port reproduces that grouping: it accumulates `Σ emissionQuant` per
//! group and multiplies by the group's factor once, **not** per row — the
//! distinction is observable in `f64` rounding and matters for
//! validation.
//!
//! The output row is stamped with the well-to-pump process (99) and keeps the
//! factor table's pollutant (91, Total Energy Consumption). Every contributing
//! source process collapses into one output row: the SQL `GROUP BY` does
//! **not** include `mwo.processID`.
//!
//! Every SQL join is an `INNER JOIN`, so a record that fails to resolve its
//! month group or well-to-pump factor is dropped; the port reproduces that
//! with map lookups that skip on a miss.
//!
//! # Scope of this port
//!
//! [`calculate`](WellToPumpProcessor::calculate) is the SQL "Processing"
//! section plus the GREET interpolation and market-share weighting of the
//! "Extract Data" section (factored into [`super::common`]). Its [`WtpInputs`]
//! argument is the set of tables the SQL extracts, as plain row vectors; a
//! future (`DataFrameStore`) wiring populates it from the per-run
//! filtered execution database.
//!
//! The Java `doExecute` gates the whole calculator on the RunSpec actually
//! requesting Total Energy Consumption for Well-To-Pump; that is
//! execution-gating, reproduced by `calculate` returning no rows on empty
//! input, matching the `SO2Calculator` precedent. `MOVESRunID` and `SCC` are
//! pass-through columns left to the wiring.
//!
//! The SQL keys `WTPFactorByFuelType` by the literal context `countyID` and
//! joins it `wfft.countyID = mwo.countyID`; a master-loop invocation is
//! single-county, so the join is trivially satisfied and the port carries
//! `countyID` straight from the energy row, matching `SO2Calculator`.
//!
//! # Data plane
//!
//! [`Calculator::execute`] receives a [`CalculatorContext`] whose execution
//! tables and scratch namespace are placeholders until the
//! `DataFrameStore` lands (), so `execute` cannot yet
//! read `MOVESWorkerOutput` nor write the well-to-pump rows back. The numeric
//! algorithm is fully ported and unit-tested on
//! [`calculate`](WellToPumpProcessor::calculate); `execute` is a documented
//! shell returning an empty [`CalculatorOutput`].

use std::collections::BTreeMap;

use moves_data::PollutantProcessAssociation;
use moves_framework::{
    Calculator, CalculatorContext, CalculatorOutput, CalculatorSubscription, DataFrameStoreTyped,
    Error,
};

use super::common::{
    build_wtp_factor_by_fuel_type, month_group_index, FuelFormulationRow, FuelSubTypeRow,
    FuelSupplyRow, GreetWellToPumpRow, MonthGroupRow, WorkerOutputRow, WtpInputs, YearRow,
};

/// Stable module name — matches the Java class and the `WellToPumpProcessor`
/// entry in the calculator-chain DAG (`calculator-dag.json`).
const CALCULATOR_NAME: &str = "WellToPumpProcessor";

/// Total Energy Consumption — `Pollutant` id 91. `WellToPumpProcessor` reads
/// the `MOVESWorkerOutput` records for this pollutant and re-emits them, still
/// pollutant 91, for the well-to-pump process.
const TOTAL_ENERGY_POLLUTANT_ID: i32 = 91;

/// Well-To-Pump — `EmissionProcess` id 99. The process the output rows carry,
/// and the one the input filter excludes so well-to-pump energy is not fed
/// back into itself.
const WELL_TO_PUMP_PROCESS_ID: i32 = 99;

/// The `GROUP BY` cell of `WellToPumpCalculator.sql`'s `MOVESOutputTemp` — the
/// output dimension minus `processID` (every source process collapses into one
/// well-to-pump row). Field order is the deterministic output sort order.
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
    /// The well-to-pump factor table's pollutant — 91, Total Energy
    /// Consumption.
    pollutant_id: i32,
    source_type_id: i32,
    fuel_type_id: i32,
    model_year_id: i32,
    road_type_id: i32,
}

/// One `MOVESOutputTemp` group's running total — `Σ emissionQuant` and the
/// group's well-to-pump factor (constant within the group).
#[derive(Debug, Clone, Copy)]
struct GroupAccumulator {
    summed_energy: f64,
    factor: f64,
}

/// The MOVES well-to-pump Total Energy Consumption processor.
///
/// A zero-sized value type owning no per-run state, as the [`Calculator`]
/// trait contract requires; all run-varying input flows through the
/// [`WtpInputs`] argument to [`calculate`](Self::calculate).
#[derive(Debug, Clone, Copy, Default)]
pub struct WellToPumpProcessor;

impl WellToPumpProcessor {
    /// Stable module name — matches the Java class and the chain-DAG entry.
    pub const NAME: &'static str = CALCULATOR_NAME;

    /// Compute the well-to-pump Total Energy Consumption rows — the port of
    /// `WellToPumpCalculator.sql`.
    ///
    /// Returns no rows when the inputs carry no usable energy: an energy
    /// record contributes only if it is pollutant 91 for a process other than
    /// Well-To-Pump, its month resolves a month group, and its
    /// `(year, monthGroup, fuelType)` resolves a well-to-pump factor — every
    /// SQL join is an `INNER JOIN`. The result is ordered by its `GROUP BY`
    /// cell for deterministic output; MOVES leaves `MOVESWorkerOutput`
    /// physically unordered.
    #[must_use]
    pub fn calculate(&self, inputs: &WtpInputs) -> Vec<WorkerOutputRow> {
        let factor_table = build_wtp_factor_by_fuel_type(inputs);
        let month_group = month_group_index(&inputs.month_of_any_year);

        let mut groups: BTreeMap<GroupKey, GroupAccumulator> = BTreeMap::new();
        for energy in &inputs.worker_output {
            // mwo.pollutantID = 91.
            if energy.pollutant_id != TOTAL_ENERGY_POLLUTANT_ID {
                continue;
            }
            // mwo.processID <> 99 — do not re-process well-to-pump energy.
            if energy.process_id == WELL_TO_PUMP_PROCESS_ID {
                continue;
            }
            // INNER JOIN may ON may.monthID = mwo.monthID.
            let Some(&month_group_id) = month_group.get(&energy.month_id) else {
                continue;
            };
            // INNER JOIN wfft ON yearID, monthGroupID, fuelTypeID (countyID is
            // the trivially-satisfied single-county join).
            let Some(cells) =
                factor_table.get(&(energy.year_id, month_group_id, energy.fuel_type_id))
            else {
                continue;
            };
            for cell in cells {
                let key = GroupKey {
                    year_id: energy.year_id,
                    month_id: energy.month_id,
                    day_id: energy.day_id,
                    hour_id: energy.hour_id,
                    state_id: energy.state_id,
                    county_id: energy.county_id,
                    zone_id: energy.zone_id,
                    link_id: energy.link_id,
                    pollutant_id: cell.pollutant_id,
                    source_type_id: energy.source_type_id,
                    fuel_type_id: energy.fuel_type_id,
                    model_year_id: energy.model_year_id,
                    road_type_id: energy.road_type_id,
                };
                let accumulator = groups.entry(key).or_insert(GroupAccumulator {
                    summed_energy: 0.0,
                    factor: cell.factor,
                });
                accumulator.summed_energy += energy.emission_quant;
            }
        }

        groups
            .into_iter()
            .map(|(key, accumulator)| WorkerOutputRow {
                year_id: key.year_id,
                month_id: key.month_id,
                day_id: key.day_id,
                hour_id: key.hour_id,
                state_id: key.state_id,
                county_id: key.county_id,
                zone_id: key.zone_id,
                link_id: key.link_id,
                pollutant_id: key.pollutant_id,
                process_id: WELL_TO_PUMP_PROCESS_ID,
                source_type_id: key.source_type_id,
                fuel_type_id: key.fuel_type_id,
                model_year_id: key.model_year_id,
                road_type_id: key.road_type_id,
                // SUM(mwo.emissionQuant) * wfft.WTPFactor — the factor
                // multiplies the summed energy, not each row.
                emission_quant: accumulator.summed_energy * accumulator.factor,
            })
            .collect()
    }
}

/// `WellToPumpProcessor` is a chained calculator — `subscribes_directly: false`
/// in `calculator-dag.json` — so it declares no MasterLoop subscription.
static NO_SUBSCRIPTIONS: &[CalculatorSubscription] = &[];

/// `WellToPumpProcessor` registers nothing: the Well-To-Pump process has no
/// `Registration` directive in `CalculatorInfo.txt` and the chain DAG records
/// `registrations_count: 0`. See the module-level supersession note.
static NO_REGISTRATIONS: &[PollutantProcessAssociation] = &[];

/// Default-DB tables the well-to-pump computation consumes.
static INPUT_TABLES: &[&str] = &[
    "FuelFormulation",
    "FuelSubtype",
    "FuelSupply",
    "GREETWellToPump",
    "MOVESWorkerOutput",
    "MonthOfAnyYear",
    "Year",
];

impl Calculator for WellToPumpProcessor {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    /// `WellToPumpProcessor` is a chained calculator: it does not subscribe to
    /// the MasterLoop directly. `calculator-dag.json` records
    /// `subscribes_directly: false` and an empty `subscriptions` list.
    fn subscriptions(&self) -> &[CalculatorSubscription] {
        NO_SUBSCRIPTIONS
    }

    /// Empty — `WellToPumpProcessor` is superseded by `BaseRateCalculator` and
    /// registers no `(pollutant, process)` pairs; see the module-level note.
    fn registrations(&self) -> &[PollutantProcessAssociation] {
        NO_REGISTRATIONS
    }

    fn input_tables(&self) -> &[&'static str] {
        INPUT_TABLES
    }

    fn execute(&self, ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
        let tables = ctx.tables();
        let year: Vec<YearRow> = tables.iter_typed("Year")?;
        // The canonical `WellToPumpCalculator.sql` substitutes every
        // `##context.year##` (the GREET interpolation bracket and the
        // `WTPFactorByFuelType` join, lines 93–145) before the script runs;
        // an unresolved one is a hard preprocessor failure, never a silent
        // default. A `None` context year here means the calculator was
        // dispatched at a granularity coarser than YEAR. Fabricating
        // `year.first()` (an arbitrary Year row) or `0` would silently shift
        // the GREET interpolation bracket and the surviving fuel supply,
        // scaling Total Energy Consumption by a wrong WTP factor — surface it
        // rather than fabricate. `IterationPosition` has no dedicated error
        // variant, so reuse `RowExtraction` (its documented "value was null
        // where a non-null value is required" case), as
        // `CriteriaStartCalculator::execute` does.
        let target_year =
            ctx.position()
                .time
                .year
                .map(i32::from)
                .ok_or_else(|| Error::RowExtraction {
                    table: "IterationPosition".into(),
                    row: ctx.position().iteration as usize,
                    column: "year".into(),
                    message: "required run-context scalar is unresolved (None)".into(),
                })?;
        let inputs = WtpInputs {
            greet: tables.iter_typed::<GreetWellToPumpRow>("GREETWellToPump")?,
            fuel_supply: tables.iter_typed::<FuelSupplyRow>("FuelSupply")?,
            fuel_formulation: tables.iter_typed::<FuelFormulationRow>("FuelFormulation")?,
            fuel_sub_type: tables.iter_typed::<FuelSubTypeRow>("FuelSubtype")?,
            year,
            month_of_any_year: tables.iter_typed::<MonthGroupRow>("MonthOfAnyYear")?,
            worker_output: tables.iter_typed::<WorkerOutputRow>("MOVESWorkerOutput")?,
            target_year,
        };
        let rows = self.calculate(&inputs);
        crate::wiring::emit_rows(rows)
    }
}

/// Construct the calculator as a boxed trait object — matches the engine's
/// calculator-factory signature so the registry can register it.
#[must_use]
pub fn factory() -> Box<dyn Calculator> {
    Box::new(WellToPumpProcessor)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::calculators::welltopump::common::{
        FuelFormulationRow, FuelSubTypeRow, FuelSupplyRow, GreetWellToPumpRow, MonthGroupRow,
        YearRow,
    };

    /// Build a one-formulation / one-energy-row input. The well-to-pump factor
    /// is `100.0 × 1.0 = 100.0` (rate × market share) and the single energy
    /// record is `200.0`, so the one output row is `200.0 × 100.0 = 20000.0`.
    fn minimal_inputs() -> WtpInputs {
        WtpInputs {
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
            month_of_any_year: vec![MonthGroupRow {
                month_id: 1,
                month_group_id: 1,
            }],
            worker_output: vec![WorkerOutputRow {
                year_id: 2020,
                month_id: 1,
                day_id: 5,
                hour_id: 8,
                state_id: 26,
                county_id: 26_161,
                zone_id: 261_610,
                link_id: 5001,
                pollutant_id: 91,
                process_id: 1,
                source_type_id: 21,
                fuel_type_id: 2,
                model_year_id: 2018,
                road_type_id: 4,
                emission_quant: 200.0,
            }],
            target_year: 2020,
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
        let rows = WellToPumpProcessor.calculate(&minimal_inputs());
        assert_eq!(rows.len(), 1);
        let r = rows[0];
        // Dimension cell carried straight from the energy row.
        assert_eq!(r.year_id, 2020);
        assert_eq!(r.month_id, 1);
        assert_eq!(r.county_id, 26_161);
        assert_eq!(r.source_type_id, 21);
        assert_eq!(r.fuel_type_id, 2);
        assert_eq!(r.model_year_id, 2018);
        assert_eq!(r.road_type_id, 4);
        // Pollutant stays 91 (Total Energy); process is stamped 99.
        assert_eq!(r.pollutant_id, 91);
        assert_eq!(r.process_id, 99);
        // 200.0 × (100.0 × 1.0).
        assert_close(r.emission_quant, 20_000.0);
    }

    #[test]
    fn calculate_sums_energy_across_source_processes() {
        // Two energy records, processes 1 and 2, same dimension cell — the
        // GROUP BY omits processID, so they collapse into one output row and
        // the factor multiplies the *sum*.
        let mut inputs = minimal_inputs();
        inputs.worker_output.push(WorkerOutputRow {
            process_id: 2,
            emission_quant: 50.0,
            ..inputs.worker_output[0]
        });
        let rows = WellToPumpProcessor.calculate(&inputs);
        assert_eq!(rows.len(), 1);
        // (200.0 + 50.0) × 100.0.
        assert_close(rows[0].emission_quant, 25_000.0);
    }

    #[test]
    fn calculate_excludes_well_to_pump_process_input() {
        // An energy record already on process 99 must not be re-processed.
        let mut inputs = minimal_inputs();
        inputs.worker_output[0].process_id = 99;
        assert!(WellToPumpProcessor.calculate(&inputs).is_empty());
    }

    #[test]
    fn calculate_ignores_non_energy_rows() {
        // A record whose pollutant is not Total Energy Consumption (91) is not
        // a well-to-pump input.
        let mut inputs = minimal_inputs();
        inputs.worker_output[0].pollutant_id = 2;
        assert!(WellToPumpProcessor.calculate(&inputs).is_empty());
    }

    #[test]
    fn calculate_drops_energy_without_a_month_group() {
        // The energy record's month is absent from MonthOfAnyYear — the inner
        // join drops it.
        let mut inputs = minimal_inputs();
        inputs.month_of_any_year.clear();
        assert!(WellToPumpProcessor.calculate(&inputs).is_empty());
    }

    #[test]
    fn calculate_drops_energy_without_a_factor() {
        // No fuel supply → no well-to-pump factor for the energy cell.
        let mut inputs = minimal_inputs();
        inputs.fuel_supply.clear();
        assert!(WellToPumpProcessor.calculate(&inputs).is_empty());
    }

    #[test]
    fn calculate_separates_distinct_dimension_cells() {
        // Two energy records on distinct links yield two output rows.
        let mut inputs = minimal_inputs();
        inputs.worker_output.push(WorkerOutputRow {
            link_id: 9999,
            emission_quant: 10.0,
            ..inputs.worker_output[0]
        });
        let rows = WellToPumpProcessor.calculate(&inputs);
        assert_eq!(rows.len(), 2);
        // BTreeMap ordering: link 5001 before 9999.
        assert_eq!(rows[0].link_id, 5001);
        assert_eq!(rows[1].link_id, 9999);
        assert_close(rows[0].emission_quant, 20_000.0);
        assert_close(rows[1].emission_quant, 1_000.0);
    }

    #[test]
    fn calculate_empty_input_yields_no_rows() {
        assert!(WellToPumpProcessor
            .calculate(&WtpInputs::default())
            .is_empty());
    }

    #[test]
    fn calculator_name_matches_dag_module() {
        assert_eq!(WellToPumpProcessor.name(), "WellToPumpProcessor");
        assert_eq!(WellToPumpProcessor::NAME, "WellToPumpProcessor");
    }

    #[test]
    fn calculator_is_chained_with_no_subscriptions() {
        // calculator-dag.json: subscribes_directly false, subscriptions [].
        assert!(WellToPumpProcessor.subscriptions().is_empty());
    }

    #[test]
    fn registrations_are_empty_because_the_process_is_unwired() {
        // Process 99 has no Registration directive; dag registrations_count 0.
        assert!(WellToPumpProcessor.registrations().is_empty());
    }

    #[test]
    fn upstream_is_empty() {
        // The unwired process leaves the DAG depends_on empty.
        assert!(WellToPumpProcessor.upstream().is_empty());
    }

    #[test]
    fn execute_returns_nonempty_dataframe_for_minimal_inputs() {
        use moves_framework::execution::execution_db::{ExecutionTime, IterationPosition};
        use moves_framework::{DataFrameStore, InMemoryStore, TableRow};
        let inputs = minimal_inputs();
        let mut store = InMemoryStore::new();
        store.insert(
            "GREETWellToPump",
            GreetWellToPumpRow::into_dataframe(inputs.greet).unwrap(),
        );
        store.insert(
            "FuelSupply",
            FuelSupplyRow::into_dataframe(inputs.fuel_supply).unwrap(),
        );
        store.insert(
            "FuelFormulation",
            FuelFormulationRow::into_dataframe(inputs.fuel_formulation).unwrap(),
        );
        store.insert(
            "FuelSubtype",
            FuelSubTypeRow::into_dataframe(inputs.fuel_sub_type).unwrap(),
        );
        store.insert("Year", YearRow::into_dataframe(inputs.year).unwrap());
        store.insert(
            "MonthOfAnyYear",
            MonthGroupRow::into_dataframe(inputs.month_of_any_year).unwrap(),
        );
        store.insert(
            "MOVESWorkerOutput",
            WorkerOutputRow::into_dataframe(inputs.worker_output).unwrap(),
        );
        // `execute` now requires a resolved context year (the SQL substitutes
        // `##context.year##` before running); supply the run year (2020) the
        // minimal inputs are keyed to.
        let pos = IterationPosition {
            time: ExecutionTime::year(2020),
            ..IterationPosition::default()
        };
        let ctx = CalculatorContext::with_position_and_tables(pos, store);
        let out = WellToPumpProcessor.execute(&ctx).expect("execute ok");
        let df = out.dataframe().expect("output should contain a DataFrame");
        assert_eq!(df.height(), 1, "minimal inputs produce exactly one row");
    }

    #[test]
    fn factory_builds_a_named_calculator() {
        assert_eq!(factory().name(), "WellToPumpProcessor");
    }

    #[test]
    fn calculator_is_object_safe() {
        let calc: Box<dyn Calculator> = Box::new(WellToPumpProcessor);
        assert_eq!(calc.name(), "WellToPumpProcessor");
    }
}
