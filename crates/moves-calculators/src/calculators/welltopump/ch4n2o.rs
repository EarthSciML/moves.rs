//! Port of `CH4N2OWTPCalculator.java` and `database/CH4N2OWTPCalculator.sql`
//! — migration plan Phase 3, Task 69.
//!
//! `CH4N2OWTPCalculator` computes **well-to-pump (upstream) methane (CH4) and
//! nitrous oxide (N2O)** — the two non-CO2 greenhouse gases released
//! extracting, refining and distributing the fuel a vehicle later burns. It
//! scales each pump-to-wheel Total Energy Consumption record by fuel-specific
//! well-to-pump CH4 and N2O factors.
//!
//! # Superseded — empty registrations
//!
//! `CH4N2OWTPCalculator` is not wired into the pinned MOVES runtime. The
//! Well-To-Pump process (id 99) has **no `Registration` directive at all** in
//! `CalculatorInfo.txt`, and `characterization/calculator-chains/calculator-dag.json`
//! records `CH4N2OWTPCalculator` with `registrations_count: 0`,
//! `subscriptions: []` and `depends_on: []`. The modern base-rate engine
//! (`BaseRateCalculator`, migration-plan Task 45) absorbed the per-pollutant
//! scripted-SQL calculators; the migration plan still lists this class as part
//! of Task 69, so the module ports its algorithm faithfully for reference and
//! cross-validation with [`Calculator::registrations`] returning an empty
//! slice. See [`super::common`] for the cluster's shared infrastructure.
//!
//! # Chained calculator
//!
//! `CH4N2OWTPCalculator` is a *chained* calculator: its Java `subscribeToMe`
//! does not subscribe to the MasterLoop but chains the calculator onto the
//! ones producing Total Energy Consumption. The chain DAG records
//! `subscribes_directly: false`; the [`Calculator`] metadata mirrors it —
//! [`subscriptions`](Calculator::subscriptions) is empty, and
//! [`upstream`](Calculator::upstream) is empty too because the unwired process
//! leaves the DAG `depends_on` empty.
//!
//! # What it computes
//!
//! [`Ch4N2oWtpCalculator::calculate`] ports `CH4N2OWTPCalculator.sql`. For
//! every pump-to-wheel Total Energy Consumption record (`MOVESWorkerOutput`,
//! pollutant 91, any process but Well-To-Pump itself) and each well-to-pump
//! pollutant — methane (5) and nitrous oxide (6):
//!
//! ```text
//! wellToPumpEmission = Σ (pumpToWheelEnergy × WTPFactor)
//! ```
//!
//! summed over the records of each output dimension cell. `WTPFactor` is the
//! market-share-weighted, year-interpolated GREET well-to-pump factor of the
//! cell's `(year, monthGroup, fuelType)` for that pollutant — the
//! [`build_wtp_factor_by_fuel_type`] table; the factor table carries a cell
//! per pollutant, so each Total Energy record fans out to one CH4 and one N2O
//! output row.
//!
//! The SQL's `MOVESOutputTemp` aggregate is `SUM(mwo.emissionQuant *
//! wfft.WTPFactor)` — the factor multiplies **inside** the sum, one row at a
//! time. `WTPFactor` is in fact constant within a `GROUP BY` group, so the
//! result equals `Σ(emissionQuant) × WTPFactor` mathematically; the two forms
//! differ only in `f64` rounding. This port reproduces the SQL's per-row form
//! exactly, in contrast to [`super::total_energy`]'s `SUM(emissionQuant) ×
//! WTPFactor` — the distinction matters for `mo-fvuf` validation.
//!
//! The output row is stamped with the well-to-pump process (99) and the
//! factor table's pollutant (5 or 6). Every contributing source process
//! collapses into one output row: the SQL `GROUP BY` does **not** include
//! `mwo.processID`.
//!
//! Every SQL join is an `INNER JOIN`, so a record that fails to resolve its
//! month group or well-to-pump factor is dropped; the port reproduces that
//! with map lookups that skip on a miss.
//!
//! # Scope of this port
//!
//! [`calculate`](Ch4N2oWtpCalculator::calculate) is the SQL "Processing"
//! section plus the GREET interpolation and market-share weighting of the
//! "Extract Data" section (factored into [`super::common`]). Its [`WtpInputs`]
//! argument is the set of tables the SQL extracts, as plain row vectors; a
//! future Task 50 (`DataFrameStore`) wiring populates it from the per-run
//! filtered execution database.
//!
//! The Java `doExecute` gates the whole calculator on the RunSpec actually
//! requesting CH4 or N2O for Well-To-Pump; that is execution-gating,
//! reproduced by `calculate` returning no rows on empty input. `SCC` is a
//! pass-through column left to the Task 50 wiring (`CH4N2OWTPCalculator.sql`,
//! unlike the other WTP scripts, does not even carry `MOVESRunID`).
//!
//! The SQL keys `WTPFactorByFuelTypeCH4N2O` by the literal context `countyID`
//! and joins it `wfft.countyID = mwo.countyID`; a master-loop invocation is
//! single-county, so the join is trivially satisfied and the port carries
//! `countyID` straight from the energy row, matching `SO2Calculator`.
//!
//! # Data plane (Task 50)
//!
//! [`Calculator::execute`] receives a [`CalculatorContext`] whose execution
//! tables and scratch namespace are Phase 2 placeholders until the
//! `DataFrameStore` lands (migration-plan Task 50), so `execute` cannot yet
//! read `MOVESWorkerOutput` nor write the well-to-pump rows back. The numeric
//! algorithm is fully ported and unit-tested on
//! [`calculate`](Ch4N2oWtpCalculator::calculate); `execute` is a documented
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

/// Stable module name — matches the Java class and the `CH4N2OWTPCalculator`
/// entry in the calculator-chain DAG (`calculator-dag.json`).
const CALCULATOR_NAME: &str = "CH4N2OWTPCalculator";

/// Total Energy Consumption — `Pollutant` id 91. `CH4N2OWTPCalculator` reads
/// the `MOVESWorkerOutput` records for this pollutant; the GREET factors
/// convert that energy into well-to-pump methane and nitrous oxide.
const TOTAL_ENERGY_POLLUTANT_ID: i32 = 91;

/// Well-To-Pump — `EmissionProcess` id 99. The process the output rows carry,
/// and the one the input filter excludes.
const WELL_TO_PUMP_PROCESS_ID: i32 = 99;

/// The `GROUP BY` cell of `CH4N2OWTPCalculator.sql`'s `MOVESOutputTemp` — the
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
    /// The well-to-pump factor table's pollutant — methane (5) or nitrous
    /// oxide (6).
    pollutant_id: i32,
    source_type_id: i32,
    fuel_type_id: i32,
    model_year_id: i32,
    road_type_id: i32,
}

/// The MOVES well-to-pump methane and nitrous oxide calculator.
///
/// A zero-sized value type owning no per-run state, as the [`Calculator`]
/// trait contract requires; all run-varying input flows through the
/// [`WtpInputs`] argument to [`calculate`](Self::calculate).
#[derive(Debug, Clone, Copy, Default)]
pub struct Ch4N2oWtpCalculator;

impl Ch4N2oWtpCalculator {
    /// Stable module name — matches the Java class and the chain-DAG entry.
    pub const NAME: &'static str = CALCULATOR_NAME;

    /// Compute the well-to-pump CH4 and N2O rows — the port of
    /// `CH4N2OWTPCalculator.sql`.
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

        // emissionQuant = Σ (energy × WTPFactor) — the factor is folded into
        // the per-row product, matching the SQL's SUM(emissionQuant * factor).
        let mut groups: BTreeMap<GroupKey, f64> = BTreeMap::new();
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
                *groups.entry(key).or_insert(0.0) += energy.emission_quant * cell.factor;
            }
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
                pollutant_id: key.pollutant_id,
                process_id: WELL_TO_PUMP_PROCESS_ID,
                source_type_id: key.source_type_id,
                fuel_type_id: key.fuel_type_id,
                model_year_id: key.model_year_id,
                road_type_id: key.road_type_id,
                emission_quant,
            })
            .collect()
    }
}

/// `CH4N2OWTPCalculator` is a chained calculator — `subscribes_directly: false`
/// in `calculator-dag.json` — so it declares no MasterLoop subscription.
static NO_SUBSCRIPTIONS: &[CalculatorSubscription] = &[];

/// `CH4N2OWTPCalculator` registers nothing: the Well-To-Pump process has no
/// `Registration` directive in `CalculatorInfo.txt` and the chain DAG records
/// `registrations_count: 0`. See the module-level supersession note.
static NO_REGISTRATIONS: &[PollutantProcessAssociation] = &[];

/// Default-DB tables the well-to-pump CH4/N2O computation consumes.
static INPUT_TABLES: &[&str] = &[
    "FuelFormulation",
    "FuelSubtype",
    "FuelSupply",
    "GREETWellToPump",
    "MOVESWorkerOutput",
    "MonthOfAnyYear",
    "Year",
];

impl Calculator for Ch4N2oWtpCalculator {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    /// `CH4N2OWTPCalculator` is a chained calculator: it does not subscribe to
    /// the MasterLoop directly. `calculator-dag.json` records
    /// `subscribes_directly: false` and an empty `subscriptions` list.
    fn subscriptions(&self) -> &[CalculatorSubscription] {
        NO_SUBSCRIPTIONS
    }

    /// Empty — `CH4N2OWTPCalculator` is superseded by `BaseRateCalculator` and
    /// registers no `(pollutant, process)` pairs; see the module-level note.
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
    /// [`Ch4N2oWtpCalculator::calculate`].
    fn execute(&self, ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
        let tables = ctx.tables();
        let inputs = WtpInputs {
            greet: tables.iter_typed::<GreetWellToPumpRow>("GREETWellToPump")?,
            fuel_supply: tables.iter_typed::<FuelSupplyRow>("FuelSupply")?,
            fuel_formulation: tables.iter_typed::<FuelFormulationRow>("FuelFormulation")?,
            fuel_sub_type: tables.iter_typed::<FuelSubTypeRow>("FuelSubtype")?,
            year: tables.iter_typed::<YearRow>("Year")?,
            month_of_any_year: tables.iter_typed::<MonthGroupRow>("MonthOfAnyYear")?,
            worker_output: tables.iter_typed::<WorkerOutputRow>("MOVESWorkerOutput")?,
            target_year: ctx.position().time.year.map(|y| i32::from(y)).unwrap_or(0),
        };
        let rows = self.calculate(&inputs);
        crate::wiring::emit_rows(rows)
    }
}

/// Construct the calculator as a boxed trait object — matches the engine's
/// calculator-factory signature so the registry can register it.
#[must_use]
pub fn factory() -> Box<dyn Calculator> {
    Box::new(Ch4N2oWtpCalculator)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::calculators::welltopump::common::{
        FuelFormulationRow, FuelSubTypeRow, FuelSupplyRow, GreetWellToPumpRow, MonthGroupRow,
        YearRow,
    };

    /// Build a one-formulation / one-energy-row input carrying GREET factors
    /// for both methane (5) and nitrous oxide (6). With market share 1.0 the
    /// factors are the GREET rates `10.0` (CH4) and `20.0` (N2O); the single
    /// energy record is `200.0`, so the two output rows are `200.0 × 10.0`
    /// and `200.0 × 20.0`.
    fn minimal_inputs() -> WtpInputs {
        WtpInputs {
            greet: vec![
                GreetWellToPumpRow {
                    pollutant_id: 5,
                    fuel_sub_type_id: 21,
                    year_id: 2020,
                    emission_rate: 10.0,
                },
                GreetWellToPumpRow {
                    pollutant_id: 6,
                    fuel_sub_type_id: 21,
                    year_id: 2020,
                    emission_rate: 20.0,
                },
            ],
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
    fn calculate_fans_out_to_methane_and_nitrous_oxide() {
        let rows = Ch4N2oWtpCalculator.calculate(&minimal_inputs());
        // One energy record → one CH4 row and one N2O row.
        assert_eq!(rows.len(), 2);
        let methane = rows.iter().find(|r| r.pollutant_id == 5).unwrap();
        let nitrous = rows.iter().find(|r| r.pollutant_id == 6).unwrap();
        assert_eq!(methane.process_id, 99);
        assert_eq!(nitrous.process_id, 99);
        // 200.0 × 10.0 and 200.0 × 20.0.
        assert_close(methane.emission_quant, 2_000.0);
        assert_close(nitrous.emission_quant, 4_000.0);
    }

    #[test]
    fn calculate_carries_the_dimension_cell() {
        let rows = Ch4N2oWtpCalculator.calculate(&minimal_inputs());
        for r in &rows {
            assert_eq!(r.year_id, 2020);
            assert_eq!(r.county_id, 26_161);
            assert_eq!(r.source_type_id, 21);
            assert_eq!(r.fuel_type_id, 2);
            assert_eq!(r.model_year_id, 2018);
            assert_eq!(r.road_type_id, 4);
        }
    }

    #[test]
    fn calculate_sums_energy_across_source_processes() {
        // Two energy records, processes 1 and 2 — collapsed (GROUP BY omits
        // processID); each pollutant row sums Σ(energy × factor).
        let mut inputs = minimal_inputs();
        inputs.worker_output.push(WorkerOutputRow {
            process_id: 2,
            emission_quant: 50.0,
            ..inputs.worker_output[0]
        });
        let rows = Ch4N2oWtpCalculator.calculate(&inputs);
        assert_eq!(rows.len(), 2);
        let methane = rows.iter().find(|r| r.pollutant_id == 5).unwrap();
        // 200.0 × 10.0 + 50.0 × 10.0.
        assert_close(methane.emission_quant, 2_500.0);
    }

    #[test]
    fn calculate_excludes_well_to_pump_process_input() {
        let mut inputs = minimal_inputs();
        inputs.worker_output[0].process_id = 99;
        assert!(Ch4N2oWtpCalculator.calculate(&inputs).is_empty());
    }

    #[test]
    fn calculate_ignores_non_energy_rows() {
        let mut inputs = minimal_inputs();
        inputs.worker_output[0].pollutant_id = 2;
        assert!(Ch4N2oWtpCalculator.calculate(&inputs).is_empty());
    }

    #[test]
    fn calculate_drops_energy_without_a_month_group() {
        let mut inputs = minimal_inputs();
        inputs.month_of_any_year.clear();
        assert!(Ch4N2oWtpCalculator.calculate(&inputs).is_empty());
    }

    #[test]
    fn calculate_drops_energy_without_a_factor() {
        let mut inputs = minimal_inputs();
        inputs.fuel_supply.clear();
        assert!(Ch4N2oWtpCalculator.calculate(&inputs).is_empty());
    }

    #[test]
    fn calculate_handles_a_single_pollutant() {
        // GREET data for methane only → only methane output rows.
        let mut inputs = minimal_inputs();
        inputs.greet.retain(|g| g.pollutant_id == 5);
        let rows = Ch4N2oWtpCalculator.calculate(&inputs);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].pollutant_id, 5);
    }

    #[test]
    fn calculate_empty_input_yields_no_rows() {
        assert!(Ch4N2oWtpCalculator
            .calculate(&WtpInputs::default())
            .is_empty());
    }

    #[test]
    fn calculate_output_is_ordered_by_pollutant() {
        // The two pollutant rows of one cell come back CH4 (5) before N2O (6).
        let rows = Ch4N2oWtpCalculator.calculate(&minimal_inputs());
        assert_eq!(rows[0].pollutant_id, 5);
        assert_eq!(rows[1].pollutant_id, 6);
    }

    #[test]
    fn calculator_name_matches_dag_module() {
        assert_eq!(Ch4N2oWtpCalculator.name(), "CH4N2OWTPCalculator");
        assert_eq!(Ch4N2oWtpCalculator::NAME, "CH4N2OWTPCalculator");
    }

    #[test]
    fn calculator_is_chained_with_no_subscriptions() {
        assert!(Ch4N2oWtpCalculator.subscriptions().is_empty());
    }

    #[test]
    fn registrations_are_empty_because_the_process_is_unwired() {
        assert!(Ch4N2oWtpCalculator.registrations().is_empty());
    }

    #[test]
    fn upstream_is_empty() {
        assert!(Ch4N2oWtpCalculator.upstream().is_empty());
    }

    #[test]
    fn execute_returns_nonempty_dataframe_for_minimal_inputs() {
        use moves_framework::{DataFrameStore, InMemoryStore, TableRow};
        let inputs = minimal_inputs();
        let mut store = InMemoryStore::new();
        store.insert("GREETWellToPump", GreetWellToPumpRow::into_dataframe(inputs.greet).unwrap());
        store.insert("FuelSupply", FuelSupplyRow::into_dataframe(inputs.fuel_supply).unwrap());
        store.insert("FuelFormulation", FuelFormulationRow::into_dataframe(inputs.fuel_formulation).unwrap());
        store.insert("FuelSubtype", FuelSubTypeRow::into_dataframe(inputs.fuel_sub_type).unwrap());
        store.insert("Year", YearRow::into_dataframe(inputs.year).unwrap());
        store.insert("MonthOfAnyYear", MonthGroupRow::into_dataframe(inputs.month_of_any_year).unwrap());
        store.insert("MOVESWorkerOutput", WorkerOutputRow::into_dataframe(inputs.worker_output).unwrap());
        let ctx = CalculatorContext::with_tables(store);
        let out = Ch4N2oWtpCalculator.execute(&ctx).expect("execute ok");
        let df = out.dataframe().expect("output should contain a DataFrame");
        assert!(df.height() > 0, "minimal inputs produce at least one row");
    }

    #[test]
    fn factory_builds_a_named_calculator() {
        assert_eq!(factory().name(), "CH4N2OWTPCalculator");
    }

    #[test]
    fn calculator_is_object_safe() {
        let calc: Box<dyn Calculator> = Box::new(Ch4N2oWtpCalculator);
        assert_eq!(calc.name(), "CH4N2OWTPCalculator");
    }
}
